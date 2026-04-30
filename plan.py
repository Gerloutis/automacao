import os
import re
import threading
import time
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from flask import Blueprint, render_template, request, redirect, session, url_for, jsonify
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
HC_PANELS_TABLE = os.getenv("HC_PANELS_TABLE", "planejamento_hc_paineis")

plan_bp = Blueprint("planejamento", __name__)

HC_DEFAULT_SHEET_ID = "1VAuoQarh9M96VQnJt85444Asw2hWZoHlb82EmTYlnyw"
HC_DEFAULT_TAB_NAME = "H.C. TT"
HC_MAX_PANELS = 3
HC_PANEL_IDS = [f"painel_{i}" for i in range(1, HC_MAX_PANELS + 1)]

# =========================================================
# CACHE GOOGLE SHEETS
# =========================================================
# Evita estouro de quota 429 do Google Sheets.
# A mesma planilha/aba compartilhada por vários painéis é lida uma única vez
# dentro do intervalo abaixo. Filtros, colunas e valores reaproveitam esses dados.
HC_SHEETS_CACHE_TTL = int(os.getenv("HC_SHEETS_CACHE_TTL", "15"))
hc_sheets_cache = {}
hc_sheets_cache_lock = threading.Lock()

# =========================================================
# IMPORTS DE PLANEJAMENTO
# =========================================================
try:
    from planejamento import (
        get_gc,
        processar_dia,
        _unique_preservando_ordem,
        des_para_qhc,
        qhc_para_base_mae_desligados,
        atualizar_act_quadro_fy,
        atualizar_whs_to_percent,
        to_mes,
        buscar_ihc_base_mae as abs_buscar_ihc,
        etapa_lista_para_abs,
        abs_para_planejamento as abs_para_plan,
        atualizar_whs_abs_percent,
        abs_mes,
    )
except Exception as e:
    print("Erro ao importar app_planejamento:", e)
    get_gc = None
    processar_dia = None
    _unique_preservando_ordem = None
    des_para_qhc = None
    qhc_para_base_mae_desligados = None
    atualizar_act_quadro_fy = None
    atualizar_whs_to_percent = None
    to_mes = None
    abs_buscar_ihc = None
    etapa_lista_para_abs = None
    abs_para_plan = None
    atualizar_whs_abs_percent = None
    abs_mes = None


def _sheet_cache_key(sheet_id, tab_name):
    return f"{safe_str(sheet_id)}::{safe_str(tab_name).upper()}"


def limpar_cache_planilha_hc(sheet_id=None, tab_name=None, painel_id="painel_1"):
    """Limpa o cache de leitura do Sheets."""
    global hc_sheets_cache
    with hc_sheets_cache_lock:
        if sheet_id and tab_name:
            hc_sheets_cache.pop(_sheet_cache_key(sheet_id, tab_name), None)
        else:
            hc_sheets_cache = {}


def carregar_valores_hc_cache(sheet_input=None, tab_name=None, painel_id="painel_1", force_refresh=False):
    """Lê a planilha usando cache compartilhado por sheet_id + aba.

    Esta função centraliza as chamadas ao Google Sheets para HC.
    Total, monitor, filtros, colunas e valores usam os mesmos dados em memória.
    """
    cfg = obter_config_hc(painel_id)
    sheet_id = extrair_sheet_id(sheet_input or cfg["sheet_id"])
    tab_name = safe_str(tab_name or cfg["tab_name"])

    if not tab_name:
        raise ValueError("Informe o nome da aba.")

    key = _sheet_cache_key(sheet_id, tab_name)
    agora = time.time()

    if not force_refresh:
        with hc_sheets_cache_lock:
            cached = hc_sheets_cache.get(key)
            if cached and (agora - cached.get("timestamp", 0) < HC_SHEETS_CACHE_TTL):
                return cached["payload"]

    gc = ensure_gc()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    valores = ws.get_all_values()

    payload = {
        "sheet_id": sheet_id,
        "tab_name": tab_name,
        "spreadsheet_name": sh.title,
        "values": valores,
        "loaded_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

    with hc_sheets_cache_lock:
        hc_sheets_cache[key] = {
            "timestamp": agora,
            "payload": payload,
        }

    return payload


def dataframe_hc_do_cache(sheet_input=None, tab_name=None, painel_id="painel_1", force_refresh=False):
    pacote = carregar_valores_hc_cache(sheet_input, tab_name, painel_id, force_refresh=force_refresh)
    valores = pacote.get("values") or []
    headers = normalizar_headers(valores)
    linhas = valores[1:] if len(valores) > 1 else []

    if not valores or len(valores) < 2:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(linhas, columns=headers)
        if not df.empty:
            df = df.dropna(how="all")
            df = df[df.apply(lambda row: any(safe_str(v) != "" for v in row.tolist()), axis=1)]

    return pacote, headers, linhas, df

@plan_bp.route("/planejamento")
def planejamento():
    if not usuario_planejamento():
        return redirect(url_for("login"))
    return render_template("planejamento.html")

@plan_bp.route("/planejamento/hc-config", methods=["GET"])
def planejamento_hc_config():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    painel_id = safe_str(request.args.get("painel_id")) or "painel_1"

    return jsonify({
        "ok": True,
        "painel_id": painel_id,
        "config": obter_config_hc(painel_id),
        "panels": obter_todos_paineis_hc()
    })


@plan_bp.route("/planejamento/hc-config/test", methods=["POST"])
def planejamento_hc_test():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = payload.get("tab_name")

        dados = carregar_total_headcount(sheet_input=sheet_input, tab_name=tab_name, painel_id=painel_id)

        return jsonify({
            "ok": True,
            "msg": "Conexão testada com sucesso.",
            "resultado": dados
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Falha ao testar a conexão.",
            "detail": str(e)
        }), 500


@plan_bp.route("/planejamento/hc-config/connect", methods=["POST"])
def planejamento_hc_connect():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))

        sheet_id = extrair_sheet_id(sheet_input)
        if not tab_name:
            raise ValueError("Informe o nome da aba.")

        dados = carregar_total_headcount(sheet_input=sheet_id, tab_name=tab_name, painel_id=painel_id)

        salvar_config_hc(painel_id, sheet_id, tab_name, obter_config_hc(painel_id).get("filters", {}), obter_config_hc(painel_id).get("title"))

        return jsonify({
            "ok": True,
            "msg": "Base conectada com sucesso.",
            "resultado": dados
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Não foi possível conectar a base.",
            "detail": str(e)
        }), 500


@plan_bp.route("/planejamento/hc-total", methods=["GET"])
def planejamento_hc_total():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        painel_id = safe_str(request.args.get("painel_id")) or "painel_1"
        force_refresh = safe_str(request.args.get("force")).lower() in {"1", "true", "sim", "yes"}
        dados = carregar_total_headcount(painel_id=painel_id, force_refresh=force_refresh)
        return jsonify({
            "ok": True,
            "painel_id": painel_id,
            "dados": dados
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar o total do HeadCount.",
            "detail": str(e)
        }), 500
    

@plan_bp.route("/planejamento/hc-reset", methods=["POST"])
def planejamento_hc_reset():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(silent=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        resetar_cache_hc_monitor(painel_id)
        limpar_cache_planilha_hc()
        return jsonify({"ok": True, "painel_id": painel_id, "msg": "Monitor e cache reiniciados com sucesso."})
    except Exception as e:
        return jsonify({"ok": False, "msg": "Erro ao reiniciar o monitor.", "detail": str(e)}), 500


@plan_bp.route("/planejamento/hc-refresh", methods=["POST"])
def planejamento_hc_refresh():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(silent=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        cfg = obter_config_hc(painel_id)
        limpar_cache_planilha_hc(cfg.get("sheet_id"), cfg.get("tab_name"), painel_id=painel_id)
        resetar_cache_hc_monitor(painel_id)
        dados = carregar_total_headcount(painel_id=painel_id, force_refresh=True)
        return jsonify({"ok": True, "painel_id": painel_id, "dados": dados})
    except Exception as e:
        return jsonify({"ok": False, "msg": "Erro ao atualizar painel.", "detail": str(e)}), 500

# =========================================================
# HEADCOUNT / GESTÃO H.C.
# =========================================================

@plan_bp.route("/planejamento/hc-tabs", methods=["POST"])
def planejamento_hc_tabs():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        sheet_id = extrair_sheet_id(sheet_input)

        gc = ensure_gc()
        sh = gc.open_by_key(sheet_id)

        abas = [ws.title for ws in sh.worksheets()]

        return jsonify({
            "ok": True,
            "sheet_id": sheet_id,
            "spreadsheet_name": sh.title,
            "tabs": abas
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Não foi possível listar as abas.",
            "detail": str(e)
        }), 500

@plan_bp.route("/planejamento/hc-columns", methods=["POST"])
def planejamento_hc_columns():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))

        force_refresh = (
            safe_str(request.args.get("force")).lower() in {"1", "true", "sim", "yes"}
            or bool(payload.get("force") or payload.get("force_refresh"))
        )
        pacote, headers, linhas, df = dataframe_hc_do_cache(sheet_input, tab_name, painel_id=painel_id, force_refresh=force_refresh)

        return jsonify({
            "ok": True,
            "sheet_id": pacote["sheet_id"],
            "tab_name": pacote["tab_name"],
            "spreadsheet_name": pacote["spreadsheet_name"],
            "columns": headers,
            "loaded_at": pacote.get("loaded_at")
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar colunas.",
            "detail": str(e)
        }), 500

@plan_bp.route("/planejamento/hc-column-values", methods=["POST"])
def planejamento_hc_column_values():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))
        column_name = safe_str(payload.get("column_name"))

        force_refresh = (
            safe_str(request.args.get("force")).lower() in {"1", "true", "sim", "yes"}
            or bool(payload.get("force") or payload.get("force_refresh"))
        )
        pacote, headers, linhas, df = dataframe_hc_do_cache(sheet_input, tab_name, painel_id=painel_id, force_refresh=force_refresh)

        if df.empty:
            return jsonify({
                "ok": True,
                "column_name": column_name,
                "values": [],
                "loaded_at": pacote.get("loaded_at")
            })

        headers_upper = {str(c).strip().upper(): c for c in df.columns}
        col_real = headers_upper.get(column_name.upper())

        if not col_real:
            raise ValueError(f"Coluna '{column_name}' não encontrada.")

        unicos = []
        vistos = set()

        for valor in df[col_real].tolist():
            txt = safe_str(valor)
            chave = txt.upper()
            if not txt or chave in vistos:
                continue
            vistos.add(chave)
            unicos.append(txt)

        return jsonify({
            "ok": True,
            "column_name": col_real,
            "values": sorted(unicos, key=lambda x: x.upper()),
            "loaded_at": pacote.get("loaded_at")
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar valores da coluna.",
            "detail": str(e)
        }), 500


@plan_bp.route("/planejamento/hc-data", methods=["GET"])
def planejamento_hc_data():
    """Entrega as linhas da planilha usando o cache do servidor.

    O objetivo é permitir que o front recalcule totais ao trocar perfil/filtro
    sem chamar novamente o Google Sheets e sem esperar salvar configuração.
    """
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        painel_id = safe_str(request.args.get("painel_id")) or "painel_1"
        force_refresh = safe_str(request.args.get("force")).lower() in {"1", "true", "sim", "yes"}
        pacote, headers, linhas, df = dataframe_hc_do_cache(painel_id=painel_id, force_refresh=force_refresh)

        rows = []
        if df is not None and not df.empty:
            rows = df.fillna("").astype(str).to_dict(orient="records")

        return jsonify({
            "ok": True,
            "painel_id": painel_id,
            "sheet_id": pacote.get("sheet_id"),
            "tab_name": pacote.get("tab_name"),
            "spreadsheet_name": pacote.get("spreadsheet_name"),
            "loaded_at": pacote.get("loaded_at"),
            "columns": headers,
            "rows": rows,
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar dados em cache do HeadCount.",
            "detail": str(e)
        }), 500

@plan_bp.route("/planejamento/hc-config/save", methods=["POST"])
def planejamento_hc_config_save():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}

        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))
        filters = payload.get("filters") or {}
        title = safe_str(payload.get("title")) or "HeadCount monitorado"

        sheet_id = extrair_sheet_id(sheet_input)

        if not sheet_id:
            raise ValueError("Informe o link ou ID da planilha.")

        if not tab_name:
            raise ValueError("Informe o nome da aba.")

        # Ao salvar configuração, força uma leitura real para validar e atualizar o cache.
        # Em troca rápida de perfil, não força leitura real no Sheets.
        # Isso evita atraso de 3-5s e reduz risco de quota 429.
        quick_switch = bool(payload.get("quick_switch") or payload.get("skip_refresh"))

        if not quick_switch:
            _ = carregar_snapshot_headcount(
                sheet_input=sheet_id,
                tab_name=tab_name,
                painel_id=painel_id,
                filters=filters,
                force_refresh=True
            )

        salvar_config_hc(painel_id, sheet_id, tab_name, filters, title)

        if not quick_switch:
            resetar_cache_hc_monitor(painel_id)

        return jsonify({
            "ok": True,
            "msg": "Configuração salva com sucesso.",
            "painel_id": painel_id,
            "config": obter_config_hc(painel_id),
            "panels": obter_todos_paineis_hc(),
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao salvar configuração.",
            "detail": str(e)
        }), 500

def carregar_total_headcount(sheet_input=None, tab_name=None, painel_id="painel_1", filters=None, force_refresh=False):
    pacote, headers, linhas, df = dataframe_hc_do_cache(sheet_input, tab_name, painel_id=painel_id, force_refresh=force_refresh)
    sheet_id_final = pacote["sheet_id"]
    tab_name_final = pacote["tab_name"]
    spreadsheet_name = pacote["spreadsheet_name"]
    valores = pacote.get("values") or []

    if not valores or len(valores) < 2:
        return {
            "total_trabalhando": 0,
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": (filters if filters is not None else obter_config_hc(painel_id).get("filters", {}))
        }

    if df.empty:
        return {
            "total_trabalhando": 0,
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": (filters if filters is not None else obter_config_hc(painel_id).get("filters", {}))
        }

    filtros = filters if filters is not None else (obter_config_hc(painel_id).get("filters", {}) or {})
    df_filtrado = aplicar_filtros_df(df, filtros)

    headers_upper = {str(c).strip().upper(): c for c in df_filtrado.columns}

    col_situacao = None
    candidatos_situacao = [
        "DESCRIÇÃO (SITUAÇÃO)",
        "DESCRICAO (SITUACAO)",
        "SITUAÇÃO",
        "SITUACAO",
        "STATUS",
    ]

    for nome in candidatos_situacao:
        if nome in headers_upper:
            col_situacao = headers_upper[nome]
            break

    total_trabalhando = 0

    if col_situacao:
        total_trabalhando = int(
            df_filtrado[col_situacao]
            .astype(str)
            .str.strip()
            .str.upper()
            .eq("TRABALHANDO")
            .sum()
        )
    else:
        total_trabalhando = len(df_filtrado)

    return {
        "total_trabalhando": total_trabalhando,
        "sheet_id": sheet_id_final,
        "tab_name": tab_name_final,
        "spreadsheet_name": spreadsheet_name,
        "total_linhas": int(len(df)),
        "total_filtrado": int(len(df_filtrado)),
        "filters": filtros
    }
def aplicar_filtros_df(df, filtros=None):
    filtros = filtros or {}

    if df is None or df.empty:
        return df

    df_filtrado = df.copy()
    headers_upper = {str(c).strip().upper(): c for c in df_filtrado.columns}

    for nome_coluna, valores_aceitos in filtros.items():
        if not valores_aceitos:
            continue

        chave = safe_str(nome_coluna).upper()
        col_real = headers_upper.get(chave)

        if not col_real:
            continue

        valores_norm = {safe_str(v).upper() for v in valores_aceitos if safe_str(v)}
        if not valores_norm:
            continue

        df_filtrado = df_filtrado[
            df_filtrado[col_real].astype(str).str.strip().str.upper().isin(valores_norm)
        ]

    return df_filtrado
def localizar_coluna_hc(headers_upper, candidatos):
    for nome in candidatos:
        if nome in headers_upper:
            return headers_upper[nome]
    return None


def carregar_snapshot_headcount(sheet_input=None, tab_name=None, painel_id="painel_1", filters=None, force_refresh=False):
    pacote, headers, linhas, df = dataframe_hc_do_cache(sheet_input, tab_name, painel_id=painel_id, force_refresh=force_refresh)
    sheet_id_final = pacote["sheet_id"]
    tab_name_final = pacote["tab_name"]
    spreadsheet_name = pacote["spreadsheet_name"]
    valores = pacote.get("values") or []

    filtros = filters if filters is not None else (obter_config_hc(painel_id).get("filters", {}) or {})

    if not valores or len(valores) < 2:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros,
        }

    if df.empty:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros,
        }

    df = aplicar_filtros_df(df, filtros)

    headers_upper = {str(c).strip().upper(): c for c in df.columns}

    col_matricula = localizar_coluna_hc(headers_upper, ["MATRÍCULA", "MATRICULA"])
    if not col_matricula:
        raise ValueError("Coluna MATRÍCULA não encontrada na planilha.")

    col_nome = localizar_coluna_hc(headers_upper, ["NOME", "COLABORADOR"])
    col_situacao = localizar_coluna_hc(headers_upper, [
        "DESCRIÇÃO (SITUAÇÃO)",
        "DESCRICAO (SITUACAO)",
        "SITUAÇÃO",
        "SITUACAO",
        "STATUS",
    ])
    if not col_situacao:
        raise ValueError("Coluna de situação não encontrada na planilha.")

    col_cargo = localizar_coluna_hc(headers_upper, [
        "TÍTULO REDUZIDO (CARGO)",
        "TITULO REDUZIDO (CARGO)",
        "CARGO",
    ])
    col_empresa = localizar_coluna_hc(headers_upper, ["EMPRESA", "AGÊNCIA", "AGENCIA"])
    col_filial = localizar_coluna_hc(headers_upper, ["APELIDO (FILIAL)", "FILIAL"])
    col_area = localizar_coluna_hc(headers_upper, ["ÁREA", "AREA"])
    col_setor = localizar_coluna_hc(headers_upper, ["SETOR", "PROCESSO"])
    col_tipo_contrato = localizar_coluna_hc(headers_upper, ["TIPO DE CONTRATO", "CONTRATO"])

    snapshot = {}

    for _, row in df.iterrows():
        matricula = safe_str(row.get(col_matricula))
        if not matricula:
            continue

        snapshot[matricula] = {
            "matricula": matricula,
            "nome": safe_str(row.get(col_nome)) if col_nome else "",
            "situacao": safe_str(row.get(col_situacao)),
            "cargo": safe_str(row.get(col_cargo)) if col_cargo else "",
            "empresa": safe_str(row.get(col_empresa)) if col_empresa else "",
            "filial": safe_str(row.get(col_filial)) if col_filial else "",
            "area": safe_str(row.get(col_area)) if col_area else "",
            "setor": safe_str(row.get(col_setor)) if col_setor else "",
            "tipo_contrato": safe_str(row.get(col_tipo_contrato)) if col_tipo_contrato else "",
        }

    return {
        "sheet_id": sheet_id_final,
        "tab_name": tab_name_final,
        "spreadsheet_name": spreadsheet_name,
        "snapshot": snapshot,
        "total_linhas": int(len(linhas)),
        "total_filtrado": int(len(df)),
        "filters": filtros,
    }
def normalize_upper(valor):
    return safe_str(valor).strip().upper()


def is_trabalhando(valor):
    txt = normalize_upper(valor)
    return txt == "TRABALHANDO"


def is_desligado(valor):
    txt = normalize_upper(valor)
    return txt in {
        "DEMITIDO", "DEMITIDA",
        "DESLIGADO", "DESLIGADA"
    }


def is_afastado(valor):
    txt = normalize_upper(valor)
    return "AFAST" in txt or txt in {"AFASTADO", "AFASTADA"}


def is_promovido(valor):
    txt = normalize_upper(valor)
    return "PROMOVID" in txt


def is_efetivado_situacao(valor):
    txt = normalize_upper(valor)
    return "EFETIV" in txt


def is_empresa_fisia(valor):
    txt = normalize_upper(valor)
    return "FISIA" in txt or "CENTAURO" in txt


def is_empresa_agencia(valor):
    txt = normalize_upper(valor)
    if not txt:
        return False
    return not is_empresa_fisia(txt)

def classificar_alteracao_hc(anterior, atual, tipo_base):
    situacao_anterior = safe_str((anterior or {}).get("situacao"))
    situacao_atual = safe_str((atual or {}).get("situacao"))
    cargo_anterior = safe_str((anterior or {}).get("cargo"))
    cargo_atual = safe_str((atual or {}).get("cargo"))
    empresa_anterior = safe_str((anterior or {}).get("empresa"))
    empresa_atual = safe_str((atual or {}).get("empresa"))

    ant_trabalhando = is_trabalhando(situacao_anterior)
    atu_trabalhando = is_trabalhando(situacao_atual)

    ant_afastado = is_afastado(situacao_anterior)
    atu_afastado = is_afastado(situacao_atual)

    atu_desligado = is_desligado(situacao_atual)
    atu_promovido = is_promovido(situacao_atual)
    atu_efetivado = is_efetivado_situacao(situacao_atual)

    # Novo colaborador
    if tipo_base == "novo":
        return "admissao", "Admissão"

    # Saiu da base
    if tipo_base == "removido":
        if ant_trabalhando:
            return "desligamento", "Desligamento"
        return "observacao", "Observação"

    # Trabalhando -> Demitido/Desligado
    if ant_trabalhando and atu_desligado:
        return "desligamento", "Desligamento"

    # Trabalhando -> Afastado
    if ant_trabalhando and atu_afastado:
        return "afastamento_entrada", "Entrada em afastamento"

    # Afastado -> Trabalhando
    if ant_afastado and atu_trabalhando:
        return "afastamento_retorno", "Retorno de afastamento"

    # Trabalhando -> Efetivado
    if ant_trabalhando and (atu_efetivado or (
        is_empresa_agencia(empresa_anterior) and is_empresa_fisia(empresa_atual)
    )):
        return "efetivacao", "Efetivação"

    # Trabalhando -> Promovido
    if ant_trabalhando and (atu_promovido or (
        cargo_anterior and cargo_atual and normalize_upper(cargo_anterior) != normalize_upper(cargo_atual)
    )):
        return "promocao", "Promoção"

    # Qualquer outra alteração ligada a afastamento
    if ant_afastado != atu_afastado:
        return "afastamento", "Afastamento"

    # Mudança genérica não mapeada
    return "observacao", "Observação"


def montar_mensagem_alteracao(categoria, nome_exibicao, anterior, atual):
    situacao_anterior = safe_str((anterior or {}).get("situacao"))
    situacao_atual = safe_str((atual or {}).get("situacao"))
    cargo_anterior = safe_str((anterior or {}).get("cargo"))
    cargo_atual = safe_str((atual or {}).get("cargo"))
    empresa_anterior = safe_str((anterior or {}).get("empresa"))
    empresa_atual = safe_str((atual or {}).get("empresa"))

    if categoria == "admissao":
        return f"{nome_exibicao} foi adicionado à base. Situação atual: '{situacao_atual}'."

    if categoria == "desligamento":
        return f"{nome_exibicao} mudou de '{situacao_anterior}' para '{situacao_atual or 'fora da base'}'."

    if categoria == "afastamento_entrada":
        return f"{nome_exibicao} entrou em afastamento: '{situacao_anterior}' → '{situacao_atual}'."

    if categoria == "afastamento_retorno":
        return f"{nome_exibicao} retornou de afastamento: '{situacao_anterior}' → '{situacao_atual}'."

    if categoria == "promocao":
        if cargo_anterior and cargo_atual and normalize_upper(cargo_anterior) != normalize_upper(cargo_atual):
            return f"{nome_exibicao} mudou de cargo: '{cargo_anterior}' → '{cargo_atual}'."
        return f"{nome_exibicao} foi promovido: '{situacao_anterior}' → '{situacao_atual}'."

    if categoria == "efetivacao":
        if empresa_anterior and empresa_atual and normalize_upper(empresa_anterior) != normalize_upper(empresa_atual):
            return f"{nome_exibicao} foi efetivado: empresa '{empresa_anterior}' → '{empresa_atual}'."
        return f"{nome_exibicao} foi efetivado: '{situacao_anterior}' → '{situacao_atual}'."

    if categoria == "afastamento":
        return f"{nome_exibicao} teve alteração relacionada a afastamento: '{situacao_anterior}' → '{situacao_atual}'."

    return f"{nome_exibicao} teve uma alteração não mapeada: '{situacao_anterior}' → '{situacao_atual}'."

def montar_detalhes_alteracao(categoria, anterior, atual):
    campos = [
        ("Situação anterior", safe_str((anterior or {}).get("situacao"))),
        ("Situação atual", safe_str((atual or {}).get("situacao"))),
        ("Cargo anterior", safe_str((anterior or {}).get("cargo"))),
        ("Cargo atual", safe_str((atual or {}).get("cargo"))),
        ("Empresa anterior", safe_str((anterior or {}).get("empresa"))),
        ("Empresa atual", safe_str((atual or {}).get("empresa"))),
        ("Filial anterior", safe_str((anterior or {}).get("filial"))),
        ("Filial atual", safe_str((atual or {}).get("filial"))),
        ("Área anterior", safe_str((anterior or {}).get("area"))),
        ("Área atual", safe_str((atual or {}).get("area"))),
        ("Setor anterior", safe_str((anterior or {}).get("setor"))),
        ("Setor atual", safe_str((atual or {}).get("setor"))),
    ]
    return {
        "categoria": categoria,
        "antes": {k: v for k, v in campos if "anterior" in k.lower() and v},
        "depois": {k: v for k, v in campos if "atual" in k.lower() and v},
    }

#---- ADIÇÕES

def carregar_snapshot_headcount_monitor(sheet_input=None, tab_name=None, painel_id="painel_1", filters=None, force_refresh=False):
    pacote, headers, linhas, df = dataframe_hc_do_cache(sheet_input, tab_name, painel_id=painel_id, force_refresh=force_refresh)
    sheet_id_final = pacote["sheet_id"]
    tab_name_final = pacote["tab_name"]
    spreadsheet_name = pacote["spreadsheet_name"]
    valores = pacote.get("values") or []

    filtros = filters if filters is not None else (obter_config_hc(painel_id).get("filters", {}) or {})
    filtros_monitor = dict(filtros)

    # remove o filtro de situação só para monitoramento
    for chave in list(filtros_monitor.keys()):
        if safe_str(chave).strip().upper() in {
            "DESCRIÇÃO (SITUAÇÃO)",
            "DESCRICAO (SITUACAO)",
            "SITUAÇÃO",
            "SITUACAO",
            "STATUS"
        }:
            filtros_monitor.pop(chave, None)

    if not valores or len(valores) < 2:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros_monitor,
        }

    if df.empty:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": spreadsheet_name,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros_monitor,
        }

    df = aplicar_filtros_df(df, filtros_monitor)

    headers_upper = {str(c).strip().upper(): c for c in df.columns}

    col_matricula = localizar_coluna_hc(headers_upper, ["MATRÍCULA", "MATRICULA"])
    if not col_matricula:
        raise ValueError("Coluna MATRÍCULA não encontrada na planilha.")

    col_nome = localizar_coluna_hc(headers_upper, ["NOME", "COLABORADOR"])
    col_situacao = localizar_coluna_hc(headers_upper, [
        "DESCRIÇÃO (SITUAÇÃO)",
        "DESCRICAO (SITUACAO)",
        "SITUAÇÃO",
        "SITUACAO",
        "STATUS",
    ])
    if not col_situacao:
        raise ValueError("Coluna de situação não encontrada na planilha.")

    col_cargo = localizar_coluna_hc(headers_upper, [
        "TÍTULO REDUZIDO (CARGO)",
        "TITULO REDUZIDO (CARGO)",
        "CARGO",
    ])
    col_empresa = localizar_coluna_hc(headers_upper, ["EMPRESA", "AGÊNCIA", "AGENCIA"])
    col_filial = localizar_coluna_hc(headers_upper, ["APELIDO (FILIAL)", "FILIAL"])
    col_area = localizar_coluna_hc(headers_upper, ["ÁREA", "AREA"])
    col_setor = localizar_coluna_hc(headers_upper, ["SETOR", "PROCESSO"])
    col_tipo_contrato = localizar_coluna_hc(headers_upper, ["TIPO DE CONTRATO", "CONTRATO"])

    snapshot = {}

    for _, row in df.iterrows():
        matricula = safe_str(row.get(col_matricula))
        if not matricula:
            continue

        situacao_raw = safe_str(row.get(col_situacao))

        snapshot[matricula] = {
            "matricula": matricula,
            "nome": safe_str(row.get(col_nome)) if col_nome else "",
            "situacao": situacao_raw,
            "situacao_norm": normalizar_situacao(situacao_raw),
            "cargo": safe_str(row.get(col_cargo)) if col_cargo else "",
            "empresa": safe_str(row.get(col_empresa)) if col_empresa else "",
            "filial": safe_str(row.get(col_filial)) if col_filial else "",
            "area": safe_str(row.get(col_area)) if col_area else "",
            "setor": safe_str(row.get(col_setor)) if col_setor else "",
            "tipo_contrato": safe_str(row.get(col_tipo_contrato)) if col_tipo_contrato else "",
        }

    return {
        "sheet_id": sheet_id_final,
        "tab_name": tab_name_final,
        "spreadsheet_name": spreadsheet_name,
        "snapshot": snapshot,
        "total_linhas": int(len(linhas)),
        "total_filtrado": int(len(df)),
        "filters": filtros_monitor,
    }

def obter_chave_colaborador(item):
    """
    Prioriza matrícula.
    Se não existir, tenta CPF.
    Se também não existir, usa nome.
    """
    matricula = normalizar_texto(item.get("Matricula") or item.get("Matrícula") or item.get("matricula"))
    cpf = normalizar_texto(item.get("CPF COLABORADOR") or item.get("cpf"))
    nome = normalizar_texto(item.get("Nome") or item.get("COLABORADOR") or item.get("nome"))

    if matricula:
        return f"MAT:{matricula}"
    if cpf:
        return f"CPF:{cpf}"
    return f"NOME:{nome}"

def montar_snapshot_indexado(snapshot_lista):
    """
    Converte a lista retornada da planilha em dict indexado pelo colaborador.
    """
    indexado = {}

    for item in snapshot_lista:
        chave = obter_chave_colaborador(item)

        nome = (
            item.get("Nome")
            or item.get("COLABORADOR")
            or item.get("nome")
            or "-"
        )

        matricula = (
            item.get("Matricula")
            or item.get("Matrícula")
            or item.get("matricula")
            or ""
        )

        situacao = (
            item.get("Descrição (Situação)")
            or item.get("descricao_situacao")
            or item.get("situacao")
            or ""
        )

        indexado[chave] = {
            "chave": chave,
            "nome": normalizar_texto(nome),
            "matricula": normalizar_texto(matricula),
            "situacao": normalizar_texto(situacao),
            "situacao_norm": normalizar_situacao(situacao),
            "linha_original": item
        }

    return indexado

def classificar_alteracao(anterior, atual):
    sit_ant = normalizar_situacao((anterior or {}).get("situacao"))
    sit_nov = normalizar_situacao((atual or {}).get("situacao"))

    if not anterior and atual and sit_nov == "trabalhando":
        return {
            "tipo": "admissao",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": "",
            "situacao_nova": atual.get("situacao"),
            "descricao": "Novo colaborador entrou na base como Trabalhando.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    if not anterior and atual:
        return {
            "tipo": "observacao",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": "",
            "situacao_nova": atual.get("situacao"),
            "descricao": "Novo registro identificado na base.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    if sit_ant == "trabalhando" and sit_nov == "demitido":
        return {
            "tipo": "desligamento",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": anterior.get("situacao"),
            "situacao_nova": atual.get("situacao"),
            "descricao": "Mudou de Trabalhando para Demitido.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    if sit_ant == "trabalhando" and sit_nov == "afastado":
        return {
            "tipo": "afastamento",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": anterior.get("situacao"),
            "situacao_nova": atual.get("situacao"),
            "descricao": "Mudou de Trabalhando para Afastado.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    if sit_ant == "afastado" and sit_nov == "trabalhando":
        return {
            "tipo": "retorno_afastamento",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": anterior.get("situacao"),
            "situacao_nova": atual.get("situacao"),
            "descricao": "Retornou de Afastado para Trabalhando.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    if sit_ant != sit_nov:
        return {
            "tipo": "observacao",
            "nome": atual.get("nome"),
            "matricula": atual.get("matricula"),
            "situacao_anterior": anterior.get("situacao"),
            "situacao_nova": atual.get("situacao"),
            "descricao": "Mudança de situação fora das regras principais.",
            "data": datetime.now().strftime("%Y-%m-%d")
        }

    return None

# ------------
def comparar_snapshots_hc(snapshot_anterior, snapshot_atual):
    alteracoes = []

    chaves_anteriores = set(snapshot_anterior.keys())
    chaves_atuais = set(snapshot_atual.keys())

    # Novos registros
    for chave in sorted(chaves_atuais - chaves_anteriores):
        atual = snapshot_atual[chave]
        alteracao = classificar_alteracao(None, atual)
        if alteracao:
            alteracoes.append(alteracao)

    # Registros existentes com mudança
    for chave in sorted(chaves_atuais & chaves_anteriores):
        anterior = snapshot_anterior[chave]
        atual = snapshot_atual[chave]

        alteracao = classificar_alteracao(anterior, atual)
        if alteracao:
            alteracoes.append(alteracao)

    for chave in sorted(chaves_anteriores - chaves_atuais):
        anterior = snapshot_anterior[chave]

        alteracoes.append({
            "tipo": "observacao",
            "nome": anterior.get("nome"),
            "matricula": anterior.get("matricula"),
            "situacao_anterior": anterior.get("situacao"),
            "situacao_nova": "",
            "descricao": "Registro deixou de aparecer na base monitorada.",
            "data": datetime.now().strftime("%Y-%m-%d")
        })
    hoje = datetime.now().strftime("%Y-%m-%d")
    alteracoes = [a for a in alteracoes if safe_str(a.get("data"))[:10] == hoje]

    resumo = {
        "admissao": sum(1 for a in alteracoes if a["tipo"] == "admissao"),
        "desligamento": sum(1 for a in alteracoes if a["tipo"] == "desligamento"),
        "afastamento": sum(1 for a in alteracoes if a["tipo"] == "afastamento"),
        "retorno_afastamento": sum(1 for a in alteracoes if a["tipo"] == "retorno_afastamento"),
        "observacao": sum(1 for a in alteracoes if a["tipo"] == "observacao"),
    }

    return {
        "ultimas_alteracoes": alteracoes[:30],
        "resumo": resumo
    }

def contar_trabalhando_snapshot(snapshot):
    return sum(
        1 for item in (snapshot or {}).values()
        if safe_str(item.get("situacao")).upper() == "TRABALHANDO"
    )

def impacto_alteracao_hc(alteracao):
    tipo = safe_str(alteracao.get("tipo")).lower()
    situacao_anterior = safe_str(alteracao.get("situacao_anterior")).upper()
    situacao_atual = safe_str(alteracao.get("situacao_atual")).upper()

    if tipo == "novo" and situacao_atual == "TRABALHANDO":
        return 1

    if tipo == "removido" and situacao_anterior == "TRABALHANDO":
        return -1

    if tipo == "situacao_alterada":
        if situacao_anterior == "TRABALHANDO" and situacao_atual != "TRABALHANDO":
            return -1
        if situacao_anterior != "TRABALHANDO" and situacao_atual == "TRABALHANDO":
            return 1

    return 0

def assinatura_alteracao_hc(alteracao):
    return "|".join([
        safe_str(alteracao.get("tipo")),
        safe_str(alteracao.get("matricula")),
        safe_str(alteracao.get("situacao_anterior")),
        safe_str(alteracao.get("situacao_atual")),
    ])

def aplicar_confirmacao_alteracao(snapshot_confirmado, snapshot_atual, alteracao):
    snapshot_confirmado = dict(snapshot_confirmado or {})
    snapshot_atual = snapshot_atual or {}

    matricula = safe_str(alteracao.get("matricula"))
    tipo = safe_str(alteracao.get("tipo")).lower()

    if not matricula:
        return snapshot_confirmado

    if tipo == "removido":
        snapshot_confirmado.pop(matricula, None)
        return snapshot_confirmado

    if matricula in snapshot_atual:
        snapshot_confirmado[matricula] = dict(snapshot_atual[matricula])

    return snapshot_confirmado

@plan_bp.route("/planejamento/hc-monitor", methods=["GET"])
def planejamento_hc_monitor():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        painel_id = safe_str(request.args.get("painel_id")) or "painel_1"
        force_refresh = safe_str(request.args.get("force")).lower() in {"1", "true", "sim", "yes"}
        dados = carregar_snapshot_headcount_monitor(painel_id=painel_id, force_refresh=force_refresh)
        snapshot_atual = dados["snapshot"]

        cache = get_hc_monitor_cache(painel_id)

        if not cache["initialized"]:
            cache["snapshot"] = snapshot_atual
            cache["last_check"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            cache["initialized"] = True

            return jsonify({
                "ok": True,
                "painel_id": painel_id,
                "initialized": True,
                "monitor": {
                    "ultimas_alteracoes": [],
                    "resumo": {
                        "admissao": 0,
                        "desligamento": 0,
                        "afastamento": 0,
                        "retorno_afastamento": 0,
                        "observacao": 0
                    }
                },
                "last_check": cache["last_check"],
                "total_monitorados": len(snapshot_atual)
            })

        snapshot_anterior = cache["snapshot"]
        monitor = comparar_snapshots_hc(snapshot_anterior, snapshot_atual)

        cache["snapshot"] = snapshot_atual
        cache["last_check"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        return jsonify({
            "ok": True,
            "painel_id": painel_id,
            "initialized": False,
            "monitor": monitor,
            "last_check": cache["last_check"],
            "total_monitorados": len(snapshot_atual)
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao monitorar alterações do HC.",
            "detail": str(e)
        }), 500
        
@plan_bp.route("/planejamento/hc-monitor/confirm", methods=["POST"])
def planejamento_hc_monitor_confirm():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        painel_id = safe_str(payload.get("painel_id")) or "painel_1"
        change_id = int(payload.get("change_id") or 0)

        if not change_id:
            raise ValueError("Informe a alteração que deve ser confirmada.")

        cache = get_hc_monitor_cache(painel_id)

        pendentes = cache.get("pendentes", []) or []
        alteracao = next((item for item in pendentes if int(item.get("id", 0)) == change_id), None)

        if not alteracao:
            raise ValueError("Alteração não encontrada ou já confirmada.")

        snapshot_confirmado = cache.get("snapshot_confirmado", {})
        snapshot_atual = cache.get("snapshot_atual", {})

        snapshot_confirmado = aplicar_confirmacao_alteracao(snapshot_confirmado, snapshot_atual, alteracao)
        cache["snapshot_confirmado"] = snapshot_confirmado
        cache["total_confirmado"] = contar_trabalhando_snapshot(snapshot_confirmado)
        cache["pendentes"] = [item for item in pendentes if int(item.get("id", 0)) != change_id]
        cache["last_check"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        return jsonify({
            "ok": True,
            "msg": "Alteração confirmada com sucesso.",
            "total_confirmado": cache["total_confirmado"],
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao confirmar alteração do HeadCount.",
            "detail": str(e)
        }), 500
def extrair_sheet_id(valor):
    valor = safe_str(valor)
    if not valor:
        raise ValueError("Informe o link ou ID da planilha.")

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", valor)
    if match:
        return match.group(1)

    return valor


def normalizar_painel_id(painel_id=None):
    painel_id = safe_str(painel_id) or "painel_1"
    return painel_id if painel_id in HC_PANEL_IDS else "painel_1"


def _defaults_hc_panel():
    return {
        "sheet_id": HC_DEFAULT_SHEET_ID,
        "tab_name": HC_DEFAULT_TAB_NAME,
        "filters": {},
        "title": "HeadCount monitorado",
    }


def usuario_chave_planejamento():
    """Chave usada para vincular as configurações dos painéis à conta logada."""
    return safe_str(session.get("usuario") or session.get("nome") or "").lower()


def get_plan_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL não configurada.")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def inicializar_tabela_hc_paineis():
    """Cria a tabela onde cada usuário salva seus 3 painéis de planejamento."""
    if not DATABASE_URL:
        return

    conn = None
    cur = None
    try:
        conn = get_plan_connection()
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HC_PANELS_TABLE} (
                id SERIAL PRIMARY KEY,
                usuario VARCHAR(160) NOT NULL,
                painel_id VARCHAR(30) NOT NULL,
                titulo VARCHAR(160),
                sheet_id TEXT,
                tab_name VARCHAR(160),
                filters JSONB DEFAULT '{{}}'::jsonb,
                enabled BOOLEAN DEFAULT TRUE,
                criado_em TIMESTAMP DEFAULT NOW(),
                atualizado_em TIMESTAMP DEFAULT NOW(),
                UNIQUE (usuario, painel_id)
            )
        """)
        conn.commit()
    except Exception as e:
        print("[HC PAINEIS] Não foi possível inicializar tabela:", e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def carregar_paineis_hc_bd():
    usuario = usuario_chave_planejamento()
    if not usuario or not DATABASE_URL:
        return None

    conn = None
    cur = None
    try:
        conn = get_plan_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            f"""
            SELECT painel_id, titulo, sheet_id, tab_name, filters, enabled
            FROM {HC_PANELS_TABLE}
            WHERE usuario = %s
            ORDER BY painel_id
            """,
            (usuario,),
        )
        rows = cur.fetchall()
        stored = {}
        for row in rows:
            painel_id = normalizar_painel_id(row.get("painel_id"))
            stored[painel_id] = {
                "sheet_id": safe_str(row.get("sheet_id")) or HC_DEFAULT_SHEET_ID,
                "tab_name": safe_str(row.get("tab_name")) or HC_DEFAULT_TAB_NAME,
                "filters": row.get("filters") or {},
                "title": safe_str(row.get("titulo")) or "HeadCount monitorado",
                "enabled": bool(row.get("enabled")),
            }
        return stored
    except Exception as e:
        print("[HC PAINEIS] Falha ao carregar do banco, usando sessão:", e)
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def salvar_config_hc_bd(painel_id, cfg):
    usuario = usuario_chave_planejamento()
    if not usuario or not DATABASE_URL:
        return False

    conn = None
    cur = None
    try:
        conn = get_plan_connection()
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {HC_PANELS_TABLE}
                (usuario, painel_id, titulo, sheet_id, tab_name, filters, enabled, atualizado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (usuario, painel_id)
            DO UPDATE SET
                titulo = EXCLUDED.titulo,
                sheet_id = EXCLUDED.sheet_id,
                tab_name = EXCLUDED.tab_name,
                filters = EXCLUDED.filters,
                enabled = EXCLUDED.enabled,
                atualizado_em = NOW()
            """,
            (
                usuario,
                painel_id,
                safe_str(cfg.get("title")) or "HeadCount monitorado",
                safe_str(cfg.get("sheet_id")) or HC_DEFAULT_SHEET_ID,
                safe_str(cfg.get("tab_name")) or HC_DEFAULT_TAB_NAME,
                Json(cfg.get("filters") or {}),
                bool(cfg.get("enabled", True)),
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        if conn:
            conn.rollback()
        print("[HC PAINEIS] Falha ao salvar no banco, usando sessão:", e)
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def obter_todos_paineis_hc():
    stored = carregar_paineis_hc_bd()

    # Fallback para manter funcionando mesmo se o banco não estiver disponível.
    if stored is None:
        stored = session.get("hc_panels") or {}

    paineis = {}
    for painel_id in HC_PANEL_IDS:
        base = dict(_defaults_hc_panel())
        cfg = stored.get(painel_id) or {}
        base["sheet_id"] = safe_str(cfg.get("sheet_id")) or base["sheet_id"]
        base["tab_name"] = safe_str(cfg.get("tab_name")) or base["tab_name"]
        base["filters"] = cfg.get("filters") or {}
        base["title"] = safe_str(cfg.get("title") or cfg.get("titulo")) or base["title"]
        base["enabled"] = bool(cfg.get("enabled", painel_id == "painel_1" or painel_id in stored))
        paineis[painel_id] = base
    return paineis


def obter_config_hc(painel_id="painel_1"):
    painel_id = normalizar_painel_id(painel_id)
    return obter_todos_paineis_hc()[painel_id]


def salvar_config_hc(painel_id, sheet_id, tab_name, filters=None, title=None):
    painel_id = normalizar_painel_id(painel_id)
    cfg = {
        "sheet_id": safe_str(sheet_id) or HC_DEFAULT_SHEET_ID,
        "tab_name": safe_str(tab_name) or HC_DEFAULT_TAB_NAME,
        "filters": filters or {},
        "title": safe_str(title) or "HeadCount monitorado",
        "enabled": True,
    }

    # Salva no banco por usuário. A sessão fica só como fallback/cache local.
    salvar_config_hc_bd(painel_id, cfg)

    paineis = session.get("hc_panels") or {}
    paineis[painel_id] = cfg
    session["hc_panels"] = paineis
    session.modified = True
    return cfg
def aplicar_filtros_df(df, filtros=None):
    filtros = filtros or {}

    if df is None or df.empty:
        return df

    df_filtrado = df.copy()
    headers_upper = {str(c).strip().upper(): c for c in df_filtrado.columns}

    for nome_coluna, valores_aceitos in filtros.items():
        if not valores_aceitos:
            continue

        nome_coluna_norm = safe_str(nome_coluna).upper()
        col_real = headers_upper.get(nome_coluna_norm)

        if not col_real:
            continue

        valores_norm = {
            safe_str(v).upper()
            for v in valores_aceitos
            if safe_str(v)
        }

        if not valores_norm:
            continue

        df_filtrado = df_filtrado[
            df_filtrado[col_real]
            .astype(str)
            .str.strip()
            .str.upper()
            .isin(valores_norm)
        ]

    return df_filtrado
def abrir_ws_hc(sheet_id=None, tab_name=None, painel_id="painel_1"):
    cfg = obter_config_hc(painel_id)
    sheet_id = extrair_sheet_id(sheet_id or cfg["sheet_id"])
    tab_name = safe_str(tab_name or cfg["tab_name"])

    if not tab_name:
        raise ValueError("Informe o nome da aba.")

    gc = ensure_gc()
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)

    return sh, ws, sheet_id, tab_name


def aplicar_filtros_df(df, filtros=None):
    filtros = filtros or {}

    if df is None or df.empty:
        return df

    df_filtrado = df.copy()
    headers_upper = {str(c).strip().upper(): c for c in df_filtrado.columns}

    for nome_coluna, valores_aceitos in filtros.items():
        if not valores_aceitos:
            continue

        nome_coluna_norm = safe_str(nome_coluna).upper()
        col_real = headers_upper.get(nome_coluna_norm)

        if not col_real:
            continue

        valores_norm = {
            safe_str(v).upper()
            for v in valores_aceitos
            if safe_str(v)
        }

        if not valores_norm:
            continue

        df_filtrado = df_filtrado[
            df_filtrado[col_real]
            .astype(str)
            .str.strip()
            .str.upper()
            .isin(valores_norm)
        ]

    return df_filtrado

_gc = None
_lock = threading.Lock()
to_percent_cache = {}


def novo_cache_hc_monitor():
    return {
        "snapshot_confirmado": {},
        "snapshot_atual": {},
        "total_confirmado": 0,
        "last_check": None,
        "initialized": False,
        "pendentes": [],
        "next_change_id": 1,
    }

# cache global do monitor por usuário/painel
hc_monitor_cache = {}


def _monitor_cache_key(painel_id="painel_1"):
    usuario = safe_str(session.get("usuario")) or "anon"
    return f"{usuario}:{normalizar_painel_id(painel_id)}"


def get_hc_monitor_cache(painel_id="painel_1"):
    key = _monitor_cache_key(painel_id)
    if key not in hc_monitor_cache:
        hc_monitor_cache[key] = novo_cache_hc_monitor()
    return hc_monitor_cache[key]


def resetar_cache_hc_monitor(painel_id="painel_1"):
    hc_monitor_cache[_monitor_cache_key(painel_id)] = novo_cache_hc_monitor()
    return hc_monitor_cache[_monitor_cache_key(painel_id)]

def normalizar_texto(valor):


    return str(valor or "").strip()

def normalizar_situacao(valor):
    v = normalizar_texto(valor).lower()

    mapa = {
        "trabalhando": "trabalhando",
        "afastado": "afastado",
        "demitido": "demitido",
        "desligado": "demitido",
    }

    return mapa.get(v, v)

def ensure_gc():
    global _gc
    with _lock:
        if _gc is None:
            if get_gc is None:
                raise RuntimeError("Função get_gc não foi carregada do app_planejamento.")
            _gc = get_gc()
    return _gc





def usuario_planejamento():
    tipo = safe_str(session.get("tipo")).lower()
    cargo = safe_str(session.get("cargo")).lower()
    usuario = safe_str(session.get("usuario")).lower()

    return (
        tipo == "planejamento"
        or cargo == "planejamento"
        or usuario == "gerle"
    )



def normalizar_headers(valores):
    if not valores:
        return []

    cabecalho = [str(c).strip() for c in valores[0]]
    cabecalho_unico = []
    contadores = {}

    for col in cabecalho:
        nome = col if col else "COLUNA_VAZIA"
        if nome in contadores:
            contadores[nome] += 1
            nome = f"{nome}_{contadores[nome]}"
        else:
            contadores[nome] = 0
        cabecalho_unico.append(nome)

    return cabecalho_unico







def safe_str(valor):
    return str(valor).strip() if valor is not None else ""




# Inicializa a tabela dos painéis por usuário ao carregar o módulo.
inicializar_tabela_hc_paineis()
