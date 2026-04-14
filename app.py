import os
import io
import re
import time
import threading
from contextlib import redirect_stdout
from datetime import date, timedelta, datetime, timezone
from calendar import monthrange
from werkzeug.utils import secure_filename

import pandas as pd
import psycopg2
from psycopg2.extras import Json, RealDictCursor
from sqlalchemy import create_engine
from flask import Flask, render_template, request, redirect, session, url_for, jsonify, send_file
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = "chave_super_secreta"

DATABASE_URL = os.getenv("DATABASE_URL")
SOLICITACOES_TABLE = os.getenv("SOLICITACOES_TABLE", "solicitacoes_colaborador")
ATESTADOS_TABLE = os.getenv("ATESTADOS_TABLE", "atestados_colaborador")
MAX_ATESTADO_BYTES = int(os.getenv("MAX_ATESTADO_BYTES", str(5 * 1024 * 1024)))
PLANILHA_PRESENCA_ID = "1Qv9mI_vo0yA987Kabn-bUM6XaQq2IOs4dLZKAzwU8P8"
HC_DEFAULT_SHEET_ID = "1VAuoQarh9M96VQnJt85444Asw2hWZoHlb82EmTYlnyw"
HC_DEFAULT_TAB_NAME = "H.C. TT"

MESES_PT = {
    1: "JANEIRO",
    2: "FEVEREIRO",
    3: "MARÇO",
    4: "ABRIL",
    5: "MAIO",
    6: "JUNHO",
    7: "JULHO",
    8: "AGOSTO",
    9: "SETEMBRO",
    10: "OUTUBRO",
    11: "NOVEMBRO",
    12: "DEZEMBRO"
}

STATUS_PRESENCA = ["P", "F", "AT", "PA", "HE", "FC", "FBH", "S", "AF", "FE", "DES"]
TIPOS_SOLICITACAO = {
    "alterar_linha_ponto": {
        "label": "Alterar linha e ponto",
        "destino": "ADM"
    },
    "trocar_gestao": {
        "label": "Trocar de gestão",
        "destino": "ADM"
    },
    "solicitar_desligamento": {
        "label": "Solicitar desligamento",
        "destino": "RH"
    },
    "solicitar_efetivacao": {
        "label": "Solicitar efetivação",
        "destino": "RH"
    },
    "solicitar_promocao": {
        "label": "Solicitar promoção",
        "destino": "RH"
    },
    "adicionar_atestado": {
        "label": "Adicionar atestado",
        "destino": "ADM"
    }
}

@app.route("/planejamento")
def planejamento():
    if not usuario_planejamento():
        return redirect(url_for("login"))
    return render_template("planejamento.html")

@app.route("/planejamento/hc-config", methods=["GET"])
def planejamento_hc_config():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    return jsonify({
        "ok": True,
        "config": obter_config_hc()
    })


@app.route("/planejamento/hc-config/test", methods=["POST"])
def planejamento_hc_test():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = payload.get("tab_name")

        dados = carregar_total_headcount(sheet_input, tab_name)

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


@app.route("/planejamento/hc-config/connect", methods=["POST"])
def planejamento_hc_connect():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))

        sheet_id = extrair_sheet_id(sheet_input)
        if not tab_name:
            raise ValueError("Informe o nome da aba.")

        dados = carregar_total_headcount(sheet_id, tab_name)

        session["hc_sheet_id"] = sheet_id
        session["hc_tab_name"] = tab_name

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


@app.route("/planejamento/hc-total", methods=["GET"])
def planejamento_hc_total():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        dados = carregar_total_headcount()
        return jsonify({
            "ok": True,
            "dados": dados
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar o total do HeadCount.",
            "detail": str(e)
        }), 500
    
# =========================================================
# HEADCOUNT / GESTÃO H.C.
# =========================================================
@app.route("/planejamento/hc-tabs", methods=["POST"])
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

@app.route("/planejamento/hc-columns", methods=["POST"])
def planejamento_hc_columns():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))

        sh, ws, sheet_id_final, tab_name_final = abrir_ws_hc(sheet_input, tab_name)
        valores = ws.get_all_values()

        if not valores:
            return jsonify({
                "ok": True,
                "sheet_id": sheet_id_final,
                "tab_name": tab_name_final,
                "spreadsheet_name": sh.title,
                "columns": []
            })

        headers = normalizar_headers(valores)

        return jsonify({
            "ok": True,
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": sh.title,
            "columns": headers
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar colunas.",
            "detail": str(e)
        }), 500

@app.route("/planejamento/hc-column-values", methods=["POST"])
def planejamento_hc_column_values():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))
        column_name = safe_str(payload.get("column_name"))

        sh, ws, sheet_id_final, tab_name_final = abrir_ws_hc(sheet_input, tab_name)
        valores = ws.get_all_values()

        if not valores or len(valores) < 2:
            return jsonify({
                "ok": True,
                "column_name": column_name,
                "values": []
            })

        headers = normalizar_headers(valores)
        linhas = valores[1:]
        df = pd.DataFrame(linhas, columns=headers)

        if df.empty:
            return jsonify({
                "ok": True,
                "column_name": column_name,
                "values": []
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
            "values": sorted(unicos, key=lambda x: x.upper())
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao carregar valores da coluna.",
            "detail": str(e)
        }), 500

@app.route("/planejamento/hc-config/save", methods=["POST"])
def planejamento_hc_config_save():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}

        sheet_input = payload.get("sheet_id") or payload.get("sheet_url")
        tab_name = safe_str(payload.get("tab_name"))
        filters = payload.get("filters") or {}

        sheet_id = extrair_sheet_id(sheet_input)

        if not sheet_id:
            raise ValueError("Informe o link ou ID da planilha.")

        if not tab_name:
            raise ValueError("Informe o nome da aba.")

        # testa se a planilha e a aba existem
        _ = carregar_snapshot_headcount(sheet_id, tab_name)

        session["hc_sheet_id"] = sheet_id
        session["hc_tab_name"] = tab_name
        session["hc_filters"] = filters
        session.modified = True

        global hc_monitor_cache
        hc_monitor_cache = novo_cache_hc_monitor()

        return jsonify({
            "ok": True,
            "msg": "Configuração salva com sucesso.",
            "config": obter_config_hc(),
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao salvar configuração.",
            "detail": str(e)
        }), 500

def carregar_total_headcount(sheet_id=None, tab_name=None):
    sh, ws, sheet_id_final, tab_name_final = abrir_ws_hc(sheet_id, tab_name)
    valores = ws.get_all_values()

    if not valores or len(valores) < 2:
        return {
            "total_trabalhando": 0,
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": sh.title,
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": session.get("hc_filters", {})
        }

    headers = normalizar_headers(valores)
    linhas = valores[1:]
    df = pd.DataFrame(linhas, columns=headers)

    if df.empty:
        return {
            "total_trabalhando": 0,
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": sh.title,
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": session.get("hc_filters", {})
        }

    df = df.dropna(how="all")
    df = df[
        df.apply(lambda row: any(safe_str(v) != "" for v in row.tolist()), axis=1)
    ]

    filtros = session.get("hc_filters", {}) or {}
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
        "spreadsheet_name": sh.title,
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


def carregar_snapshot_headcount(sheet_id=None, tab_name=None):
    sh, ws, sheet_id_final, tab_name_final = abrir_ws_hc(sheet_id, tab_name)
    valores = ws.get_all_values()

    filtros = session.get("hc_filters", {}) or {}

    if not valores or len(valores) < 2:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": sh.title,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros,
        }

    headers = normalizar_headers(valores)
    linhas = valores[1:]
    df = pd.DataFrame(linhas, columns=headers)

    if df.empty:
        return {
            "sheet_id": sheet_id_final,
            "tab_name": tab_name_final,
            "spreadsheet_name": sh.title,
            "snapshot": {},
            "total_linhas": 0,
            "total_filtrado": 0,
            "filters": filtros,
        }

    df = df.dropna(how="all")
    df = df[
        df.apply(lambda row: any(safe_str(v) != "" for v in row.tolist()), axis=1)
    ]

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
        "spreadsheet_name": sh.title,
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


def comparar_snapshots_hc(snapshot_anterior, snapshot_atual):
    alteracoes = []

    matriculas_anteriores = set(snapshot_anterior.keys())
    matriculas_atuais = set(snapshot_atual.keys())

    novas = matriculas_atuais - matriculas_anteriores
    removidas = matriculas_anteriores - matriculas_atuais
    comuns = matriculas_anteriores & matriculas_atuais

    for matricula in sorted(novas):
        atual = snapshot_atual[matricula]
        categoria, categoria_label = classificar_alteracao_hc(None, atual, "novo")
        nome_exibicao = atual.get("nome", matricula)
        alteracoes.append({
            "tipo": "novo",
            "categoria": categoria,
            "categoria_label": categoria_label,
            "matricula": matricula,
            "nome": atual.get("nome", ""),
            "situacao_anterior": "",
            "situacao_atual": atual.get("situacao", ""),
            "mensagem": montar_mensagem_alteracao(categoria, nome_exibicao, None, atual),
            "detalhes": montar_detalhes_alteracao(categoria, None, atual),
        })

    for matricula in sorted(removidas):
        anterior = snapshot_anterior[matricula]
        categoria, categoria_label = classificar_alteracao_hc(anterior, None, "removido")
        nome_exibicao = anterior.get("nome", matricula)
        alteracoes.append({
            "tipo": "removido",
            "categoria": categoria,
            "categoria_label": categoria_label,
            "matricula": matricula,
            "nome": anterior.get("nome", ""),
            "situacao_anterior": anterior.get("situacao", ""),
            "situacao_atual": "",
            "mensagem": montar_mensagem_alteracao(categoria, nome_exibicao, anterior, None),
            "detalhes": montar_detalhes_alteracao(categoria, anterior, None),
        })

    for matricula in sorted(comuns):
        anterior = snapshot_anterior[matricula]
        atual = snapshot_atual[matricula]

        sit_ant = safe_str(anterior.get("situacao"))
        sit_atual = safe_str(atual.get("situacao"))
        cargo_ant = safe_str(anterior.get("cargo"))
        cargo_atual = safe_str(atual.get("cargo"))
        empresa_ant = safe_str(anterior.get("empresa"))
        empresa_atual = safe_str(atual.get("empresa"))

        if any([
            normalize_upper(sit_ant) != normalize_upper(sit_atual),
            normalize_upper(cargo_ant) != normalize_upper(cargo_atual),
            normalize_upper(empresa_ant) != normalize_upper(empresa_atual),
        ]):
            categoria, categoria_label = classificar_alteracao_hc(anterior, atual, "situacao_alterada")
            nome_exibicao = atual.get("nome", "") or anterior.get("nome", "") or matricula
            alteracoes.append({
                "tipo": "situacao_alterada",
                "categoria": categoria,
                "categoria_label": categoria_label,
                "matricula": matricula,
                "nome": atual.get("nome", "") or anterior.get("nome", ""),
                "situacao_anterior": sit_ant,
                "situacao_atual": sit_atual,
                "mensagem": montar_mensagem_alteracao(categoria, nome_exibicao, anterior, atual),
                "detalhes": montar_detalhes_alteracao(categoria, anterior, atual),
            })

    return alteracoes

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

@app.route("/planejamento/hc-monitor", methods=["GET"])
def planejamento_hc_monitor():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        dados = carregar_snapshot_headcount()
        snapshot_atual = dados["snapshot"]

        global hc_monitor_cache

        agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        if not hc_monitor_cache["initialized"]:
            hc_monitor_cache["snapshot_confirmado"] = dict(snapshot_atual)
            hc_monitor_cache["snapshot_atual"] = dict(snapshot_atual)
            hc_monitor_cache["total_confirmado"] = contar_trabalhando_snapshot(snapshot_atual)
            hc_monitor_cache["last_check"] = agora
            hc_monitor_cache["initialized"] = True
            hc_monitor_cache["pendentes"] = []

            return jsonify({
                "ok": True,
                "initialized": True,
                "alteracoes": [],
                "alteracoes_novas": [],
                "last_check": hc_monitor_cache["last_check"],
                "total_monitorados": len(snapshot_atual),
                "total_confirmado": hc_monitor_cache["total_confirmado"],
                "delta_pendente": 0,
                "total_previsto": hc_monitor_cache["total_confirmado"],
            })

        snapshot_confirmado = hc_monitor_cache["snapshot_confirmado"]
        alteracoes_brutas = comparar_snapshots_hc(snapshot_confirmado, snapshot_atual)
        alteracoes_por_assinatura = {}

        for alteracao in alteracoes_brutas:
            assinatura = assinatura_alteracao_hc(alteracao)
            alteracao["assinatura"] = assinatura
            alteracao["impacto"] = impacto_alteracao_hc(alteracao)
            alteracoes_por_assinatura[assinatura] = alteracao

        pendentes_atuais = hc_monitor_cache.get("pendentes", []) or []
        pendentes_por_assinatura = {item.get("assinatura"): item for item in pendentes_atuais}

        novos_pendentes = []
        alteracoes_novas = []

        for assinatura, alteracao in alteracoes_por_assinatura.items():
            if assinatura in pendentes_por_assinatura:
                existente = pendentes_por_assinatura[assinatura]
                existente["mensagem"] = alteracao.get("mensagem", existente.get("mensagem", ""))
                existente["impacto"] = alteracao.get("impacto", existente.get("impacto", 0))
                existente["nome"] = alteracao.get("nome", existente.get("nome", ""))
                existente["categoria"] = alteracao.get("categoria", existente.get("categoria", "situacao_alterada"))
                existente["categoria_label"] = alteracao.get("categoria_label", existente.get("categoria_label", "Mudança"))
                existente["detalhes"] = alteracao.get("detalhes", existente.get("detalhes", {}))
                novos_pendentes.append(existente)
                continue

            novo_item = {
                "id": hc_monitor_cache["next_change_id"],
                "tipo": alteracao.get("tipo"),
                "categoria": alteracao.get("categoria", "situacao_alterada"),
                "categoria_label": alteracao.get("categoria_label", "Mudança"),
                "matricula": alteracao.get("matricula"),
                "nome": alteracao.get("nome", ""),
                "situacao_anterior": alteracao.get("situacao_anterior", ""),
                "situacao_atual": alteracao.get("situacao_atual", ""),
                "mensagem": alteracao.get("mensagem", "Alteração detectada."),
                "detalhes": alteracao.get("detalhes", {}),
                "impacto": alteracao.get("impacto", 0),
                "assinatura": assinatura,
                "detectado_em": agora,
            }
            hc_monitor_cache["next_change_id"] += 1
            novos_pendentes.append(novo_item)
            alteracoes_novas.append(novo_item)

        hc_monitor_cache["snapshot_atual"] = dict(snapshot_atual)
        hc_monitor_cache["pendentes"] = novos_pendentes
        hc_monitor_cache["last_check"] = agora

        delta_pendente = sum(int(item.get("impacto", 0)) for item in novos_pendentes)
        total_confirmado = int(hc_monitor_cache.get("total_confirmado", 0))

        return jsonify({
            "ok": True,
            "initialized": False,
            "alteracoes": novos_pendentes,
            "alteracoes_novas": alteracoes_novas,
            "last_check": hc_monitor_cache["last_check"],
            "total_monitorados": len(snapshot_atual),
            "total_confirmado": total_confirmado,
            "delta_pendente": delta_pendente,
            "total_previsto": total_confirmado + delta_pendente,
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Erro ao monitorar alterações do HeadCount.",
            "detail": str(e)
        }), 500

@app.route("/planejamento/hc-monitor/confirm", methods=["POST"])
def planejamento_hc_monitor_confirm():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        change_id = int(payload.get("change_id") or 0)

        if not change_id:
            raise ValueError("Informe a alteração que deve ser confirmada.")

        global hc_monitor_cache

        pendentes = hc_monitor_cache.get("pendentes", []) or []
        alteracao = next((item for item in pendentes if int(item.get("id", 0)) == change_id), None)

        if not alteracao:
            raise ValueError("Alteração não encontrada ou já confirmada.")

        snapshot_confirmado = hc_monitor_cache.get("snapshot_confirmado", {})
        snapshot_atual = hc_monitor_cache.get("snapshot_atual", {})

        snapshot_confirmado = aplicar_confirmacao_alteracao(snapshot_confirmado, snapshot_atual, alteracao)
        hc_monitor_cache["snapshot_confirmado"] = snapshot_confirmado
        hc_monitor_cache["total_confirmado"] = contar_trabalhando_snapshot(snapshot_confirmado)
        hc_monitor_cache["pendentes"] = [item for item in pendentes if int(item.get("id", 0)) != change_id]
        hc_monitor_cache["last_check"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

        return jsonify({
            "ok": True,
            "msg": "Alteração confirmada com sucesso.",
            "total_confirmado": hc_monitor_cache["total_confirmado"],
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


def obter_config_hc():
    return {
        "sheet_id": session.get("hc_sheet_id", HC_DEFAULT_SHEET_ID),
        "tab_name": session.get("hc_tab_name", HC_DEFAULT_TAB_NAME),
        "filters": session.get("hc_filters", {}),
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
def abrir_ws_hc(sheet_id=None, tab_name=None):
    cfg = obter_config_hc()
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
# =========================================================
# BANCO / CONEXÃO
# =========================================================
def get_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


engine = create_engine(DATABASE_URL)


def inicializar_tabela_atestados():
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ATESTADOS_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                solicitacao_id BIGINT,
                matricula VARCHAR(50) NOT NULL,
                colaborador_nome TEXT NOT NULL,
                supervisor_usuario VARCHAR(100),
                supervisor_nome TEXT,
                data_referencia DATE NOT NULL,
                quantidade_dias INTEGER NOT NULL DEFAULT 1,
                observacao TEXT,
                nome_arquivo TEXT NOT NULL,
                tipo_arquivo VARCHAR(120) NOT NULL,
                tamanho_bytes BIGINT NOT NULL,
                arquivo BYTEA NOT NULL,
                criado_em TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{ATESTADOS_TABLE}_matricula_data ON {ATESTADOS_TABLE} (matricula, data_referencia DESC)")
        cur.execute(f"ALTER TABLE {ATESTADOS_TABLE} ADD COLUMN IF NOT EXISTS solicitacao_id BIGINT")
        cur.execute(f"ALTER TABLE {ATESTADOS_TABLE} ADD COLUMN IF NOT EXISTS quantidade_dias INTEGER NOT NULL DEFAULT 1")
        conn.commit()
    finally:
        cur.close()
        conn.close()


inicializar_tabela_atestados()


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

hc_monitor_cache = novo_cache_hc_monitor()

def ensure_gc():
    global _gc
    with _lock:
        if _gc is None:
            if get_gc is None:
                raise RuntimeError("Função get_gc não foi carregada do app_planejamento.")
            _gc = get_gc()
    return _gc


# =========================================================
# CONTROLE DE ACESSO
# =========================================================
def usuario_logado():
    return "usuario" in session


def usuario_planejamento():
    return session.get("usuario") == "gerle" and session.get("tipo") == "planejamento"


def _cargo_normalizado():
    return str(session.get("cargo", "")).strip().lower()


def usuario_supervisor():
    return usuario_logado() and session.get("tipo") == "operacao" and _cargo_normalizado() == "supervisor"


def usuario_adm():
    cargo = _cargo_normalizado()
    return usuario_logado() and cargo in {"adm", "administracao", "administração", "administrador"}


def usuario_rh():
    cargo = _cargo_normalizado()
    return usuario_logado() and cargo in {"rh", "recursos humanos"}


# =========================================================
# AJUDANTES
# =========================================================
def nome_aba_mes_atual():
    hoje = datetime.now()
    return MESES_PT[hoje.month]


def prefixo_coluna_hoje():
    hoje = datetime.now()
    return hoje.strftime("%d/%m")


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


def carregar_presenca_supervisor(nome_supervisor):
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(nome_aba_mes_atual())
    valores = ws.get_all_values()

    if not valores or len(valores) < 2:
        return pd.DataFrame(), ws, None

    cabecalho_unico = normalizar_headers(valores)
    linhas = valores[1:]
    df = pd.DataFrame(linhas, columns=cabecalho_unico)

    if df.empty:
        return df, ws, None

    if "SUPERVISOR" not in df.columns:
        raise ValueError("Coluna SUPERVISOR não encontrada na planilha.")

    nome_supervisor = str(nome_supervisor).strip().upper()
    df["SUPERVISOR"] = df["SUPERVISOR"].astype(str).str.strip().str.upper()
    filtrado = df[df["SUPERVISOR"] == nome_supervisor].copy()

    coluna_dia = None
    prefixo = prefixo_coluna_hoje()
    for col in filtrado.columns:
        if str(col).startswith(prefixo):
            coluna_dia = col
            break

    return filtrado, ws, coluna_dia


def carregar_supervisores_disponiveis(data_ref=None):
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(nome_aba_por_data(data_ref or datetime.now().date()))
    valores = ws.get_all_values()

    if not valores or len(valores) < 2:
        return []

    headers = normalizar_headers(valores)
    df = pd.DataFrame(valores[1:], columns=headers)
    if df.empty or "SUPERVISOR" not in df.columns:
        return []

    supervisores = []
    vistos = set()
    for valor in df["SUPERVISOR"].tolist():
        nome = safe_str(valor)
        chave = nome.upper()
        if not nome or chave in vistos:
            continue
        vistos.add(chave)
        supervisores.append(nome)

    return sorted(supervisores, key=lambda x: x.upper())


def safe_str(valor):
    return str(valor).strip() if valor is not None else ""


def buscar_colaborador_por_matricula(nome_supervisor, matricula):
    df, _, coluna_dia = carregar_presenca_supervisor(nome_supervisor)
    if df.empty:
        return None

    matricula = safe_str(matricula)
    for _, row in df.iterrows():
        if safe_str(row.get("MATRÍCULA", "")) == matricula:
            estatisticas = calcular_estatisticas_colaborador(row)
            return {
                "matricula": safe_str(row.get("MATRÍCULA", "")),
                "colaborador": safe_str(row.get("COLABORADOR", "")),
                "cargo": safe_str(row.get("CARGO", "")),
                "area": safe_str(row.get("ÁREA", "")),
                "cidade": safe_str(row.get("CIDADE", "")),
                "turno": safe_str(row.get("TURNO", "")),
                "supervisor": safe_str(row.get("SUPERVISOR", "")),
                "coordenador": safe_str(row.get("COORDENADOR", "")),
                "setor": safe_str(row.get("PROCESSO", "")),
                "linha": safe_str(row.get("LINHA", "")),
                "ponto": safe_str(row.get("PONTO", "")),
                "empresa": safe_str(row.get("EMPRESA", "")),
                "status_hoje": safe_str(row.get(coluna_dia, "")) if coluna_dia else "",
                "desligado": safe_str(row.get("STATUS", "")).upper() == "DESLIGADO",
                **estatisticas,
            }
    return None




def _extrair_data_coluna(coluna, ano_ref=None):
    nome = safe_str(coluna)
    if len(nome) < 5 or nome[2] != '/':
        return None
    trecho = nome[:5]
    try:
        dia = int(trecho[:2])
        mes = int(trecho[3:5])
        ano = ano_ref or datetime.now().year
        return date(ano, mes, dia)
    except Exception:
        return None


def _normalizar_turno_estatistica(turno):
    turno_txt = safe_str(turno).upper().replace("º", "").replace("°", "")
    turno_txt = " ".join(turno_txt.split())

    if turno_txt in {"T1", "1 T", "1T", "1 TURNO", "1 TUR"}:
        return "T1"
    if turno_txt in {"T2", "2 T", "2T", "2 TURNO", "2 TUR"}:
        return "T2"
    if turno_txt in {"T3", "3 T", "3T", "3 TURNO", "3 TUR"}:
        return "T3"

    return turno_txt



def _data_valida_para_turno(data_coluna, turno):
    turno_norm = _normalizar_turno_estatistica(turno)
    dia_semana = data_coluna.weekday()  # segunda=0 ... domingo=6

    if turno_norm in {"T1", "T2"}:
        return dia_semana <= 5  # seg a sáb

    if turno_norm == "T3":
        return dia_semana != 5  # dom a sex (sábado fora)

    return True



def calcular_estatisticas_colaborador(row):
    hoje = datetime.now().date()
    turno = row.get("TURNO", "")
    presenca_codigos = {"P", "PH", "HE"}
    falta_codigos = {"F"}
    atestado_codigos = {"AT"}
    status_validos = set(STATUS_PRESENCA) | {"PH"}

    total_presencas = 0
    total_faltas = 0
    total_atestados = 0
    total_lancados = 0

    for coluna in row.index:
        data_coluna = _extrair_data_coluna(coluna)
        if not data_coluna or data_coluna > hoje:
            continue

        if not _data_valida_para_turno(data_coluna, turno):
            continue

        status = safe_str(row.get(coluna, "")).upper()
        if not status or status not in status_validos:
            continue

        total_lancados += 1
        if status in presenca_codigos:
            total_presencas += 1
        if status in falta_codigos:
            total_faltas += 1
        if status in atestado_codigos:
            total_atestados += 1

    percentual_presenca = round((total_presencas / total_lancados) * 100, 1) if total_lancados else 0.0

    return {
        "total_presencas": total_presencas,
        "total_faltas": total_faltas,
        "total_atestados": total_atestados,
        "total_lancados": total_lancados,
        "percentual_presenca": percentual_presenca,
    }

def calcular_estatisticas_equipe(df):
    colaboradores = []
    totais = {
        "total_colaboradores": 0,
        "colaboradores_ativos": 0,
        "colaboradores_desligados": 0,
        "total_presencas": 0,
        "total_faltas": 0,
        "total_atestados": 0,
        "total_outros": 0,
        "total_lancados": 0,
    }

    if df is None or df.empty:
        return {
            **totais,
            "percentual_presenca_equipe": 0.0,
            "media_presenca_por_colaborador": 0.0,
            "ranking_presencas": [],
            "ranking_faltas": [],
            "ranking_atestados": [],
            "colaboradores_estatisticas": [],
        }

    for _, row in df.iterrows():
        est = calcular_estatisticas_colaborador(row)
        outros = max(est["total_lancados"] - est["total_presencas"] - est["total_faltas"] - est["total_atestados"], 0)
        desligado = safe_str(row.get("STATUS", "")).upper() == "DESLIGADO"

        item = {
            "matricula": safe_str(row.get("MATRÍCULA", "")),
            "colaborador": safe_str(row.get("COLABORADOR", "")),
            "cargo": safe_str(row.get("CARGO", "")),
            "area": safe_str(row.get("ÁREA", "")),
            "setor": safe_str(row.get("PROCESSO", "")),
            "turno": safe_str(row.get("TURNO", "")),
            "desligado": desligado,
            "total_outros": outros,
            **est,
        }
        colaboradores.append(item)

        totais["total_colaboradores"] += 1
        totais["colaboradores_desligados" if desligado else "colaboradores_ativos"] += 1
        totais["total_presencas"] += est["total_presencas"]
        totais["total_faltas"] += est["total_faltas"]
        totais["total_atestados"] += est["total_atestados"]
        totais["total_outros"] += outros
        totais["total_lancados"] += est["total_lancados"]

    percentual_presenca_equipe = round((totais["total_presencas"] / totais["total_lancados"]) * 100, 1) if totais["total_lancados"] else 0.0
    media_presenca_por_colaborador = round((totais["total_presencas"] / totais["total_colaboradores"]), 1) if totais["total_colaboradores"] else 0.0

    colaboradores_ordenados = sorted(
        colaboradores,
        key=lambda x: (-x["percentual_presenca"], -x["total_presencas"], x["colaborador"])
    )
    ranking_presencas = sorted(
        colaboradores,
        key=lambda x: (-x["total_presencas"], -x["percentual_presenca"], x["colaborador"])
    )[:5]
    ranking_faltas = sorted(
        colaboradores,
        key=lambda x: (-x["total_faltas"], x["colaborador"])
    )[:5]
    ranking_atestados = sorted(
        colaboradores,
        key=lambda x: (-x["total_atestados"], x["colaborador"])
    )[:5]

    return {
        **totais,
        "percentual_presenca_equipe": percentual_presenca_equipe,
        "media_presenca_por_colaborador": media_presenca_por_colaborador,
        "ranking_presencas": ranking_presencas,
        "ranking_faltas": ranking_faltas,
        "ranking_atestados": ranking_atestados,
        "colaboradores_estatisticas": colaboradores_ordenados,
    }




def montar_painel_presenca_mensal(df):
    hoje = datetime.now().date()
    ultimo_dia_mes = monthrange(hoje.year, hoje.month)[1]

    legenda_status = [
        {"codigo": "P", "label": "Presença", "classe": "p"},
        {"codigo": "PH", "label": "Presença com hora extra", "classe": "ph"},
        {"codigo": "HE", "label": "Hora extra", "classe": "he"},
        {"codigo": "F", "label": "Falta", "classe": "f"},
        {"codigo": "AT", "label": "Atestado", "classe": "at"},
        {"codigo": "FE", "label": "Férias", "classe": "fe"},
        {"codigo": "PA", "label": "Presença abonada", "classe": "pa"},
        {"codigo": "FC", "label": "Folga/compensação", "classe": "fc"},
        {"codigo": "FBH", "label": "Banco de horas", "classe": "fbh"},
        {"codigo": "S", "label": "Suspensão", "classe": "s"},
        {"codigo": "AF", "label": "Afastamento", "classe": "af"},
        {"codigo": "DES", "label": "Desligado", "classe": "des"},
        {"codigo": "", "label": "Sem lançamento", "classe": "sem"},
    ]

    if df is None or df.empty:
        return {
            "dias": [],
            "linhas": [],
            "legenda_status": legenda_status,
        }

    mapa_semana = {
        0: "Seg",
        1: "Ter",
        2: "Qua",
        3: "Qui",
        4: "Sex",
        5: "Sáb",
        6: "Dom",
    }

    dias = []
    for dia_num in range(1, ultimo_dia_mes + 1):
        data_ref = date(hoje.year, hoje.month, dia_num)
        dias.append({
            "numero": f"{dia_num:02d}",
            "semana": mapa_semana[data_ref.weekday()],
            "data": data_ref.strftime("%d/%m/%Y"),
            "fim_semana": data_ref.weekday() >= 5,
            "futuro": data_ref > hoje,
        })

    linhas = []
    for _, row in df.iterrows():
        colaborador = safe_str(row.get("COLABORADOR", ""))
        matricula = safe_str(row.get("MATRÍCULA", ""))
        turno = safe_str(row.get("TURNO", ""))
        status_mes = []

        for dia in dias:
            prefixo = dia["data"][:5]
            valor_status = ""
            for coluna in row.index:
                if safe_str(coluna).startswith(prefixo):
                    valor_status = safe_str(row.get(coluna, "")).upper()
                    break

            classe = (valor_status.lower().replace("º", "").replace("°", "") if valor_status else "sem")
            if classe not in {"p", "ph", "he", "f", "at", "fe", "pa", "fc", "fbh", "s", "af", "des", "sem"}:
                classe = "outro"

            status_mes.append({
                "codigo": valor_status,
                "classe": classe,
                "tooltip": f"{dia['data']}: {valor_status or 'Sem lançamento'}",
                "futuro": dia["futuro"],
                "fim_semana": dia["fim_semana"],
            })

        linhas.append({
            "colaborador": colaborador,
            "matricula": matricula,
            "turno": turno,
            "status_mes": status_mes,
        })

    return {
        "dias": dias,
        "linhas": linhas,
        "legenda_status": legenda_status,
    }

def parse_data_br(valor):
    valor = safe_str(valor)
    if not valor:
        return None
    return datetime.strptime(valor, "%d/%m/%Y").date()


def nome_aba_por_data(data_ref):
    return MESES_PT[data_ref.month]


def localizar_coluna_por_data(headers, data_ref):
    prefixo = data_ref.strftime("%d/%m")
    for idx, col in enumerate(headers, start=1):
        if safe_str(col).startswith(prefixo):
            return idx, safe_str(col)
    return None, None


def carregar_planilha_mes_por_data(data_ref):
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(nome_aba_por_data(data_ref))
    valores = ws.get_all_values()
    return ws, valores


def localizar_linha_colaborador_por_data(data_ref, supervisor_nome, matricula):
    ws, valores = carregar_planilha_mes_por_data(data_ref)
    if not valores:
        return ws, None, None, None

    headers = [safe_str(c) for c in valores[0]]
    if "MATRÍCULA" not in headers or "SUPERVISOR" not in headers:
        raise ValueError("Colunas obrigatórias não encontradas na planilha de presença.")

    idx_matricula = headers.index("MATRÍCULA")
    idx_supervisor = headers.index("SUPERVISOR")
    coluna_idx, coluna_nome = localizar_coluna_por_data(headers, data_ref)

    linha_planilha = None
    supervisor_ref = safe_str(supervisor_nome).upper()
    matricula_ref = safe_str(matricula)

    for i, linha in enumerate(valores[1:], start=2):
        mat = safe_str(linha[idx_matricula]) if idx_matricula < len(linha) else ""
        sup = safe_str(linha[idx_supervisor]).upper() if idx_supervisor < len(linha) else ""
        if mat == matricula_ref and sup == supervisor_ref:
            linha_planilha = i
            break

    return ws, linha_planilha, coluna_idx, coluna_nome


def validar_arquivo_atestado(upload):
    if not upload or not upload.filename:
        return "Envie a imagem do atestado."

    tipo = safe_str(getattr(upload, "mimetype", "")).lower()
    permitidos = {"image/jpeg", "image/jpg", "image/png", "image/webp"}
    if tipo not in permitidos:
        return "Envie uma imagem JPG, PNG ou WEBP."

    return None


def salvar_atestado_bd(colaborador, data_referencia, quantidade_dias, observacao, upload, solicitacao_id=None):
    nome_arquivo = secure_filename(upload.filename or "atestado") or "atestado"
    conteudo = upload.read()
    if not conteudo:
        raise ValueError("O arquivo enviado está vazio.")
    if len(conteudo) > MAX_ATESTADO_BYTES:
        raise ValueError(f"A imagem excede o limite de {MAX_ATESTADO_BYTES // (1024 * 1024)} MB.")

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            INSERT INTO {ATESTADOS_TABLE} (
                solicitacao_id,
                matricula,
                colaborador_nome,
                supervisor_usuario,
                supervisor_nome,
                data_referencia,
                quantidade_dias,
                observacao,
                nome_arquivo,
                tipo_arquivo,
                tamanho_bytes,
                arquivo
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, criado_em
            """,
            (
                solicitacao_id,
                colaborador["matricula"],
                colaborador["colaborador"],
                session.get("usuario"),
                session.get("nome"),
                data_referencia,
                quantidade_dias,
                observacao,
                nome_arquivo,
                upload.mimetype,
                len(conteudo),
                psycopg2.Binary(conteudo),
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return row
    finally:
        cur.close()
        conn.close()


def buscar_atestados_supervisor(supervisor_usuario, limite=100):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            SELECT id, solicitacao_id, matricula, colaborador_nome, supervisor_usuario, supervisor_nome,
                   data_referencia, quantidade_dias, observacao, nome_arquivo, tipo_arquivo, tamanho_bytes, criado_em
            FROM {ATESTADOS_TABLE}
            WHERE supervisor_usuario = %s
            ORDER BY criado_em DESC
            LIMIT %s
            """,
            (supervisor_usuario, limite),
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def buscar_atestado_por_id(atestado_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f"SELECT * FROM {ATESTADOS_TABLE} WHERE id = %s LIMIT 1", (atestado_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()

def buscar_atestado_por_solicitacao_id(solicitacao_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"SELECT id, solicitacao_id, data_referencia, quantidade_dias, observacao, nome_arquivo, tipo_arquivo, criado_em FROM {ATESTADOS_TABLE} WHERE solicitacao_id = %s ORDER BY criado_em DESC LIMIT 1",
            (solicitacao_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def formatar_atestados_para_template(atestados):
    itens = []
    for item in atestados or []:
        novo = dict(item)
        data_ref = novo.get("data_referencia")
        criado_em = novo.get("criado_em")
        if isinstance(data_ref, (datetime, date)):
            novo["data_referencia_fmt"] = data_ref.strftime("%d/%m/%Y")
        else:
            novo["data_referencia_fmt"] = formatar_data_segura(data_ref)
        novo["criado_em_fmt"] = formatar_data_segura(criado_em)
        novo["arquivo_url"] = f"/atestados/{novo.get('id')}/arquivo"
        itens.append(novo)
    return itens


def mapear_dados_solicitados(tipo, payload):
    payload = payload or {}

    if tipo == "alterar_linha_ponto":
        return {
            "linha_nova": safe_str(payload.get("linha_nova")),
            "ponto_novo": safe_str(payload.get("ponto_novo")),
        }

    if tipo == "trocar_gestao":
        return {
            "supervisor_novo": safe_str(payload.get("supervisor_novo")),
            "coordenador_novo": safe_str(payload.get("coordenador_novo")),
        }

    if tipo == "solicitar_desligamento":
        return {
            "data_sugerida": safe_str(payload.get("data_sugerida")),
            "motivo": safe_str(payload.get("motivo")),
        }

    if tipo == "solicitar_efetivacao":
        return {
            "cargo_sugerido": safe_str(payload.get("cargo_sugerido")),
            "observacao": safe_str(payload.get("observacao")),
        }

    if tipo == "solicitar_promocao":
        return {
            "cargo_atual": safe_str(payload.get("cargo_atual")),
            "cargo_novo": safe_str(payload.get("cargo_novo")),
            "observacao": safe_str(payload.get("observacao")),
        }

    if tipo == "adicionar_atestado":
        return {
            "data_inicio": safe_str(payload.get("data_inicio")),
            "quantidade_dias": safe_str(payload.get("quantidade_dias")),
            "observacao": safe_str(payload.get("observacao")),
        }

    return payload


def validar_solicitacao(tipo, dados_solicitados, justificativa):
    if tipo not in TIPOS_SOLICITACAO:
        return "Tipo de solicitação inválido."

    if len(justificativa.strip()) < 5:
        return "Informe uma justificativa mais completa."

    if tipo == "alterar_linha_ponto":
        if not dados_solicitados.get("linha_nova") and not dados_solicitados.get("ponto_novo"):
            return "Informe ao menos a nova linha ou o novo ponto."

    elif tipo == "trocar_gestao":
        if not dados_solicitados.get("supervisor_novo"):
            return "Informe o novo supervisor."

    elif tipo == "solicitar_desligamento":
        if not dados_solicitados.get("motivo"):
            return "Informe o motivo do desligamento."

    elif tipo == "solicitar_efetivacao":
        if not dados_solicitados.get("cargo_sugerido"):
            return "Informe o cargo sugerido para efetivação."

    elif tipo == "solicitar_promocao":
        if not dados_solicitados.get("cargo_novo"):
            return "Informe o novo cargo sugerido."

    elif tipo == "adicionar_atestado":
        if not dados_solicitados.get("data_inicio"):
            return "Informe a data inicial do atestado."
        try:
            quantidade = int(str(dados_solicitados.get("quantidade_dias") or "0"))
        except ValueError:
            return "Informe uma quantidade de dias válida."
        if quantidade < 1:
            return "A quantidade de dias deve ser maior que zero."

    return None


def criar_solicitacao_bd(colaborador, tipo, justificativa, dados_solicitados):
    config = TIPOS_SOLICITACAO[tipo]
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            INSERT INTO {SOLICITACOES_TABLE} (
                matricula,
                colaborador_nome,
                solicitado_por_usuario,
                solicitado_por_nome,
                solicitado_por_cargo,
                supervisor_atual,
                tipo_solicitacao,
                destino_setor,
                status,
                dados_anteriores,
                dados_solicitados,
                justificativa,
                data_solicitacao,
                updated_at,
                visualizado_supervisor_em
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDENTE', %s, %s, %s, NOW(), NOW(), NULL)
            RETURNING id
            """,
            (
                colaborador["matricula"],
                colaborador["colaborador"],
                session.get("usuario"),
                session.get("nome"),
                session.get("cargo"),
                colaborador.get("supervisor", ""),
                tipo,
                config["destino"],
                Json({
                    "linha_atual": colaborador.get("linha", ""),
                    "ponto_atual": colaborador.get("ponto", ""),
                    "supervisor_atual": colaborador.get("supervisor", ""),
                    "coordenador_atual": colaborador.get("coordenador", ""),
                    "cargo_atual": colaborador.get("cargo", ""),
                    "area": colaborador.get("area", ""),
                    "setor": colaborador.get("setor", ""),
                }),
                Json(dados_solicitados),
                justificativa,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return row["id"]
    finally:
        cur.close()
        conn.close()

def buscar_solicitacoes(destino_setor=None, status=None, solicitado_por_usuario=None, supervisor_atual=None, limite=100):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filtros = []
        params = []

        if destino_setor:
            filtros.append("UPPER(destino_setor) = UPPER(%s)")
            params.append(destino_setor)
        if status:
            filtros.append("UPPER(status) = UPPER(%s)")
            params.append(status)
        if solicitado_por_usuario:
            filtros.append("solicitado_por_usuario = %s")
            params.append(solicitado_por_usuario)
        if supervisor_atual:
            filtros.append("UPPER(supervisor_atual) = UPPER(%s)")
            params.append(supervisor_atual)

        where = ""
        if filtros:
            where = "WHERE " + " AND ".join(filtros)

        cur.execute(
            f"""
            SELECT
                id,
                matricula,
                colaborador_nome,
                solicitado_por_usuario,
                solicitado_por_nome,
                solicitado_por_cargo,
                supervisor_atual,
                tipo_solicitacao,
                destino_setor,
                status,
                dados_anteriores,
                dados_solicitados,
                justificativa,
                resposta_aprovador,
                aprovado_por_usuario,
                aprovado_por_nome,
                data_solicitacao,
                data_resposta,
                updated_at,
                visualizado_supervisor_em
            FROM {SOLICITACOES_TABLE}
            {where}
            ORDER BY id DESC
            LIMIT %s
            """,
            params + [limite],
        )
        rows = cur.fetchall()
        rows.sort(
            key=lambda item: (
                _normalizar_datetime_com_timezone(
                    item.get("updated_at") or item.get("data_resposta") or item.get("data_solicitacao")
                ) or datetime.min.replace(tzinfo=timezone.utc),
                item.get("id") or 0,
            ),
            reverse=True,
        )
        return rows
    finally:
        cur.close()
        conn.close()

def buscar_solicitacao_por_id(solicitacao_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"SELECT * FROM {SOLICITACOES_TABLE} WHERE id = %s LIMIT 1",
            (solicitacao_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()

def _normalizar_nome_pessoa(valor):
    return " ".join(safe_str(valor).upper().split())


def _linha_eh_do_supervisor(row, nome_supervisor, usuario_supervisor=None):
    nome_linha = _normalizar_nome_pessoa(row.get("COLABORADOR", ""))
    supervisor_nome = _normalizar_nome_pessoa(nome_supervisor)
    usuario_supervisor = safe_str(usuario_supervisor)

    if nome_linha and supervisor_nome and nome_linha == supervisor_nome:
        return True

    matricula_linha = safe_str(row.get("MATRÍCULA", ""))
    if usuario_supervisor and matricula_linha and matricula_linha == usuario_supervisor:
        return True

    return False

def atualizar_status_solicitacao(solicitacao_id, novo_status, resposta_aprovador):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            UPDATE {SOLICITACOES_TABLE}
            SET
                status = %s,
                resposta_aprovador = %s,
                aprovado_por_usuario = %s,
                aprovado_por_nome = %s,
                data_resposta = CURRENT_DATE,
                updated_at = CURRENT_DATE,
                visualizado_supervisor_em = NULL
            WHERE id = %s
            RETURNING id
            """,
            (
                novo_status,
                resposta_aprovador,
                session.get("usuario"),
                session.get("nome"),
                solicitacao_id,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return row
    finally:
        cur.close()
        conn.close()

def label_tipo(tipo):
    return TIPOS_SOLICITACAO.get(tipo, {}).get("label", tipo)


def formatar_data_segura(valor):
    if not valor:
        return "-"

    if isinstance(valor, datetime):
        return valor.strftime("%d/%m/%Y %H:%M")

    if isinstance(valor, str):
        valor = valor.strip()
        if not valor:
            return "-"

        formatos = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d"
        ]

        for fmt in formatos:
            try:
                dt = datetime.strptime(valor, fmt)
                if fmt == "%Y-%m-%d":
                    return dt.strftime("%d/%m/%Y")
                return dt.strftime("%d/%m/%Y %H:%M")
            except ValueError:
                pass

        return valor

    return str(valor)


def _normalizar_datetime_com_timezone(valor):
    if not valor:
        return None
    if isinstance(valor, datetime):
        if valor.tzinfo is None:
            return valor.replace(tzinfo=timezone.utc)
        return valor
    if isinstance(valor, str):
        bruto = valor.strip()
        if not bruto:
            return None
        bruto = bruto.replace('Z', '+00:00')
        try:
            dt = datetime.fromisoformat(bruto)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
        formatos = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
        ]
        for fmt in formatos:
            try:
                dt = datetime.strptime(bruto, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def solicitacao_tem_novidade_para_supervisor(item):
    visualizado = _normalizar_data_sem_hora(item.get("visualizado_supervisor_em"))
    atualizado = _normalizar_data_sem_hora(
        item.get("updated_at") or item.get("data_resposta") or item.get("data_solicitacao")
    )

    if atualizado is None:
        return False

    if visualizado is None:
        return True

    return atualizado > visualizado

def marcar_solicitacoes_visualizadas_supervisor(usuario_supervisor):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            SELECT id, data_solicitacao, data_resposta, updated_at, visualizado_supervisor_em
            FROM {SOLICITACOES_TABLE}
            WHERE solicitado_por_usuario = %s
            """,
            (usuario_supervisor,),
        )
        rows = cur.fetchall()
        ids_para_atualizar = [
            row["id"] for row in rows
            if solicitacao_tem_novidade_para_supervisor(row)
        ]

        if not ids_para_atualizar:
            return 0

        cur.execute(
            f"""
            UPDATE {SOLICITACOES_TABLE}
            SET visualizado_supervisor_em = CURRENT_DATE
            WHERE id = ANY(%s)
            RETURNING id
            """,
            (ids_para_atualizar,),
        )
        atualizadas = cur.fetchall()
        conn.commit()
        return len(atualizadas)
    finally:
        cur.close()
        conn.close()

def formatar_solicitacoes_para_template(solicitacoes):
    saida = []

    for item in solicitacoes:
        item = dict(item)

        item["tipo_label"] = label_tipo(item.get("tipo_solicitacao"))
        item["status_exibicao"] = {
            "PENDENTE": "Aguardando análise",
            "APROVADA": "Aprovada",
            "RECUSADA": "Recusada",
        }.get(safe_str(item.get("status")).upper(), safe_str(item.get("status")) or "-")
        item["data_solicitacao_fmt"] = formatar_data_segura(item.get("data_solicitacao"))
        item["data_resposta_fmt"] = formatar_data_segura(item.get("data_resposta"))
        item["updated_at_fmt"] = formatar_data_segura(item.get("updated_at"))
        item["visualizado_supervisor_em_fmt"] = formatar_data_segura(item.get("visualizado_supervisor_em"))
        item["tem_novidade_supervisor"] = solicitacao_tem_novidade_para_supervisor(item)
        dados_solicitados = item.get("dados_solicitados") or {}
        resumo_extra = []

        if item.get("tipo_solicitacao") == "adicionar_atestado":
            data_inicio = safe_str(dados_solicitados.get("data_inicio"))
            quantidade_dias = safe_str(dados_solicitados.get("quantidade_dias"))
            if data_inicio:
                resumo_extra.append(f"Início: {data_inicio}")
            if quantidade_dias:
                resumo_extra.append(f"Dias: {quantidade_dias}")
            anexo = buscar_atestado_por_solicitacao_id(item.get("id"))
            if anexo:
                item["arquivo_atestado_url"] = f"/atestados/{anexo.get('id')}/arquivo"
                item["arquivo_atestado_nome"] = anexo.get("nome_arquivo") or "Ver atestado"

        item["resumo_extra"] = " • ".join(resumo_extra)
        saida.append(item)

    return saida
def _normalizar_data_sem_hora(valor):
    if not valor:
        return None

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, date):
        return valor

    if isinstance(valor, str):
        bruto = valor.strip()
        if not bruto:
            return None

        bruto = bruto.replace('Z', '+00:00')

        try:
            dt = datetime.fromisoformat(bruto)
            return dt.date()
        except ValueError:
            pass

        formatos = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
        ]

        for fmt in formatos:
            try:
                dt = datetime.strptime(bruto, fmt)
                return dt.date()
            except ValueError:
                continue

    return None
def montar_resumo_solicitacoes(solicitacoes):
    resumo = {"pendentes": 0, "aprovadas": 0, "recusadas": 0, "total": len(solicitacoes), "nao_lidas": 0}

    for item in solicitacoes:
        status = safe_str(item.get("status")).upper()
        if status == "PENDENTE":
            resumo["pendentes"] += 1
        elif status == "APROVADA":
            resumo["aprovadas"] += 1
        elif status == "RECUSADA":
            resumo["recusadas"] += 1

        if solicitacao_tem_novidade_para_supervisor(item):
            resumo["nao_lidas"] += 1

    return resumo

# =========================================================
# PRESENÇA
# =========================================================
@app.route("/salvar_presencas", methods=["POST"])
def salvar_presencas():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        dados = request.get_json(force=True)
        presencas = dados.get("presencas", [])

        if not presencas:
            return jsonify({"ok": False, "msg": "Nenhuma presença recebida."}), 400

        nome_supervisor = session.get("nome")
        usuario_supervisor = session.get("usuario")

        df, ws, coluna_dia = carregar_presenca_supervisor(nome_supervisor)

        if df.empty:
            return jsonify({"ok": False, "msg": "Nenhum colaborador encontrado."}), 404

        if not coluna_dia:
            return jsonify({"ok": False, "msg": "Coluna do dia atual não encontrada."}), 404

        todos_valores = ws.get_all_values()
        cabecalho = [str(c).strip() for c in todos_valores[0]]

        if "MATRÍCULA" not in cabecalho or "SUPERVISOR" not in cabecalho or "COLABORADOR" not in cabecalho:
            return jsonify({"ok": False, "msg": "Colunas obrigatórias não encontradas."}), 404

        col_idx = cabecalho.index(coluna_dia) + 1
        idx_matricula = cabecalho.index("MATRÍCULA")
        idx_supervisor = cabecalho.index("SUPERVISOR")
        idx_colaborador = cabecalho.index("COLABORADOR")

        linhas_por_matricula = {}
        linha_supervisor = None

        supervisor_nome_norm = _normalizar_nome_pessoa(nome_supervisor)

        for i, linha in enumerate(todos_valores[1:], start=2):
            mat = str(linha[idx_matricula]).strip() if idx_matricula < len(linha) else ""
            sup = str(linha[idx_supervisor]).strip().upper() if idx_supervisor < len(linha) else ""
            nome_colab = str(linha[idx_colaborador]).strip() if idx_colaborador < len(linha) else ""

            if sup == str(nome_supervisor).strip().upper():
                if mat:
                    linhas_por_matricula[mat] = i

                nome_colab_norm = _normalizar_nome_pessoa(nome_colab)
                if nome_colab_norm == supervisor_nome_norm:
                    linha_supervisor = i

        atualizacoes = 0

        for item in presencas:
            matricula = str(item.get("matricula", "")).strip()
            status = str(item.get("status", "")).strip().upper()

            if not matricula or status not in STATUS_PRESENCA:
                continue

            linha_planilha = linhas_por_matricula.get(matricula)
            if not linha_planilha:
                continue

            ws.update_cell(linha_planilha, col_idx, status)
            atualizacoes += 1

        if linha_supervisor:
            ws.update_cell(linha_supervisor, col_idx, "P")
            atualizacoes += 1

        return jsonify({
            "ok": True,
            "msg": f"{atualizacoes} presença(s) salva(s) com sucesso. Supervisor marcado automaticamente como P."
        })
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500

@app.route("/presenca")
def presenca():
    if not usuario_supervisor():
        return redirect(url_for("login"))

    try:
        nome_supervisor = session.get("nome")
        df, ws, coluna_dia = carregar_presenca_supervisor(nome_supervisor)

        colaboradores = []
        if not df.empty:
            for _, row in df.iterrows():
                if _linha_eh_do_supervisor(
                    row,
                    nome_supervisor=session.get("nome"),
                    usuario_supervisor=session.get("usuario")
                ):
                    continue

                status_hoje = row.get(coluna_dia, "") if coluna_dia else ""
                estatisticas = calcular_estatisticas_colaborador(row)
                colaboradores.append({
                    "matricula": safe_str(row.get("MATRÍCULA", "")),
                    "colaborador": safe_str(row.get("COLABORADOR", "")),
                    "cargo": safe_str(row.get("CARGO", "")),
                    "area": safe_str(row.get("ÁREA", "")),
                    "cidade": safe_str(row.get("CIDADE", "")),
                    "turno": safe_str(row.get("TURNO", "")),
                    "supervisor": safe_str(row.get("SUPERVISOR", "")),
                    "coordenador": safe_str(row.get("COORDENADOR", "")),
                    "setor": safe_str(row.get("PROCESSO", "")),
                    "linha": safe_str(row.get("LINHA", "")),
                    "ponto": safe_str(row.get("PONTO", "")),
                    "empresa": safe_str(row.get("EMPRESA", "")),
                    "status_hoje": safe_str(status_hoje),
                    "obs_hoje": "",
                    "desligado": safe_str(row.get("STATUS", "")).upper() == "DESLIGADO",
                    **estatisticas,
                })

        matriculas = [c["matricula"] for c in colaboradores if c["matricula"]]
        minhas_solicitacoes = formatar_solicitacoes_para_template(
            buscar_solicitacoes(solicitado_por_usuario=session.get("usuario"), limite=200)
        )
        resumo_solicitacoes = montar_resumo_solicitacoes(minhas_solicitacoes)
        meus_atestados = formatar_atestados_para_template(
            buscar_atestados_supervisor(session.get("usuario"), limite=50)
        )
        supervisores_disponiveis = [
            nome for nome in carregar_supervisores_disponiveis()
            if safe_str(nome).upper() != safe_str(nome_supervisor).upper()
        ]

        return render_template(
            "presenca.html",
            supervisor=nome_supervisor,
            usuario=session.get("usuario"),
            coluna_dia=coluna_dia,
            data_hoje=datetime.now().strftime("%d/%m/%Y"),
            colaboradores=colaboradores,
            matriculas=matriculas,
            status_opcoes=STATUS_PRESENCA,
            tipos_solicitacao=[
                {"valor": k, "label": v["label"], "destino": v["destino"]}
                for k, v in TIPOS_SOLICITACAO.items()
            ],
            supervisores_disponiveis=supervisores_disponiveis,
            minhas_solicitacoes=minhas_solicitacoes,
            resumo_solicitacoes=resumo_solicitacoes,
            meus_atestados=meus_atestados,
        )
    except Exception as e:
        return f"Erro ao carregar presença: {e}"


@app.route("/estatisticas")
def estatisticas_supervisor():
    if not usuario_supervisor():
        return redirect(url_for("login"))

    try:
        nome_supervisor = session.get("nome")
        df, _, _ = carregar_presenca_supervisor(nome_supervisor)
        dados = calcular_estatisticas_equipe(df)
        painel_mensal = montar_painel_presenca_mensal(df)
        return render_template(
            "estatisticas.html",
            supervisor=nome_supervisor,
            usuario=session.get("usuario"),
            data_hoje=datetime.now().strftime("%d/%m/%Y"),
            painel_mensal=painel_mensal,
            **dados,
        )
    except Exception as e:
        return f"Erro ao carregar estatísticas: {e}"


@app.route("/solicitacoes/nova", methods=["POST"])
def nova_solicitacao():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        if request.content_type and "multipart/form-data" in request.content_type.lower():
            payload = {
                "matricula": request.form.get("matricula"),
                "tipo_solicitacao": request.form.get("tipo_solicitacao"),
                "justificativa": request.form.get("justificativa"),
                "dados_solicitados": {
                    "data_inicio": request.form.get("data_inicio"),
                    "quantidade_dias": request.form.get("quantidade_dias"),
                    "observacao": request.form.get("observacao"),
                }
            }
            upload = request.files.get("arquivo")
        else:
            payload = request.get_json(force=True) or {}
            upload = None

        matricula = safe_str(payload.get("matricula"))
        tipo = safe_str(payload.get("tipo_solicitacao"))
        justificativa = safe_str(payload.get("justificativa"))

        colaborador = buscar_colaborador_por_matricula(session.get("nome"), matricula)
        if not colaborador:
            return jsonify({"ok": False, "msg": "Colaborador não encontrado para este supervisor."}), 404

        dados_solicitados = mapear_dados_solicitados(tipo, payload.get("dados_solicitados") or {})
        erro = validar_solicitacao(tipo, dados_solicitados, justificativa)
        if erro:
            return jsonify({"ok": False, "msg": erro}), 400

        if tipo == "adicionar_atestado":
            erro_arquivo = validar_arquivo_atestado(upload)
            if erro_arquivo:
                return jsonify({"ok": False, "msg": erro_arquivo}), 400

            data_inicio = parse_data_br(dados_solicitados.get("data_inicio"))
            if not data_inicio:
                return jsonify({"ok": False, "msg": "Data inicial inválida."}), 400

            hoje = datetime.now().date()
            if data_inicio > hoje:
                return jsonify({"ok": False, "msg": "A data inicial do atestado não pode ser futura."}), 400
            if (hoje - data_inicio).days > 31:
                return jsonify({"ok": False, "msg": "Por enquanto, envie atestados de até 31 dias atrás."}), 400

            quantidade_dias = int(str(dados_solicitados.get("quantidade_dias") or "1"))
            solicitacao_id = criar_solicitacao_bd(colaborador, tipo, justificativa, dados_solicitados)
            registro = salvar_atestado_bd(
                colaborador=colaborador,
                data_referencia=data_inicio,
                quantidade_dias=quantidade_dias,
                observacao=dados_solicitados.get("observacao", ""),
                upload=upload,
                solicitacao_id=solicitacao_id,
            )
            return jsonify({
                "ok": True,
                "msg": f"Solicitação #{solicitacao_id} de atestado enviada com sucesso para ADM.",
                "id": solicitacao_id,
                "anexo_id": registro.get("id"),
            })

        solicitacao_id = criar_solicitacao_bd(colaborador, tipo, justificativa, dados_solicitados)
        return jsonify({
            "ok": True,
            "msg": f"Solicitação #{solicitacao_id} enviada com sucesso para {TIPOS_SOLICITACAO[tipo]['destino']}.",
            "id": solicitacao_id,
        })
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao criar solicitação: {e}"}), 500


@app.route("/solicitacoes/minhas", methods=["GET"])
def minhas_solicitacoes():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        solicitacoes = formatar_solicitacoes_para_template(
            buscar_solicitacoes(solicitado_por_usuario=session.get("usuario"), limite=200)
        )
        return jsonify({
            "ok": True,
            "solicitacoes": solicitacoes,
            "resumo": montar_resumo_solicitacoes(solicitacoes),
        })
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao buscar solicitações: {e}"}), 500


@app.route("/solicitacoes/marcar-visualizadas", methods=["POST"])
def marcar_solicitacoes_visualizadas():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        total = marcar_solicitacoes_visualizadas_supervisor(session.get("usuario"))
        return jsonify({"ok": True, "total_atualizadas": total})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao marcar notificações como visualizadas: {e}"}), 500


@app.route("/atestados/meus", methods=["GET"])
def meus_atestados():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        atestados = formatar_atestados_para_template(
            buscar_atestados_supervisor(session.get("usuario"), limite=100)
        )
        return jsonify({"ok": True, "atestados": atestados})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao buscar atestados: {e}"}), 500


@app.route("/atestados/<int:atestado_id>/arquivo", methods=["GET"])
def baixar_arquivo_atestado(atestado_id):
    if not usuario_logado():
        return redirect(url_for("login"))

    registro = buscar_atestado_por_id(atestado_id)
    if not registro:
        return "Atestado não encontrado.", 404

    if usuario_supervisor() and registro.get("supervisor_usuario") != session.get("usuario"):
        return "Não autorizado.", 403

    arquivo = registro.get("arquivo")
    if arquivo is None:
        return "Arquivo não encontrado.", 404

    if hasattr(arquivo, "tobytes"):
        arquivo = arquivo.tobytes()

    return send_file(
        io.BytesIO(arquivo),
        mimetype=registro.get("tipo_arquivo") or "application/octet-stream",
        as_attachment=False,
        download_name=registro.get("nome_arquivo") or f"atestado_{atestado_id}",
    )


@app.route("/atestados/novo", methods=["POST"])
def novo_atestado():
    return jsonify({"ok": False, "msg": "Use /solicitacoes/nova para enviar atestado para aprovação da ADM."}), 400


# =========================================================
# LOGIN
# =========================================================
@app.route("/")
def login():
    return render_template("login.html")


@app.route("/entrar", methods=["POST"])
def entrar():
    usuario = request.form["usuario"].strip()
    senha = request.form["senha"].strip()

    if usuario == "gerle" and senha == "123":
        session["usuario"] = usuario
        session["tipo"] = "planejamento"
        session["cargo"] = "planejamento"
        session["nome"] = "Gerle"
        return redirect(url_for("planejamento"))

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT id, usuario, senha, cargo, nome, matricula
        FROM perfil
        WHERE usuario = %s AND senha = %s
        LIMIT 1
        """,
        (usuario, senha),
    )
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()

    if not resultado:
        return "Usuário ou senha inválidos"

    session["usuario"] = resultado[1]
    session["tipo"] = "operacao"
    session["cargo"] = resultado[3]
    session["nome"] = resultado[4]
    session["matricula"] = resultado[5]

    cargo = (resultado[3] or "").strip().lower()
    if cargo == "supervisor":
        return redirect(url_for("operacao"))
    if cargo in {"adm", "administracao", "administração", "administrador"}:
        return redirect(url_for("administracao"))
    if cargo in {"rh", "recursos humanos"}:
        return redirect(url_for("rh"))

    return redirect(url_for("operacao"))


# =========================================================
# PLANEJAMENTO
# =========================================================

@app.route("/verify", methods=["GET"])
def verify():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        gc = ensure_gc()
        _ = bool(gc)
        return jsonify({"ok": True, "msg": "Credenciais e conexão OK."})
    except Exception as e:
        return jsonify({"ok": False, "msg": "Falha na verificação.", "detail": str(e)}), 500


@app.route("/run", methods=["POST"])
def run():
    if not usuario_planejamento():
        return jsonify({"ok": False, "log": "Não autorizado."}), 401

    payload = request.get_json(force=True, silent=True) or {}
    tasks = payload.get("tasks") or []
    if isinstance(tasks, str):
        tasks = [tasks]

    tasks = [t.strip() for t in tasks if str(t).strip()]
    if not tasks:
        return jsonify({"ok": False, "log": "Nenhuma tarefa selecionada."}), 400

    datas = []
    if isinstance(payload.get("datas"), list) and payload["datas"]:
        datas = [str(d).strip() for d in payload["datas"] if str(d).strip()]
    elif payload.get("data"):
        datas = [str(payload["data"]).strip()]

    if not datas:
        return jsonify({"ok": False, "log": "Informe ao menos uma data."}), 400

    if _unique_preservando_ordem:
        datas = _unique_preservando_ordem(datas)

    order = [
        "des_qhc", "qhc_base_mae", "act_quadro_fy", "to_planejamento",
        "whs_to", "presenca_abs", "resumo_to", "resumo_abs"
    ]
    tasks_ordered = [t for t in order if t in tasks]
    buf = io.StringIO()

    try:
        gc = ensure_gc()
        with redirect_stdout(buf):
            print(f"[UI] Tarefas selecionadas: {', '.join(tasks_ordered)}")
            for d in datas:
                print(f"[UI] === Data {d} ===")
                for t in tasks_ordered:
                    try:
                        if t == "to_planejamento":
                            if processar_dia:
                                to_percent_cache[d] = processar_dia(gc, d)
                            else:
                                print("[ERRO] processar_dia não encontrada.")
                        elif t == "des_qhc":
                            if des_para_qhc:
                                des_para_qhc(d)
                            else:
                                print("[ERRO] des_para_qhc não encontrada.")
                        elif t == "qhc_base_mae":
                            if qhc_para_base_mae_desligados:
                                qhc_para_base_mae_desligados(gc, d)
                            else:
                                print("[ERRO] qhc_para_base_mae_desligados não encontrada.")
                        elif t == "act_quadro_fy":
                            if atualizar_act_quadro_fy:
                                atualizar_act_quadro_fy(gc, d)
                            else:
                                print("[ERRO] atualizar_act_quadro_fy não encontrada.")
                        elif t == "whs_to":
                            if atualizar_whs_to_percent:
                                to_val = to_percent_cache.get(d)
                                if to_val is None and processar_dia:
                                    to_val = processar_dia(gc, d)
                                    to_percent_cache[d] = to_val
                                atualizar_whs_to_percent(gc, d, to_val)
                            else:
                                print("[ERRO] atualizar_whs_to_percent não encontrada.")
                        elif t == "presenca_abs":
                            if not (abs_buscar_ihc and etapa_lista_para_abs and abs_para_plan and atualizar_whs_abs_percent):
                                print("[ERRO] Funções de ABS não foram encontradas.")
                            else:
                                def _parse_data(dstr):
                                    dd_s, mm_s, yy_s = dstr.split("/")
                                    return date(int(yy_s), int(mm_s), int(dd_s))

                                def _fmt_data(dobj):
                                    return f"{dobj.day:02d}/{dobj.month:02d}/{dobj.year}"

                                def _prev_nao_domingo(dstr):
                                    atual = _parse_data(dstr)
                                    prev = atual - timedelta(days=1)
                                    if prev.weekday() == 6:
                                        prev -= timedelta(days=1)
                                    return _fmt_data(prev)

                                def _rodar_para_data(d_exec):
                                    dd, mm, yyyy = d_exec.split("/")
                                    print(f"\n[ABS] ===== Processando {d_exec} =====")
                                    ihc_ext = abs_buscar_ihc(gc, d_exec)
                                    if ihc_ext is None:
                                        print("[ABS] IHC não encontrado na Base Mãe.")
                                    else:
                                        abs_ws = etapa_lista_para_abs(gc, d_exec, dd, mm, yyyy)
                                        abs_percent_str = abs_para_plan(gc, abs_ws, d_exec, ihc_override=ihc_ext)
                                        atualizar_whs_abs_percent(gc, d_exec, abs_percent_str)

                                hoje = _parse_data(d)
                                wd = hoje.weekday()
                                if wd == 0:
                                    d_sabado = _prev_nao_domingo(d)
                                    d_sexta = _fmt_data(hoje - timedelta(days=3))
                                    _rodar_para_data(d_sexta)
                                    _rodar_para_data(d_sabado)
                                    _rodar_para_data(d)
                                else:
                                    d_anterior = _prev_nao_domingo(d)
                                    _rodar_para_data(d_anterior)
                                    _rodar_para_data(d)
                        elif t == "resumo_to":
                            if to_mes:
                                to_mes(gc, d)
                            else:
                                print("[ERRO] to_mes não encontrada.")
                        elif t == "resumo_abs":
                            if abs_mes:
                                abs_mes(gc)
                            else:
                                print("[ERRO] abs_mes não encontrada.")
                        else:
                            print(f"[ERRO] Task desconhecida: {t}")
                        time.sleep(1)
                    except Exception as e:
                        print(f"[ERRO] Falha em '{t}' ({d}): {e}")
            print("[FIM] Lote concluído.")
    except Exception as e:
        return jsonify({"ok": False, "log": f"Falha geral: {e}\n{buf.getvalue()}"}), 500

    return jsonify({"ok": True, "log": buf.getvalue()}), 200


# =========================================================
# TELAS PRINCIPAIS
# =========================================================
@app.route("/operacao")
def operacao():
    if not usuario_logado():
        return redirect(url_for("login"))
    return render_template("operacao.html")


@app.route("/configuracoes")
def configuracoes():
    if not usuario_logado():
        return redirect(url_for("login"))
    return render_template("configuracoes.html")


@app.route("/administracao")
def administracao():
    if not usuario_adm():
        return redirect(url_for("login"))

    pendentes = formatar_solicitacoes_para_template(buscar_solicitacoes(destino_setor="ADM", status="PENDENTE", limite=200))
    historico = formatar_solicitacoes_para_template(buscar_solicitacoes(destino_setor="ADM", limite=200))
    return render_template(
        "administracao.html",
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        pendentes=pendentes,
        historico=historico,
    )


@app.route("/rh")
def rh():
    if not usuario_rh():
        return redirect(url_for("login"))

    pendentes = formatar_solicitacoes_para_template(buscar_solicitacoes(destino_setor="RH", status="PENDENTE", limite=200))
    historico = formatar_solicitacoes_para_template(buscar_solicitacoes(destino_setor="RH", limite=200))
    return render_template(
        "rh.html",
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        pendentes=pendentes,
        historico=historico,
    )


@app.route("/solicitacoes/<int:solicitacao_id>/decidir", methods=["POST"])
def decidir_solicitacao(solicitacao_id):
    if not (usuario_adm() or usuario_rh()):
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload = request.get_json(force=True) or {}
        acao = safe_str(payload.get("acao")).upper()
        resposta = safe_str(payload.get("resposta"))

        solicitacao = buscar_solicitacao_por_id(solicitacao_id)
        if not solicitacao:
            return jsonify({"ok": False, "msg": "Solicitação não encontrada."}), 404

        if solicitacao.get("status") != "PENDENTE":
            return jsonify({"ok": False, "msg": "Essa solicitação já foi tratada."}), 400

        destino = safe_str(solicitacao.get("destino_setor")).upper()
        if destino == "ADM" and not usuario_adm():
            return jsonify({"ok": False, "msg": "Apenas ADM pode tratar essa solicitação."}), 403
        if destino == "RH" and not usuario_rh():
            return jsonify({"ok": False, "msg": "Apenas RH pode tratar essa solicitação."}), 403

        if acao not in {"APROVAR", "RECUSAR"}:
            return jsonify({"ok": False, "msg": "Ação inválida."}), 400

        novo_status = "APROVADA" if acao == "APROVAR" else "RECUSADA"
        atualizar_status_solicitacao(solicitacao_id, novo_status, resposta)
        return jsonify({"ok": True, "msg": f"Solicitação {novo_status.lower()} com sucesso."})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao tratar solicitação: {e}"}), 500


@app.route("/importar_colaboradores", methods=["POST"])
def importar_colaboradores():
    if not usuario_logado():
        return redirect(url_for("login"))

    arquivo = request.files["arquivo"]
    df = pd.read_excel(arquivo)
    df.columns = df.columns.str.strip()

    df = df[[
        "MATRÍCULA", "COLABORADOR", "COORDENADOR", "SUPERVISOR", "CARGO", "TURNO",
        "ÁREA", "PROCESSO", "STATUS", "Data Admissão", "Data Demissão", "EMPRESA"
    ]]

    df.columns = [
        "matricula", "nome", "coordenador", "supervisor", "cargo", "turno",
        "area", "setor", "status", "data_admissao", "data_demissao", "empresa"
    ]
    df.to_sql("colaboradores", engine, if_exists="append", index=False)
    return "✅ Colaboradores importados com sucesso!"


@app.route("/insumos")
def insumos():
    return "<h1>Tela de Insumos</h1>"


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


if __name__ == "__main__":
    print("Hello")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
