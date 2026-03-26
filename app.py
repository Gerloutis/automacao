import os
import io
import time
import threading
from contextlib import redirect_stdout
from datetime import date, timedelta, datetime
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


def calcular_estatisticas_colaborador(row):
    hoje = datetime.now().date()
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
                data_solicitacao
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'PENDENTE', %s, %s, %s, NOW())
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
                data_resposta
            FROM {SOLICITACOES_TABLE}
            {where}
            ORDER BY data_solicitacao DESC
            LIMIT %s
            """,
            params + [limite],
        )
        return cur.fetchall()
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
                data_resposta = NOW()
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


def montar_resumo_solicitacoes(solicitacoes):
    resumo = {"pendentes": 0, "aprovadas": 0, "recusadas": 0, "total": len(solicitacoes)}

    for item in solicitacoes:
        status = safe_str(item.get("status")).upper()
        if status == "PENDENTE":
            resumo["pendentes"] += 1
        elif status == "APROVADA":
            resumo["aprovadas"] += 1
        elif status == "RECUSADA":
            resumo["recusadas"] += 1

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
        df, ws, coluna_dia = carregar_presenca_supervisor(nome_supervisor)

        if df.empty:
            return jsonify({"ok": False, "msg": "Nenhum colaborador encontrado."}), 404

        if not coluna_dia:
            return jsonify({"ok": False, "msg": "Coluna do dia atual não encontrada."}), 404

        todos_valores = ws.get_all_values()
        cabecalho = [str(c).strip() for c in todos_valores[0]]

        if "MATRÍCULA" not in cabecalho or "SUPERVISOR" not in cabecalho:
            return jsonify({"ok": False, "msg": "Colunas obrigatórias não encontradas."}), 404

        col_idx = cabecalho.index(coluna_dia) + 1
        idx_matricula = cabecalho.index("MATRÍCULA")
        idx_supervisor = cabecalho.index("SUPERVISOR")

        linhas_por_matricula = {}
        for i, linha in enumerate(todos_valores[1:], start=2):
            mat = str(linha[idx_matricula]).strip() if idx_matricula < len(linha) else ""
            sup = str(linha[idx_supervisor]).strip().upper() if idx_supervisor < len(linha) else ""
            if mat and sup == str(nome_supervisor).strip().upper():
                linhas_por_matricula[mat] = i

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

        return jsonify({"ok": True, "msg": f"{atualizacoes} presença(s) salva(s) com sucesso."})
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
        return render_template(
            "estatisticas.html",
            supervisor=nome_supervisor,
            usuario=session.get("usuario"),
            data_hoje=datetime.now().strftime("%d/%m/%Y"),
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
@app.route("/planejamento")
def planejamento():
    if not usuario_planejamento():
        return redirect(url_for("login"))
    return render_template("planejamento.html")


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
