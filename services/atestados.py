import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.utils import secure_filename
from datetime import date, datetime

from config import ATESTADOS_TABLE, MAX_ATESTADO_BYTES
from database import get_connection
from utils.helpers import safe_str, formatar_data_segura


# =========================================================
# VALIDAÇÃO
# =========================================================

def validar_arquivo_atestado(upload) -> str | None:
    """Retorna mensagem de erro ou None se válido."""
    if not upload or not upload.filename:
        return "Envie a imagem do atestado."
    tipo = safe_str(getattr(upload, "mimetype", "")).lower()
    if tipo not in {"image/jpeg", "image/jpg", "image/png", "image/webp"}:
        return "Envie uma imagem JPG, PNG ou WEBP."
    return None


# =========================================================
# CRUD
# =========================================================

def salvar_atestado_bd(
    colaborador: dict,
    data_referencia: date,
    quantidade_dias: int,
    observacao: str,
    upload,
    session: dict,
    solicitacao_id: int = None,
) -> dict:
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
                solicitacao_id, matricula, colaborador_nome,
                supervisor_usuario, supervisor_nome,
                data_referencia, quantidade_dias, observacao,
                nome_arquivo, tipo_arquivo, tamanho_bytes, arquivo
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


def buscar_atestados_supervisor(supervisor_usuario: str, limite: int = 100) -> list:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            SELECT id, solicitacao_id, matricula, colaborador_nome,
                   supervisor_usuario, supervisor_nome, data_referencia,
                   quantidade_dias, observacao, nome_arquivo, tipo_arquivo,
                   tamanho_bytes, criado_em
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


def buscar_atestado_por_id(atestado_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(f"SELECT * FROM {ATESTADOS_TABLE} WHERE id = %s LIMIT 1", (atestado_id,))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


def buscar_atestado_por_solicitacao_id(solicitacao_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            SELECT id, solicitacao_id, data_referencia, quantidade_dias,
                   observacao, nome_arquivo, tipo_arquivo, criado_em
            FROM {ATESTADOS_TABLE}
            WHERE solicitacao_id = %s
            ORDER BY criado_em DESC
            LIMIT 1
            """,
            (solicitacao_id,),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()


# =========================================================
# FORMATAÇÃO PARA TEMPLATE
# =========================================================

def formatar_atestados_para_template(atestados: list) -> list:
    itens = []
    for item in atestados or []:
        novo = dict(item)
        data_ref  = novo.get("data_referencia")
        criado_em = novo.get("criado_em")
        novo["data_referencia_fmt"] = (
            data_ref.strftime("%d/%m/%Y")
            if isinstance(data_ref, (datetime, date))
            else formatar_data_segura(data_ref)
        )
        novo["criado_em_fmt"] = formatar_data_segura(criado_em)
        novo["arquivo_url"]   = f"/atestados/{novo.get('id')}/arquivo"
        itens.append(novo)
    return itens
