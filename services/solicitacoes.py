from datetime import datetime, date, timezone
from psycopg2.extras import Json, RealDictCursor

from config import SOLICITACOES_TABLE, TIPOS_SOLICITACAO
from database import get_connection
from utils.helpers import safe_str, formatar_data_segura


# =========================================================
# MAPEAMENTO E VALIDAÇÃO
# =========================================================

def mapear_dados_solicitados(tipo: str, payload: dict) -> dict:
    payload = payload or {}

    mapa = {
        "alterar_linha_ponto":   ["linha_nova", "ponto_novo"],
        "trocar_gestao":         ["supervisor_novo", "coordenador_novo"],
        "solicitar_desligamento": ["data_sugerida", "motivo"],
        "solicitar_efetivacao":  ["cargo_sugerido", "observacao"],
        "solicitar_promocao":    ["cargo_atual", "cargo_novo", "observacao"],
        "adicionar_atestado":    ["data_inicio", "quantidade_dias", "observacao"],
    }

    campos = mapa.get(tipo, list(payload.keys()))
    return {campo: safe_str(payload.get(campo)) for campo in campos}


def validar_solicitacao(tipo: str, dados_solicitados: dict, justificativa: str):
    """Retorna mensagem de erro ou None se válido."""
    if tipo not in TIPOS_SOLICITACAO:
        return "Tipo de solicitação inválido."

    if len(justificativa.strip()) < 5:
        return "Informe uma justificativa mais completa."

    regras = {
        "alterar_linha_ponto":   lambda d: not d.get("linha_nova") and not d.get("ponto_novo"),
        "trocar_gestao":         lambda d: not d.get("supervisor_novo"),
        "solicitar_desligamento": lambda d: not d.get("motivo"),
        "solicitar_efetivacao":  lambda d: not d.get("cargo_sugerido"),
        "solicitar_promocao":    lambda d: not d.get("cargo_novo"),
    }

    if tipo in regras and regras[tipo](dados_solicitados):
        mensagens = {
            "alterar_linha_ponto":   "Informe ao menos a nova linha ou o novo ponto.",
            "trocar_gestao":         "Informe o novo supervisor.",
            "solicitar_desligamento": "Informe o motivo do desligamento.",
            "solicitar_efetivacao":  "Informe o cargo sugerido para efetivação.",
            "solicitar_promocao":    "Informe o novo cargo sugerido.",
        }
        return mensagens[tipo]

    if tipo == "adicionar_atestado":
        if not dados_solicitados.get("data_inicio"):
            return "Informe a data inicial do atestado."
        try:
            quantidade = int(str(dados_solicitados.get("quantidade_dias") or "0"))
        except ValueError:
            return "Informe uma quantidade de dias válida."
        if quantidade < 1:
            return "A quantidade de dias deve ser maior que zero."

    return None


# =========================================================
# CRUD
# =========================================================

def criar_solicitacao_bd(colaborador: dict, tipo: str, justificativa: str, dados_solicitados: dict, session: dict) -> int:
    config = TIPOS_SOLICITACAO[tipo]
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(
            f"""
            INSERT INTO {SOLICITACOES_TABLE} (
                matricula, colaborador_nome,
                solicitado_por_usuario, solicitado_por_nome, solicitado_por_cargo,
                supervisor_atual, tipo_solicitacao, destino_setor, status,
                dados_anteriores, dados_solicitados, justificativa,
                data_solicitacao, updated_at, visualizado_supervisor_em
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
                    "linha_atual":       colaborador.get("linha", ""),
                    "ponto_atual":       colaborador.get("ponto", ""),
                    "supervisor_atual":  colaborador.get("supervisor", ""),
                    "coordenador_atual": colaborador.get("coordenador", ""),
                    "cargo_atual":       colaborador.get("cargo", ""),
                    "area":              colaborador.get("area", ""),
                    "setor":             colaborador.get("setor", ""),
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


def buscar_solicitacoes(
    destino_setor=None,
    status=None,
    solicitado_por_usuario=None,
    supervisor_atual=None,
    limite=100,
) -> list:
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        filtros, params = [], []

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

        where = ("WHERE " + " AND ".join(filtros)) if filtros else ""

        cur.execute(
            f"""
            SELECT
                id, matricula, colaborador_nome,
                solicitado_por_usuario, solicitado_por_nome, solicitado_por_cargo,
                supervisor_atual, tipo_solicitacao, destino_setor, status,
                dados_anteriores, dados_solicitados, justificativa,
                resposta_aprovador, aprovado_por_usuario, aprovado_por_nome,
                data_solicitacao, data_resposta, updated_at, visualizado_supervisor_em
            FROM {SOLICITACOES_TABLE}
            {where}
            ORDER BY data_solicitacao DESC
            LIMIT %s
            """,
            params + [limite],
        )
        rows = cur.fetchall()
        rows.sort(
            key=lambda item: (
                _normalizar_datetime_tz(
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


def buscar_solicitacao_por_id(solicitacao_id: int):
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


def atualizar_status_solicitacao(solicitacao_id: int, novo_status: str, resposta_aprovador: str, session: dict):
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


def marcar_solicitacoes_visualizadas_supervisor(usuario_supervisor: str) -> int:
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
        ids_para_atualizar = [row["id"] for row in rows if solicitacao_tem_novidade_para_supervisor(row)]

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


def label_tipo(tipo: str) -> str:
    return TIPOS_SOLICITACAO.get(tipo, {}).get("label", tipo)


def montar_resumo_solicitacoes(solicitacoes: list) -> dict:
    resumo = {"pendentes": 0, "aprovadas": 0, "recusadas": 0, "total": len(solicitacoes), "nao_lidas": 0}
    for item in solicitacoes:
        status = safe_str(item.get("status")).upper()
        if status == "PENDENTE":   resumo["pendentes"] += 1
        elif status == "APROVADA": resumo["aprovadas"] += 1
        elif status == "RECUSADA": resumo["recusadas"] += 1
        if solicitacao_tem_novidade_para_supervisor(item):
            resumo["nao_lidas"] += 1
    return resumo


def formatar_solicitacoes_para_template(solicitacoes: list) -> list:
    from services.atestados import buscar_atestado_por_solicitacao_id
    saida = []
    for item in solicitacoes:
        item = dict(item)
        item["tipo_label"]       = label_tipo(item.get("tipo_solicitacao"))
        item["status_exibicao"]  = {
            "PENDENTE": "Aguardando análise",
            "APROVADA": "Aprovada",
            "RECUSADA": "Recusada",
        }.get(safe_str(item.get("status")).upper(), safe_str(item.get("status")) or "-")
        item["data_solicitacao_fmt"]         = formatar_data_segura(item.get("data_solicitacao"))
        item["data_resposta_fmt"]            = formatar_data_segura(item.get("data_resposta"))
        item["updated_at_fmt"]               = formatar_data_segura(item.get("updated_at"))
        item["visualizado_supervisor_em_fmt"] = formatar_data_segura(item.get("visualizado_supervisor_em"))
        item["tem_novidade_supervisor"]      = solicitacao_tem_novidade_para_supervisor(item)

        dados_solicitados = item.get("dados_solicitados") or {}
        resumo_extra = []

        if item.get("tipo_solicitacao") == "adicionar_atestado":
            data_inicio    = safe_str(dados_solicitados.get("data_inicio"))
            quantidade_dias = safe_str(dados_solicitados.get("quantidade_dias"))
            if data_inicio:     resumo_extra.append(f"Início: {data_inicio}")
            if quantidade_dias: resumo_extra.append(f"Dias: {quantidade_dias}")
            anexo = buscar_atestado_por_solicitacao_id(item.get("id"))
            if anexo:
                item["arquivo_atestado_url"]  = f"/atestados/{anexo.get('id')}/arquivo"
                item["arquivo_atestado_nome"] = anexo.get("nome_arquivo") or "Ver atestado"

        item["resumo_extra"] = " • ".join(resumo_extra)
        saida.append(item)
    return saida


# =========================================================
# HELPERS INTERNOS
# =========================================================

def _normalizar_data_sem_hora(valor):
    if not valor:
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    if isinstance(valor, str):
        bruto = valor.strip().replace("Z", "+00:00")
        if not bruto:
            return None
        try:
            return datetime.fromisoformat(bruto).date()
        except ValueError:
            pass
        for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
            try:
                return datetime.strptime(bruto, fmt).date()
            except ValueError:
                continue
    return None


def _normalizar_datetime_tz(valor):
    if not valor:
        return None
    if isinstance(valor, datetime):
        return valor if valor.tzinfo else valor.replace(tzinfo=timezone.utc)
    if isinstance(valor, str):
        bruto = valor.strip().replace("Z", "+00:00")
        if not bruto:
            return None
        try:
            dt = datetime.fromisoformat(bruto)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        for fmt in ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.strptime(bruto, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def solicitacao_tem_novidade_para_supervisor(item: dict) -> bool:
    visualizado = _normalizar_data_sem_hora(item.get("visualizado_supervisor_em"))
    atualizado  = _normalizar_data_sem_hora(
        item.get("updated_at") or item.get("data_resposta") or item.get("data_solicitacao")
    )
    if atualizado is None:
        return False
    if visualizado is None:
        return True
    return atualizado > visualizado
