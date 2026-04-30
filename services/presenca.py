import pandas as pd
from datetime import datetime, date
from calendar import monthrange

from config import PLANILHA_PRESENCA_ID, MESES_PT, STATUS_PRESENCA
from services.sheets import ensure_gc
from utils.helpers import (
    safe_str,
    normalizar_headers,
    extrair_data_coluna,
    data_valida_para_turno,
    normalizar_turno,
)


# =========================================================
# HELPERS INTERNOS
# =========================================================

def _nome_aba_por_data(data_ref: date) -> str:
    return MESES_PT[data_ref.month]


def _nome_aba_mes_atual() -> str:
    return _nome_aba_por_data(datetime.now().date())


def _prefixo_coluna_hoje() -> str:
    return datetime.now().strftime("%d/%m")


def _localizar_coluna_por_data(headers: list, data_ref: date):
    """Retorna (índice_1based, nome_coluna) para a data informada."""
    prefixo = data_ref.strftime("%d/%m")
    for idx, col in enumerate(headers, start=1):
        if safe_str(col).startswith(prefixo):
            return idx, safe_str(col)
    return None, None


# =========================================================
# CARREGAMENTO DA PLANILHA
# =========================================================

def carregar_planilha_mes_por_data(data_ref: date):
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(_nome_aba_por_data(data_ref))
    valores = ws.get_all_values()
    return ws, valores


def carregar_presenca_supervisor(nome_supervisor: str):
    """
    Retorna (DataFrame filtrado pelo supervisor, worksheet, nome_coluna_hoje).
    """
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(_nome_aba_mes_atual())
    valores = ws.get_all_values()

    if not valores or len(valores) < 2:
        return pd.DataFrame(), ws, None

    cabecalho_unico = normalizar_headers(valores)
    df = pd.DataFrame(valores[1:], columns=cabecalho_unico)

    if df.empty:
        return df, ws, None

    if "SUPERVISOR" not in df.columns:
        raise ValueError("Coluna SUPERVISOR não encontrada na planilha.")

    nome_supervisor = safe_str(nome_supervisor).upper()
    df["SUPERVISOR"] = df["SUPERVISOR"].astype(str).str.strip().str.upper()
    filtrado = df[df["SUPERVISOR"] == nome_supervisor].copy()

    coluna_dia = None
    prefixo = _prefixo_coluna_hoje()
    for col in filtrado.columns:
        if safe_str(col).startswith(prefixo):
            coluna_dia = col
            break

    return filtrado, ws, coluna_dia


def carregar_supervisores_disponiveis(data_ref: date = None):
    gc = ensure_gc()
    sh = gc.open_by_key(PLANILHA_PRESENCA_ID)
    ws = sh.worksheet(_nome_aba_por_data(data_ref or datetime.now().date()))
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


def localizar_linha_colaborador_por_data(data_ref: date, supervisor_nome: str, matricula: str):
    ws, valores = carregar_planilha_mes_por_data(data_ref)

    if not valores:
        return ws, None, None, None

    headers = [safe_str(c) for c in valores[0]]

    if "MATRÍCULA" not in headers or "SUPERVISOR" not in headers:
        raise ValueError("Colunas obrigatórias não encontradas na planilha de presença.")

    idx_matricula = headers.index("MATRÍCULA")
    idx_supervisor = headers.index("SUPERVISOR")
    coluna_idx, coluna_nome = _localizar_coluna_por_data(headers, data_ref)

    supervisor_ref = safe_str(supervisor_nome).upper()
    matricula_ref = safe_str(matricula)
    linha_planilha = None

    for i, linha in enumerate(valores[1:], start=2):
        mat = safe_str(linha[idx_matricula]) if idx_matricula < len(linha) else ""
        sup = safe_str(linha[idx_supervisor]).upper() if idx_supervisor < len(linha) else ""
        if mat == matricula_ref and sup == supervisor_ref:
            linha_planilha = i
            break

    return ws, linha_planilha, coluna_idx, coluna_nome


# =========================================================
# ESTATÍSTICAS
# =========================================================

def calcular_estatisticas_colaborador(row) -> dict:
    hoje = datetime.now().date()
    turno = row.get("TURNO", "")

    presenca_codigos = {"P", "PH", "HE"}
    falta_codigos    = {"F"}
    atestado_codigos = {"AT"}
    status_validos   = set(STATUS_PRESENCA) | {"PH"}

    total_presencas = total_faltas = total_atestados = total_lancados = 0

    for coluna in row.index:
        data_coluna = extrair_data_coluna(coluna)
        if not data_coluna or data_coluna > hoje:
            continue
        if not data_valida_para_turno(data_coluna, turno):
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

    percentual = round((total_presencas / total_lancados) * 100, 1) if total_lancados else 0.0

    return {
        "total_presencas":    total_presencas,
        "total_faltas":       total_faltas,
        "total_atestados":    total_atestados,
        "total_lancados":     total_lancados,
        "percentual_presenca": percentual,
    }


def calcular_estatisticas_equipe(df) -> dict:
    colaboradores = []
    totais = {
        "total_colaboradores":     0,
        "colaboradores_ativos":    0,
        "colaboradores_desligados": 0,
        "total_presencas":         0,
        "total_faltas":            0,
        "total_atestados":         0,
        "total_outros":            0,
        "total_lancados":          0,
    }

    vazio = {
        **totais,
        "percentual_presenca_equipe":      0.0,
        "media_presenca_por_colaborador":  0.0,
        "ranking_presencas":               [],
        "ranking_faltas":                  [],
        "ranking_atestados":               [],
        "colaboradores_estatisticas":      [],
    }

    if df is None or df.empty:
        return vazio

    for _, row in df.iterrows():
        est = calcular_estatisticas_colaborador(row)
        outros = max(
            est["total_lancados"] - est["total_presencas"] - est["total_faltas"] - est["total_atestados"],
            0,
        )
        desligado = safe_str(row.get("STATUS", "")).upper() == "DESLIGADO"

        item = {
            "matricula":   safe_str(row.get("MATRÍCULA", "")),
            "colaborador": safe_str(row.get("COLABORADOR", "")),
            "cargo":       safe_str(row.get("CARGO", "")),
            "area":        safe_str(row.get("ÁREA", "")),
            "setor":       safe_str(row.get("PROCESSO", "")),
            "turno":       safe_str(row.get("TURNO", "")),
            "desligado":   desligado,
            "total_outros": outros,
            **est,
        }
        colaboradores.append(item)

        totais["total_colaboradores"] += 1
        totais["colaboradores_desligados" if desligado else "colaboradores_ativos"] += 1
        totais["total_presencas"]  += est["total_presencas"]
        totais["total_faltas"]     += est["total_faltas"]
        totais["total_atestados"]  += est["total_atestados"]
        totais["total_outros"]     += outros
        totais["total_lancados"]   += est["total_lancados"]

    pct_equipe = (
        round((totais["total_presencas"] / totais["total_lancados"]) * 100, 1)
        if totais["total_lancados"] else 0.0
    )
    media = (
        round(totais["total_presencas"] / totais["total_colaboradores"], 1)
        if totais["total_colaboradores"] else 0.0
    )

    ordenados       = sorted(colaboradores, key=lambda x: (-x["percentual_presenca"], -x["total_presencas"], x["colaborador"]))
    ranking_p       = sorted(colaboradores, key=lambda x: (-x["total_presencas"],  -x["percentual_presenca"], x["colaborador"]))[:5]
    ranking_f       = sorted(colaboradores, key=lambda x: (-x["total_faltas"],     x["colaborador"]))[:5]
    ranking_at      = sorted(colaboradores, key=lambda x: (-x["total_atestados"],  x["colaborador"]))[:5]

    return {
        **totais,
        "percentual_presenca_equipe":     pct_equipe,
        "media_presenca_por_colaborador": media,
        "ranking_presencas":              ranking_p,
        "ranking_faltas":                 ranking_f,
        "ranking_atestados":              ranking_at,
        "colaboradores_estatisticas":     ordenados,
    }


def montar_painel_presenca_mensal(df) -> dict:
    hoje = datetime.now().date()
    ultimo_dia_mes = monthrange(hoje.year, hoje.month)[1]

    legenda_status = [
        {"codigo": "P",   "label": "Presença",                "classe": "p"},
        {"codigo": "PH",  "label": "Presença com hora extra", "classe": "ph"},
        {"codigo": "HE",  "label": "Hora extra",              "classe": "he"},
        {"codigo": "F",   "label": "Falta",                   "classe": "f"},
        {"codigo": "AT",  "label": "Atestado",                "classe": "at"},
        {"codigo": "FE",  "label": "Férias",                  "classe": "fe"},
        {"codigo": "PA",  "label": "Presença abonada",        "classe": "pa"},
        {"codigo": "FC",  "label": "Folga/compensação",       "classe": "fc"},
        {"codigo": "FBH", "label": "Banco de horas",          "classe": "fbh"},
        {"codigo": "S",   "label": "Suspensão",               "classe": "s"},
        {"codigo": "AF",  "label": "Afastamento",             "classe": "af"},
        {"codigo": "DES", "label": "Desligado",               "classe": "des"},
        {"codigo": "",    "label": "Sem lançamento",          "classe": "sem"},
    ]

    if df is None or df.empty:
        return {"dias": [], "linhas": [], "legenda_status": legenda_status}

    mapa_semana = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}

    dias = []
    for dia_num in range(1, ultimo_dia_mes + 1):
        data_ref = date(hoje.year, hoje.month, dia_num)
        dias.append({
            "numero":    f"{dia_num:02d}",
            "semana":    mapa_semana[data_ref.weekday()],
            "data":      data_ref.strftime("%d/%m/%Y"),
            "fim_semana": data_ref.weekday() >= 5,
            "futuro":    data_ref > hoje,
        })

    classes_validas = {"p", "ph", "he", "f", "at", "fe", "pa", "fc", "fbh", "s", "af", "des", "sem"}
    linhas = []

    for _, row in df.iterrows():
        status_mes = []
        for dia in dias:
            prefixo = dia["data"][:5]
            valor_status = ""
            for coluna in row.index:
                if safe_str(coluna).startswith(prefixo):
                    valor_status = safe_str(row.get(coluna, "")).upper()
                    break

            classe = valor_status.lower().replace("º", "").replace("°", "") if valor_status else "sem"
            if classe not in classes_validas:
                classe = "outro"

            status_mes.append({
                "codigo":    valor_status,
                "classe":    classe,
                "tooltip":   f"{dia['data']}: {valor_status or 'Sem lançamento'}",
                "futuro":    dia["futuro"],
                "fim_semana": dia["fim_semana"],
            })

        linhas.append({
            "colaborador": safe_str(row.get("COLABORADOR", "")),
            "matricula":   safe_str(row.get("MATRÍCULA", "")),
            "turno":       safe_str(row.get("TURNO", "")),
            "status_mes":  status_mes,
        })

    return {"dias": dias, "linhas": linhas, "legenda_status": legenda_status}


def buscar_colaborador_por_matricula(nome_supervisor: str, matricula: str):
    df, _, coluna_dia = carregar_presenca_supervisor(nome_supervisor)
    if df.empty:
        return None

    matricula = safe_str(matricula)
    for _, row in df.iterrows():
        if safe_str(row.get("MATRÍCULA", "")) == matricula:
            estatisticas = calcular_estatisticas_colaborador(row)
            return {
                "matricula":    safe_str(row.get("MATRÍCULA", "")),
                "colaborador":  safe_str(row.get("COLABORADOR", "")),
                "cargo":        safe_str(row.get("CARGO", "")),
                "area":         safe_str(row.get("ÁREA", "")),
                "cidade":       safe_str(row.get("CIDADE", "")),
                "turno":        safe_str(row.get("TURNO", "")),
                "supervisor":   safe_str(row.get("SUPERVISOR", "")),
                "coordenador":  safe_str(row.get("COORDENADOR", "")),
                "setor":        safe_str(row.get("PROCESSO", "")),
                "linha":        safe_str(row.get("LINHA", "")),
                "ponto":        safe_str(row.get("PONTO", "")),
                "empresa":      safe_str(row.get("EMPRESA", "")),
                "status_hoje":  safe_str(row.get(coluna_dia, "")) if coluna_dia else "",
                "desligado":    safe_str(row.get("STATUS", "")).upper() == "DESLIGADO",
                **estatisticas,
            }
    return None
