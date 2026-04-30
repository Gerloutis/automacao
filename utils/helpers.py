from datetime import date, datetime


# =========================================================
# STRINGS
# =========================================================

def safe_str(valor):
    """Converte qualquer valor para string limpa (sem None, sem espaços extras)."""
    return str(valor).strip() if valor is not None else ""


def normalizar_texto(valor):
    return safe_str(valor)


def normalizar_situacao(valor):
    v = normalizar_texto(valor).lower()
    mapa = {
        "trabalhando": "trabalhando",
        "afastado":    "afastado",
        "demitido":    "demitido",
        "desligado":   "demitido",
    }
    return mapa.get(v, v)


# =========================================================
# DATAS
# =========================================================

def parse_data_br(valor):
    """Converte string 'dd/mm/yyyy' para objeto date."""
    valor = safe_str(valor)
    if not valor:
        return None
    return datetime.strptime(valor, "%d/%m/%Y").date()


def formatar_data_segura(valor):
    """Formata date/datetime para 'dd/mm/yyyy'. Retorna '' se inválido."""
    if isinstance(valor, (datetime, date)):
        return valor.strftime("%d/%m/%Y")
    try:
        return datetime.fromisoformat(str(valor)).strftime("%d/%m/%Y")
    except Exception:
        return safe_str(valor)


# =========================================================
# TURNOS
# =========================================================

def normalizar_turno(turno):
    """Normaliza variações de turno para T1, T2 ou T3."""
    turno_txt = safe_str(turno).upper().replace("º", "").replace("°", "")
    turno_txt = " ".join(turno_txt.split())

    if turno_txt in {"T1", "1 T", "1T", "1 TURNO", "1 TUR"}:
        return "T1"
    if turno_txt in {"T2", "2 T", "2T", "2 TURNO", "2 TUR"}:
        return "T2"
    if turno_txt in {"T3", "3 T", "3T", "3 TURNO", "3 TUR"}:
        return "T3"
    return turno_txt


def data_valida_para_turno(data_coluna: date, turno: str) -> bool:
    """
    Verifica se uma data deve ser contabilizada para o turno.
    T1/T2: segunda a sábado. T3: domingo a sexta.
    """
    turno_norm = normalizar_turno(turno)
    dia_semana = data_coluna.weekday()  # segunda=0 ... domingo=6

    if turno_norm in {"T1", "T2"}:
        return dia_semana <= 5  # seg a sáb
    if turno_norm == "T3":
        return dia_semana != 5  # dom a sex (sábado fora)
    return True


# =========================================================
# PLANILHA — HEADERS
# =========================================================

def normalizar_headers(valores):
    """
    Recebe os valores brutos da planilha e retorna a primeira linha
    com nomes de coluna únicos (sufixo _N para duplicatas).
    """
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


def extrair_data_coluna(coluna, ano_ref=None):
    """
    Extrai um objeto date do nome da coluna no formato 'dd/mm...'
    Retorna None se o formato não bater.
    """
    nome = safe_str(coluna)
    if len(nome) < 5 or nome[2] != "/":
        return None
    trecho = nome[:5]
    try:
        dia = int(trecho[:2])
        mes = int(trecho[3:5])
        ano = ano_ref or datetime.now().year
        return date(ano, mes, dia)
    except Exception:
        return None
