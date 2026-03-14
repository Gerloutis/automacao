#-------------------------------------------------------#
#   Título: Automátização Planejamento Fisia            #
#   Autor: Antonio Gerle Teofilo da Silva               #
#   Data de Inicio: 26/11/2025                          #
#   Última modificação: 27/11/2025                      #
#-------------------------------------------------------#

# ========== IMPORTAÇÃO DE PACOTES ==========

import os
import re
import time
import gspread
import unicodedata
import numpy as np
import json, base64
import pandas as pd
from zoneinfo import ZoneInfo
from decimal import Decimal, getcontext
from datetime import date, datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.credentials import Credentials as UserCredentials

# ========== CREDENCIAIS EMBUTIDAS ==========

CREDENCIAL = os.environ.get("GOOGLE_CREDENTIALS")

DEPENDENCIAS = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

def _creds_embutidas():
    
    data = CREDENCIAL.strip()
    info = json.loads(data)
    
    return Credentials.from_service_account_info(info, scopes=DEPENDENCIAS)

def _obter_creds():
    
    try:
        return _creds_embutidas()
    except Exception:
        print("Credencial não obtida ")

# ========== URLs DE PLANILHAS ACESSADAS ==========

PLANILHA_PRE_URL = "https://docs.google.com/spreadsheets/d/1Qv9mI_vo0yA987Kabn-bUM6XaQq2IOs4dLZKAzwU8P8/edit" # Planilha de LISTA DE PRESENÇA (Seleção de ABA automática)

PLANILHA_WHS_URL = "https://docs.google.com/spreadsheets/d/1LcLBGg6JzINJa5rhaVNJC9HdhBKt9rrlXb7bFbycG14/edit" # Planilha Warehouse Meeting - Planejamento (Seleção de ABA automática)

PLANILHA_ABS_URL = "https://docs.google.com/spreadsheets/d/1sG0AYIe7ap7pzYh4S9-cvuTkKF7AKzb7E7kVzZOVVko/edit" # Planilha ABS FY
ABA_ABS = "ABS" # Seleção de ABA

PLANILHA_MAE_URL = "https://docs.google.com/spreadsheets/d/1M44kE_8flXkl450ubcEp8cuVvwI4M5DcQYT0Q-a2TOo/edit" 
ABA_TO = "Quadro FY - V2" # Seleção de ABA

PLANILHA_TO_URL = "https://docs.google.com/spreadsheets/d/1ATLx_YeBcDZSp6jRlg-9GbULVRmy5ymhJEzX_IClKnc/edit"
ABA_TO_FY = "TO"

PLANILHA_DES_URL = "https://docs.google.com/spreadsheets/d/1iZmRoxV7vEnLqqjPuO0s78wJeVuhLBDWANge9ffP9CQ/edit"
ABA_DES_LIST = ["Fênix / FISIA", "Sertec/FISIA", "Mendes/ FISIA"]

PLANILHA_QHC_URL = "https://docs.google.com/spreadsheets/d/1wxwncI3t62D6vkIGiazVR-hoGnBCW2-CBgZJB5aruZ8/edit"
ABA_QHC = "Ativos Setembro 2025"

PLANILHA_DADOS_DESL_URL = "https://docs.google.com/spreadsheets/d/1IMduv1Qxn9AA4--bQV3twurWGVnA-Y0CI21sqJm5qpM/edit"
ABA_DADOS_BLOQUEIO = "Bloqueio de Acesso"

PLANILHA_TO_FY = "https://docs.google.com/spreadsheets/d/1ATLx_YeBcDZSp6jRlg-9GbULVRmy5ymhJEzX_IClKnc/edit"
ABA_TO_FY_MES = "Resumo Mensal"

PLANILHA_DIARISTAS_ID = "1vwtDxR7fyAYPDy7vKVk_cWYevXz4Ixuh14j6wwPwanA"
ABA_DIARISTAS = "Controle 2026"

PLANILHA_HEADCOUNT_ID = "1wxwncI3t62D6vkIGiazVR-hoGnBCW2-CBgZJB5aruZ8"
ABA_HEADCOUNT = "H.C. TT"

PLANILHA_BASEMAE_ID = "1M44kE_8flXkl450ubcEp8cuVvwI4M5DcQYT0Q-a2TOo"
ABA_DESTINO = "Refeitório"

ABA_DESLIGADOS = "Desligados"

ABA_PLA_ABS = "Resumo ABS" # Seleção de ABA (Planejamento ABS)
ABA_PLA_TO = "Resumo TO" # Seleção de ABA (Planejamento TO)

ABS_cabecalho = ["MATRICULA","NOME","COORDENADOR","SUPERVISOR","ÁREA","SETOR","CARGO","TURNO","EMPRESA","CIDADE","PONTO","LINHA","DATA DEMISSÃO","DATA","STATUS","SIGLA","MES","CONTRATO","CANAL"]

PLAN_cabecalho_ini = ["DATA","MES","I.H.C","ABS","ABS(%)","FALTAS","ATESTADOS","EFETIVOS","TEMPORÁRIOS","MOI","MOD","MOD+","1º","2º","3º","4º","5º","ADM"]
CIDADES_FIXAS = [
    "BRAGANÇA PAULISTA","VARGEM","EXTREMA","ITAPEVA","JOANOPOLIS","CAMANDUCAIA",
    "PIRACAIA","PINHALZINHO","GUARULHOS","ATIBAIA"
]
CIDADES_COLUNAS = CIDADES_FIXAS + ["OUTRAS CIDADES"]
PLAN_cab_canais    = ["INBOUND","DIGITAL","NIKESTORE","REVERSA","WHOLESALE","ROTA SP","OUTROS CANAIS"]
PLAN_agencias = ["ADECCO","DPX","FISIA","FENIX","SERTEC","MENDES"]    

UF_SUFIXO = re.compile(r"\s*-?\s*(SP|MG)$")

ORDEM_MESES = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]

getcontext().prec = 12

DECIMAIS = 4  

forc = {"SOLICITACAO DA EMPRESA", "DISPENSA"}
espo = {"PEDIDO","PEDIDO DE DEMISSAO","ABANDONO"}
correcoes = {
    "ESPOTANEO": "ESPONTANEO",
    "ESPOANTANEO": "ESPONTANEO",
    "ESPOTÂNEO" : "ESPONTANEO"
}
carry_accum = []
# ====================== PLANEJAMENTO TO =======================
PLAN_DES_cab          = ["DATA","GESTOR","CD","MATRÍCULA","COLABORADOR DESLIGADO","C/C","CARGO","TURNO","MOTIVO DO DESLIGAMENTO","SOLICITAÇÃO PEDIDO"]
PLAN_TO_cabecalho_ini = ["MES","DATA","I.H.C","TO","TO(%)","EFETIVOS","TEMPORÁRIOS","ESPONTÂNEO","FORÇADO","MASCULINO","FEMININO","MOI","MOD","MOD+","1º","2º","3º","4º","5º","ADM","SEM DADOS"]
PLAN_TO_cabecalho_fim = ["ADECCO","DPX","FISIA","FÊNIX","SERTEC","MENDES"]
PLAN_TO_cab_canais    = ["INBOUND","DIGITAL","NIKESTORE","REVERSA","WHOLESALE","ROTA SP","OUTROS CANAIS"]
PLAN_TO_cab_cidades   = ["ATIBAIA","BRAGANÇA PAULISTA","VARGEM","EXTREMA","ITAPEVA","JOANÓPOLIS","CAMANDUCAIA","PIRACAIA","PINHALZINHO","GUARULHOS","OUTRAS CIDADES"]
PLAN_TO_cab_adic      = ["EFETIVO ESPONTÂNEO","EFETIVO FORÇADO","TEMP ESPONTÂNEO","TEMP FORÇADO","EFETIVO ESPONTÂNEO(%)","EFETIVO FORÇADO(%)","TEMPORÁRIO ESPONTÂNEO(%)","TEMPORÁRIO FORÇADO(%)"     ]

AGENCIA_KEYS = ["ADECCO","DPX","FISIA","FÊNIX","SERTEC","MENDES"]


# ========== DADOS DE CARGOS ==========

MOI = {"ANALISTA", "ANALISTA SENIOR", "ASSISTENTE LOGISTICO", "SUPERVISOR"}
MOD = {"ASSIST. DEPOSITO", "ASSISTENTE DEPOSITO", "ASSISTENTE DE DEPÓSITO"}
MOD_M = {"CONFERENTE", "TECNICO DE PERSONALIZACAO", "OPERADOR DE EMPILHADEIRA","ASSIST. OPERACAO CD", "ASSIST. DEVOLUCAO","ARTILHEIRO PERSON"}

# ========== FUNÇÕES AUXILIARES ==========

def achar_coluna_data_whs(cabecalho, dd, mm, yyyy):
    
    padroes = (f"{dd}/{mm}", f"{dd}/{mm}/{yyyy}", f"{dd}/{mm} -")
    
    for j, h in enumerate(cabecalho):
        h = (h or "").strip()
        if not h:
            continue
        if any(h.startswith(p) for p in padroes):
            return j
        if re.search(rf"\\b{dd}/{mm}(\\b|/\\d{{4}}\\b)", h):
            return j
    return None

def _unique_preservando_ordem(seq):
    seen = set()
    saida = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            saida.append(x)
    return saida
 
def normaliza(s: str) -> str:
    s = (s or "").strip().upper() 
    return ''.join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def normaliza_empresa():
    print("oi")

def normaliza_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .replace(correcoes, regex=False)
         .map(lambda x: normaliza(x or ""))
    )

def cidade_canonica(c: str) -> str:
    n = cidade_normalizada(c)
    if n in PLAN_TO_cab_cidades:
        return n
    hit = _CIDADES_CANON.get(normaliza(n))
    return hit if hit else OUTRAS_LABEL

def cidade_normalizada(nome: str) -> str:
    nome = normaliza(nome)
    nome = " ".join(nome.split())
    nome = UF_SUFIXO.sub("", nome).strip()
    equivalencias = {
        "ATIBAIA": "ATIBAIA",
        "BRAGANCA": "BRAGANCA PAULISTA",
        "BRAGANCA PAULISTA": "BRAGANCA PAULISTA",
        "BRAGANÇA PAULISTA": "BRAGANCA PAULISTA",
        "CAMADUCAIA": "CAMANDUCAIA",
        "CAMANDUCAIA": "CAMANDUCAIA",
        "EXTREMA": "EXTREMA",
        "GUARAIUVA": "VARGEM",
        "GUARULHOS": "GUARULHOS",
        "ITAPEVA": "ITAPEVA",
        "JOANOPOLIS": "JOANOPOLIS",
        "PINHALZINHO": "PINHALZINHO",
        "VARGEM": "VARGEM",
        "PIRACAIA": "PIRACAIA",
    }

    return equivalencias.get(nome, nome)
    
NAO_ESCALADO = {}
NAO_ESCALADO_N = {normaliza(x) for x in NAO_ESCALADO}

def nome_mes_pt(dt: datetime) -> str:
    meses = ["Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro"]
    return meses[dt.month - 1]

def classificar_grupo(cargo: str) -> str:
    c = normaliza(cargo).replace('.', '').replace('-', ' ')
    for k in MOI:
        if normaliza(k) in c:
            return "MOI"
    for k in MOD:
        if normaliza(k) in c:
            return "MOD"
    for k in MOD_M:
        if normaliza(k) in c:
            return "MOD+"
    if ("ASSIST" in c) and ("DEPOS" in c):
        return "MOD"
    return "MOD+"

def classificar_regime(empresa: str) -> str:
    empresas_efetivo = {"1080", "1081", "7006", "2103"}
    return "EFETIVO" if empresa in empresas_efetivo else "TEMPORÁRIO"

def turno_normalizado(t: str) -> str:
    t = normaliza(t).replace("°","º")
    if "1" in t: return "1º"
    if "2" in t: return "2º"
    if "3" in t: return "3º"
    if "4" in t: return "4º"
    if "5" in t: return "5º"
    return t or ""

# Monta fórmula para nomeclatura do canal
def montar_formula_canal(area_col_letter: str, row: int) -> str:
    ac = f"{area_col_letter}{row}"
    return (
        f'=SE({ac}="Inbound";"Inbound";'
        f'SE(OU({ac}="Armazenagem - Digital";{ac}="Outbound - Digital";{ac}="Gestão de Pedidos - Digital";'
        f'{ac}="Auditoria / Inventário";{ac}="DIGITAL");"Digital";'
        f'SE(OU({ac}="Armazenagem - Nike Store";{ac}="Outbound - Nike Store";{ac}="Gestão de Pedidos - Nike Store";'
        f'{ac}="NIKESTORE");"NikeStore";'
        f'SE(OU({ac}="Projeto - IF";{ac}="WHOLESALE");"Wholesale";'
        f'SE({ac}="TSP - Nike Store";"Rota SP";'
        f'SE({ac}="ROTA SP";"Rota SP";'
        f'SE({ac}="Auditoria / Inventário";"Digital";'
        f'SE(OU({ac}="Reversa - Nike Store";{ac}="Reversa - Digital");"Reversa";"VER"))))))))'
    )

def agencia_normalizada(a: str) -> str:
    a_n = normaliza(a)
    if "ADECCO" in a_n:  return "ADECCO"
    if "DPX" in a_n:    return "DPX"
    if "FISIA" in a_n:   return "FISIA"
    if "FENIX" in a_n:   return "FÊNIX"
    if "SERTEC" in a_n:  return "SERTEC"
    if "MENDES" in a_n:  return "MENDES"
    return a

def genero_normalizado(g: str) -> str:
    g = normaliza(g)
    if g.startswith("MASC"): return "MASCULINO"
    if g.startswith("FEM"):  return "FEMININO"
    return ""

# Normaliza o nome do canal
def canal_normalizado(c: str) -> str:
    c = normaliza(c)
    if "NIKE" in c: return "NIKESTORE"
    if "DIGITAL" in c: return "DIGITAL"
    if "INBOUND" in c: return "INBOUND"
    if "REVERSA" in c: return "REVERSA"
    if "WHOLE" in c: return "WHOLESALE"
    if "ROTA" in c and "SP" in c: return "ROTA SP"
    if "AUDITOR" in c or "INVENT" in c or c == "VER":
        return "OUTROS CANAIS"
    if c == "VER": return "OUTROS CANAIS"
    return c

# Verificação se o atleta é escalado para os cálculos
def verificar_escalacao(sigla: str) -> bool:
    return normaliza(sigla) not in NAO_ESCALADO_N

# Verifica a ocorrencia de falta
def verificar_falta(sigla: str) -> bool:
    return normaliza(sigla) == "F"

# Verifica a ocorrencia de atestado
def verificar_atestado(sigla: str) -> bool:
    return normaliza(sigla) == "AT"

# [1/2] Encontra a coluna necessário no cabeçalho
def encontrar_coluna(cabecalho, nome):
    try:
        return cabecalho.index(nome)
    except ValueError:
        H = [normaliza(h) for h in cabecalho]
        k = normaliza(nome)
        return H.index(k) if k in H else None

# [2/2] Encontra a coluna dia, pois deve-se dividir o texto antes da procura
def achar_coluna_dia(cabecalho, dia, mes, ano):
    padroes_inicio = (f"{dia}/{mes} -", f"{dia}/{mes}/{ano}", f"{dia}/{mes}")
    for j, h in enumerate(cabecalho):
        h = (h or "").strip()
        if not h:
            continue
        if any(h.startswith(p) for p in padroes_inicio):
            return j
        if re.search(rf"\b{dia}/{mes}(\b|/\d{{4}}\b)", h):
            return j
    return None

def idx_por_nome(headers, alvo, *tambem_contendo):
    alvo_n = normaliza(alvo)
    extras = [normaliza(x) for x in tambem_contendo]
    for i, h in enumerate(headers):
        nh = normaliza(h)
        if alvo_n in nh and all(x in nh for x in extras):
            return i
    return None
def get_gc():
    creds = _obter_creds()
    return gspread.authorize(creds)

# Pega e atualiza o cabeçalho da planilha requisitada
def ver_cabecalho(planilha, title, cabecalho=None, cols=120):
    try:
        ws = planilha.worksheet(title)
    except Exception:
        ws = planilha.add_worksheet(title=title, rows=5000, cols=cols)
        if cabecalho:
            ws.update([cabecalho])
        return ws
    if cabecalho:
        first = ws.get_values("1:1")
        if not first and cabecalho:
            ws.update([cabecalho])
    return ws

def contar_desligados_no_dia_qhc(gc, data_str: str) -> int:
    #aba_ativos = aba_qhc_ativos_from_data(data_str)
    aba_ativos = "H.C. TT"
    ws = gc.open_by_url(PLANILHA_QHC_URL).worksheet(aba_ativos)
    linhas = ws.get_all_values()
    if not linhas or len(linhas) < 2:
        return 0
    head, dados = linhas[0], linhas[1:]

    i_sit   = idx_por_nome(head, "Descricao", "Situac") or idx_por_nome(head, "Situac")
    i_afast = idx_por_nome(head, "Data", "Atuali")
    if i_sit is None or i_afast is None:
        return 0

    cnt = 0
    for r in dados:
        sit   = (r[i_sit]   if i_sit   < len(r) else "").strip()
        afast = (r[i_afast] if i_afast < len(r) else "").strip()
        if sit and afast and afast == data_str and str(sit).strip().upper().startswith("DEMIT"):
            cnt += 1
    return cnt

def aba_qhc_ativos_from_data(data_str: str) -> str:
    dd, mm, yyyy = data_str.split("/")
    dt = datetime.strptime(data_str, "%d/%m/%Y")
    return f"H.C. TT"

# Identifica a coluna de acordo com o Google Sheets [Ex.: A, J, AA, AZ]
def nome_coluna(n: int) -> str:
    letra = ""
    while n:
        n, r = divmod(n-1, 26)
        letra = chr(65 + r) + letra
    return letra

def col_esperada(df: pd.DataFrame, alvo: str) -> str:
    alvo_n = normaliza(alvo)
    for c in df.columns:
        if normaliza(c) == alvo_n:
            return c
    return alvo

def _to_date_obj(s: str):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None

def buscar_ihc_qhc(gc, data_str: str) -> int | None:

    try:
        qhc = gc.open_by_url(PLANILHA_QHC_URL)
        #aba_ativos = aba_qhc_ativos_from_data(data_str)
        aba_ativos = "H.C. TT"
        ws = qhc.worksheet(aba_ativos) # Configurar aqui depois
        linhas = ws.get_all_values()
        if not linhas or len(linhas) < 2:
            return 0

        head, dados = linhas[0], linhas[1:]

        # localizar colunas relevantes (tolerante a variações de título)
        i_sit   = idx_por_nome(head, "Descrição", "Situação") or idx_por_nome(head, "Situação")
        i_cargo = idx_por_nome(head, "Título Reduzido", "Cargo") or idx_por_nome(head, "Cargo")
        i_area  = idx_por_nome(head, "Área")
        i_fil   = idx_por_nome(head, "Apelido", "Filial") or idx_por_nome(head, "Filial")

        for k, v in {"Descrição (Situação)": i_sit, "Título Reduzido (Cargo)": i_cargo,
                     "Área": i_area, "Apelido (Filial)": i_fil}.items():
            if v is None:
                raise ValueError(f"[QHC/IHC] Coluna '{k}' não encontrada em '{aba_ativos}'.")

        # conjuntos/normalizações
        def n(x): return normaliza(x or "")

        # variações comuns do cargo
        cargos_ok = {
            "ASSIS. DEPOSITO",
            "ASSIST. DEPOSITO",
            "ASSISTENTE DEPOSITO",
            "ASSISTENTE DE DEPÓSITO",
            "ASSISTENTE DE DEPÓSITO",  # com acento
        }
        cargos_ok_norm = {n(c) for c in cargos_ok}

        filiais_ok = {"CD 2103 | FISIA HUB", "CD 2103 | FISIA"}
        filiais_ok_norm = {n(f) for f in filiais_ok}

        cnt = 0
        for row in dados:
            sit   = row[i_sit]   if i_sit   is not None and i_sit   < len(row) else ""
            cargo = row[i_cargo] if i_cargo is not None and i_cargo < len(row) else ""
            area  = row[i_area]  if i_area  is not None and i_area  < len(row) else ""
            fil   = row[i_fil]   if i_fil   is not None and i_fil   < len(row) else ""

            if n(sit) != "TRABALHANDO":
                continue
            if n(area) != "WAREHOUSE":
                continue
            if n(cargo) not in cargos_ok_norm:
                continue
            if n(fil) not in filiais_ok_norm:
                continue

            cnt += 1

        return int(cnt)
    except Exception as e:
        print(f"[QHC/IHC] Falha ao obter IHC no QHC: {e}")
        return None
# Rótulo canônico para fallback de cidades
OUTRAS_LABEL = "OUTRAS CIDADES"
_CIDADES_CANON = {normaliza(c): c for c in PLAN_TO_cab_cidades if c != OUTRAS_LABEL}
def buscar_qhc_contagens(gc, data_str: str) -> dict | None:
    try:
        qhc = gc.open_by_url(PLANILHA_QHC_URL)
        #aba_ativos = aba_qhc_ativos_from_data(data_str)
        aba_ativos = "H.C. TT"
        ws = qhc.worksheet(aba_ativos)
        linhas = ws.get_all_values()
        if not linhas or len(linhas) < 2:
            return {'IHC': 0, 'MODP': 0}

        head, dados = linhas[0], linhas[1:]

        # localizar colunas
        i_sit   = idx_por_nome(head, "Descrição", "Situação") or idx_por_nome(head, "Situação")
        i_cargo = idx_por_nome(head, "Título Reduzido", "Cargo") or idx_por_nome(head, "Cargo")
        i_area  = idx_por_nome(head, "Área")
        i_fil   = idx_por_nome(head, "Apelido", "Filial") or idx_por_nome(head, "Filial")
        for k, v in {"Descrição (Situação)": i_sit, "Título Reduzido (Cargo)": i_cargo,
                     "Área": i_area, "Apelido (Filial)": i_fil}.items():
            if v is None:
                raise ValueError(f"[QHC] Coluna '{k}' não encontrada em '{aba_ativos}'.")

        def n(x): 
            try:
                return normaliza(x or "")
            except NameError:
                return (str(x or "")).strip().upper()

        # IHC = assistentes de depósito (variações comuns)
        cargos_ihc = {
            "ASSIS. DEPOSITO", "ASSIST. DEPOSITO",
            "ASSISTENTE DEPOSITO", "ASSISTENTE DE DEPÓSITO"
        }
        cargos_ihc_n = {n(c) for c in cargos_ihc}

        # MOD+ conjunto: se existir um set global MOD_M, use-o; senão, use fallback
        try:
            cargos_modp_n = {n(c) for c in MOD_M}
        except NameError:
            cargos_modp = {
                "CONFERENTE", "OPERADOR DE EMPILHADEIRA",
                "TECNICO DE PERSONALIZACAO",
                "ASSIST. OPERACAO", "ASSIST. DEVOLUCAO",
                "ASSISTENTE DE OPERAÇÃO", "ASSISTENTE DEVOLUÇÃO"
            }
            cargos_modp_n = {n(c) for c in cargos_modp}

        filiais_ok_n = {n("CD 2103 | FISIA HUB"), n("CD 2103 | FISIA")}

        ihc_cnt = 0
        modp_cnt = 0

        for row in dados:
            sit   = row[i_sit]   if i_sit   is not None and i_sit   < len(row) else ""
            cargo = row[i_cargo] if i_cargo is not None and i_cargo < len(row) else ""
            area  = row[i_area]  if i_area  is not None and i_area  < len(row) else ""
            fil   = row[i_fil]   if i_fil   is not None and i_fil   < len(row) else ""

            if n(sit) != "TRABALHANDO":   continue
            if n(area) != "WAREHOUSE":    continue
            if n(fil) not in filiais_ok_n: continue

            cargo_n = n(cargo)
            if cargo_n in cargos_ihc_n:
                ihc_cnt += 1
            if cargo_n in cargos_modp_n:
                modp_cnt += 1
        
        return {'IHC': int(ihc_cnt), 'MODP': int(modp_cnt)}

    except Exception as e:
        print(f"[QHC] Falha ao obter contagens no QHC: {e}")
        return None
    
def deve_marcar_des_sigla(data_falta: date, data_demissao_str: str) -> bool:
    data_demissao = _to_date_obj(data_demissao_str)
    if not data_demissao:
        return False
    return data_falta >= data_demissao

def deve_considerar_falta(dt, row):
    if row["status"] != "DESLIGADO":
        return True
    
    data_demissao = _to_date_obj(row["demissao"])
    
    if data_demissao is None:
        return False
    
    return dt < data_demissao

def norm_mes_para_data(s: str) -> str:
    ano_ref = datetime.strptime("10/10/2025", "%d/%m/%Y").year

    mapa_datas_lc = {
        "janeiro":   f"01/01/{ano_ref}",
        "fevereiro": f"01/02/{ano_ref}",
        "março":     f"01/03/{ano_ref}",
        "marco":     f"01/03/{ano_ref}", 
        "abril":     f"01/04/{ano_ref}",
        "maio":      f"01/05/{ano_ref}",
        "junho":     f"01/06/{ano_ref}",
        "julho":     f"01/07/{ano_ref}",
        "agosto":    f"01/08/{ano_ref}",
        "setembro":  f"01/09/{ano_ref}",
        "outubro":   f"01/10/{ano_ref}",
        "novembro":  f"01/11/{ano_ref}",
        "dezembro":  f"01/12/{ano_ref}",
    }
    
    if s is None:
        return ""
    s = str(s).strip()
    # já é uma data interpretável?
    dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%d/%m/%Y")
    # tenta por nome do mês (case-insensitive)
    return mapa_datas_lc.get(s.lower(), s)

# ========== Funções para coleta do IHC da base mãe ==========

def buscar_ihc_base_mae(gc, data_str: str, aba_quadro: str = "Quadro FY - V2") -> int | None:
    try:
        ws = gc.open_by_url(PLANILHA_MAE_URL).worksheet(aba_quadro)
        linhas = ws.get_all_values()
        if not linhas or len(linhas) < 2:
            return None

        cab, dados = linhas[0], linhas[1:]
        df = pd.DataFrame(dados, columns=cab)

        # Colunas
        c_dia = col_esperada(df, "Dia")

        c_act_op = next(
            (c for c in df.columns if normaliza(c) == normaliza("ACT OP")),
            None
        )

        c_act_modp = next(
            (c for c in df.columns if normaliza(c) == normaliza("ACT MOD+")),
            None
        )

        if not all([c_dia, c_act_op, c_act_modp]):
            raise ValueError("Colunas obrigatórias não encontradas")

        # Filtro por dia
        dd, mm, _ = data_str.split("/")
        padrao = rf"^\s*{dd}/{mm}\b"
        mask = df[c_dia].astype(str).str.match(padrao)

        if not mask.any():
            return None

        row = df.loc[mask].iloc[-1]

        def to_int(v):
            if v is None or str(v).strip() == "":
                return 0
            return int(float(str(v).replace(".", "").replace(",", ".")))

        act_op = to_int(row[c_act_op])
        act_modp = to_int(row[c_act_modp])
        return act_op + act_modp

    except Exception as e:
        print("Erro buscar_ihc_base_mae:", e)
        return None

MES_MAP = {"JAN":1,"FEV":2,"MAR":3,"ABR":4,"MAI":5,"JUN":6,"JUL":7,"AGO":8,"SET":9,"OUT":10,"NOV":11,"DEZ":12}

def parse_header_date(header: str, year_default: int) -> date | None:
    """
    Converte cabeçalho de coluna de 'dias' para um objeto date.
    Aceita: '26/09/2025', '26/09', '26/set.' (variações de caixa).
    Também tolera textos como '26/09 - sex.' (usa prefixo).
    """
    if not header:
        return None
    s = normaliza(str(header)).strip()
    s = s.replace('.', '')
    # usará apenas o prefixo até 10/11 chars para evitar sufixos tipo ' - sex.'
    s_pref = s[:10]

    # 1) dd/mm/aaaa
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', s_pref)
    if m:
        d, m_, y = map(int, m.groups())
        return date(y, m_, d)

    # 2) dd/mm (usa ano default)
    m = re.match(r'^(\d{1,2})/(\d{1,2})', s_pref)
    if m:
        d, m_ = map(int, m.groups())
        return date(year_default, m_, d)

    # 3) dd/mmm (pt-BR abreviado)
    m = re.match(r'^(\d{1,2})/([A-Za-z]{3,})', s_pref)
    if m:
        d = int(m.group(1))
        abbr = m.group(2)[:3].upper()
        m_ = MES_MAP.get(abbr)
        if m_:
            return date(year_default, m_, d)

    return None

# ==================== PIPELINE DES->QHC ====================

def normaliza_dispensa(c: str) -> str:
    s = ("" if c is None else str(c)).strip().upper()
    s_noacc = normaliza(s)

    # palavras/labels livres
    if "ESPONT" in s_noacc:
        # Se vier "NAO ESPONTANEO" / "NÃO ESPONTÂNEO" tratamos como FORÇADO
        if "NAO" in s_noacc or "NÃO" in s:
            return "FORÇADO"
        return "ESPONTÂNEO"

    # compat anterior com sets
    for m in forc:
        if s_noacc in m:
            return "FORÇADO"
        
    for n in espo:
        if s_noacc in n:
            return "ESPONTÂNEO"
        
    return "INDEFINIDO"

def _parse_data_flex(s):
    s = str(s or "").strip()
    if not s:
        return pd.NaT
    # Excel serial (número grande)
    if re.fullmatch(r"\d{5,}", s):
        try:
            base = pd.Timestamp("1899-12-30")  # base do Excel
            return base + pd.to_timedelta(int(s), unit="D")
        except Exception:
            pass
    # Formatos comuns
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            continue
    # Última tentativa: heurística com dayfirst
    return pd.to_datetime(s, dayfirst=True, errors="coerce")

# ========================== ATUALIZAR ACT DO QUADRO FY ========================================
def atualizar_act_quadro_fy(gc, data_str: str):
    hoje_str = datetime.now().strftime("%d/%m/%Y")

    def col_a1_from_idx_zero_based(i0: int) -> str:
        return nome_coluna(i0 + 1)

    if data_str == hoje_str:
        
        dd, mm, yyyy = data_str.split("/")
        dt = datetime.strptime(data_str, "%d/%m/%Y").date()

        # === 1) Calcula IHC (para HOJE)
        cont = buscar_qhc_contagens(gc, data_str)  # {'IHC': ..., 'MODP': ...}
        act_calc = None
        if cont and 'IHC' in cont:
            try:
                act_calc = int(cont['IHC'])
            except Exception:
                act_calc = None
        if act_calc is None:
            qhc = gc.open_by_url(PLANILHA_QHC_URL)
            aba_resumo = f"Resumo Ativos {nome_mes_pt(dt)} {yyyy}"
            ws_resumo = qhc.worksheet(aba_resumo) 
            v = ws_resumo.acell("AG76").value
            act_calc = int(str(v).replace(".", "").replace(",", "")) if (v not in (None, "")) else 0

        # === 2) Localiza e garante colunas no Quadro FY ===
        mae = gc.open_by_url(PLANILHA_MAE_URL)
        ws_fy = mae.worksheet("Quadro FY - V2")
        header = ws_fy.row_values(1)

        i_act = idx_por_nome(header, "ACT - Interface OP") or idx_por_nome(header, "ACT OP")
        if i_act is None:
            raise ValueError("[Quadro FY] Coluna 'ACT (Interface)' não encontrada.")

        # Garante ACT_ORIG (idempotência, como no seu código)
        i_act_orig = idx_por_nome(header, "ACT_ORIG")
        if i_act_orig is None:
            ws_fy.update_cell(1, len(header) + 1, "ACT_ORIG")
            header = ws_fy.row_values(1)
            i_act_orig = idx_por_nome(header, "ACT_ORIG")

        # Garante colunas ACT MOI e MOD+ ACT (se não existirem, cria no final)
        i_act_moi = idx_por_nome(header, "ACT MOI")
        if i_act_moi is None:
            ws_fy.update_cell(1, len(header) + 1, "ACT MOI")
            header = ws_fy.row_values(1)
            i_act_moi = idx_por_nome(header, "ACT MOI")

        i_modp_act = idx_por_nome(header, "ACT MOD+")
        if i_modp_act is None:
            ws_fy.update_cell(1, len(header) + 1, "ACT MOD+")
            header = ws_fy.row_values(1)
            i_modp_act = idx_por_nome(header, "ACT MOD+")

        col_act_a1    = col_a1_from_idx_zero_based(i_act)
        col_actorig_a1= col_a1_from_idx_zero_based(i_act_orig)
        col_moi_a1    = col_a1_from_idx_zero_based(i_act_moi)
        col_modp_a1   = col_a1_from_idx_zero_based(i_modp_act)

        # === 3) Acha a linha do dia (coluna 'Dia' inicia com dd/mm) ===
        colA = ws_fy.col_values(1)
        alvo = f"{dd}/{mm}"
        row_idx = None
        for i, v in enumerate(colA[1:], start=2):
            if str(v).strip().startswith(alvo):
                row_idx = i
                break
        if row_idx is None:
            raise ValueError(f"[Quadro FY] Linha do dia iniciando com '{alvo}' não encontrada.")

        # === 4) Atualiza ACT de hoje ===
        cel_act = f"{col_act_a1}{row_idx}"
        ws_fy.update_acell(cel_act, act_calc)
        try:
            ws_fy.format(cel_act, {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
        except Exception:
            pass
        print(f"[Quadro FY] (HOJE) Atualizado {cel_act} com {act_calc}.")

        # === 5) Calcula ACT MOI e MOD+ ACT no QHC com os filtros solicitados ===
        qhc = gc.open_by_url(PLANILHA_QHC_URL)
        #aba_ativos = aba_qhc_ativos_from_data(data_str)
        ws_qhc = qhc.worksheet("H.C. TT") 
        linhas_qhc = ws_qhc.get_all_values()
        if not linhas_qhc or len(linhas_qhc) < 2:
            print("[QHC] Aba de Ativos vazia para contagem de ACT MOI / MOD+ ACT.")
            return

        head_qhc, dados_qhc = linhas_qhc[0], linhas_qhc[1:]

        i_fil   = idx_por_nome(head_qhc, "Apelido", "Filial") or idx_por_nome(head_qhc, "Filial")
        i_area  = idx_por_nome(head_qhc, "Área")
        i_sit   = idx_por_nome(head_qhc, "Descrição", "Situação") or idx_por_nome(head_qhc, "Situação")
        i_mo    = idx_por_nome(head_qhc, "Mão de Obra") or idx_por_nome(head_qhc, "Mao de Obra")
        i_cargo = idx_por_nome(head_qhc, "Título Reduzido", "Cargo") or idx_por_nome(head_qhc, "Título Reduzido (Cargo)")

        if None in (i_fil, i_area, i_sit, i_mo):
            raise RuntimeError("[QHC] Cabeçalho não possui colunas de 'Apelido/Filial', 'Área', 'Descrição (Situação)' ou 'Mão de Obra'.")

        def getv(row, idx):
            return (row[idx] if idx is not None and idx < len(row) and row[idx] is not None else "").strip()

        # Filiais que contam para MOI (MOD)
        filiais_moi = { 
            normaliza("CD 2103 | FISIA"), 
            normaliza("CD 2103 | FISIA HUB")
        }
        
        # Filiais que contam para MOD+ (MOI/MOI GESTÃO) - apenas Extrema
        filiais_modp = { 
            normaliza("CD 2103 | FISIA"), 
            normaliza("CD 2103 | FISIA HUB")
        }
        
        area_ok    = normaliza("WAREHOUSE")
        sit_ok     = normaliza("TRABALHANDO")
        cargo_excluir = normaliza("JOVEM APRENDIZ - ADMINISTRATIVO")

        moi_act_count = 0    # Mão de Obra = MOD (Extrema + Jarinu)
        modp_act_count = 0   # Mão de Obra = {MOI, MOI GESTÃO} (apenas Extrema)

        for r in dados_qhc:
            filial = normaliza(getv(r, i_fil))
            area   = normaliza(getv(r, i_area))
            sit    = normaliza(getv(r, i_sit))
            mo     = normaliza(getv(r, i_mo))
            cargo  = normaliza(getv(r, i_cargo))

            # Filtro: exclui JOVEM APRENDIZ - ADMINISTRATIVO
            if cargo == cargo_excluir:
                continue

            # Contabiliza MOI (MOD) - Extrema + Jarinu
            if (filial in filiais_moi) and (area == area_ok) and (sit == sit_ok):
                if mo == normaliza("MOD"):
                    moi_act_count += 1
            
            # Contabiliza MOD+ (MOI/MOI GESTÃO) - apenas Extrema
            if (filial in filiais_modp) and (area == area_ok) and (sit == sit_ok):
                if mo in { normaliza("MOI"), normaliza("MOI GESTÃO"), normaliza("MOI GESTAO") }:
                    modp_act_count += 1

        # === 6) Grava ACT MOI e MOD+ ACT na linha do dia ===
        cel_modp = f"{col_moi_a1}{row_idx}"
        cel_moi = f"{col_modp_a1}{row_idx}"

        ws_fy.update_acell(cel_moi, moi_act_count)
        ws_fy.update_acell(cel_modp, modp_act_count)

        try:
            ws_fy.format(cel_moi,  {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
            ws_fy.format(cel_modp, {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
        except Exception:
            pass

        print(f"[Quadro FY] (HOJE) Atualizado {cel_moi} (ACT MOI) = {moi_act_count} e {cel_modp} (MOD+ ACT) = {modp_act_count}.")
        return

    else:
        print(f"[Quadro FY] Data antiga selecionada, Não atualizada.")

# =========================== DESLIGADOS PARA QUADRO HEAD COUNT ================================
def des_para_qhc(data: str):
    creds = _obter_creds()
    gc = gspread.authorize(creds)
    qhc_sh = gc.open_by_url(PLANILHA_QHC_URL)
    des_sh = gc.open_by_url(PLANILHA_DES_URL)

    try:
        tz = ZoneInfo("America/Sao_Paulo")
        d,m,a = data.split("/")
        hoje_dt = datetime(int(a), int(m), int(d), tzinfo=tz)
    except Exception:
        hoje_dt = datetime.now()
    # Loop: hoje (0) e 4 dias anteriores (-1, -2, -3, -4)
    for offset in range(0, -7 , -1):
        data_alvo_dt = hoje_dt + timedelta(days=offset)
        data_alvo_str = data_alvo_dt.strftime("%d/%m/%Y")
        print(f"[+] Iniciando processamento do Desligamento para {data_alvo_str}...")

        # aba de ATIVOS dinâmica por mês/ano (função espera string)
        #aba_ativos = aba_qhc_ativos_from_data(data_alvo_str)
        aba_ativos = "H.C. TT"
        qhc_ws = ver_cabecalho(qhc_sh,aba_ativos) # Configurar aqui depois
        ws = qhc_sh.worksheet(aba_ativos)
        linhas_ = ws.get_all_values()
        dfs = []

        head, dados = linhas_[0], linhas_[1:]
        i_sit = idx_por_nome(head, "Descrição", "Situação") or idx_por_nome(head, "Situação")

        # Coleta dados das abas de desligamento
        for aba in ABA_DES_LIST:
            try:
                ws = des_sh.worksheet(aba)
            except Exception:
                print(f"[DES] Aba '{aba}' não encontrada, pulando.")
                continue
            linhas = ws.get_all_values()
            if not linhas or len(linhas) < 2:
                print(f"[DES] Aba '{aba}' está vazia, pulando.")
                continue
            cab, dados = linhas[0], linhas[1:]
            df = pd.DataFrame(dados, columns=cab)

            # garante as 3 colunas usadas
            for col in ["Data do Desligamento", "Colaborador que será desligado", "Solicitação/ Pedido"]:
                if col not in df.columns:
                    df[col] = ""
                df[col] = df[col].astype(str)

            # evita problemas com colunas duplicadas / lixo do cabeçalho
            df = df.loc[:, ["Data do Desligamento", "Colaborador que será desligado", "Solicitação/ Pedido"]]

            # origem e ordenação
            df["_ORIGEM_ABA"] = aba
            df = df[["Data do Desligamento", "Colaborador que será desligado", "Solicitação/ Pedido", "_ORIGEM_ABA"]]
            dfs.append(df)

        # Abas “Dados de Desligados / Bloqueio de Acesso”
        try:
            dados_desl_sh = gc.open_by_url(PLANILHA_DADOS_DESL_URL)
            ws_bloq = dados_desl_sh.worksheet(ABA_DADOS_BLOQUEIO)
            linhas_bloq = ws_bloq.get_all_values()
            if linhas_bloq and len(linhas_bloq) >= 2:
                cab_b, dados_b = linhas_bloq[0], linhas_bloq[1:]
                df_b = pd.DataFrame(dados_b, columns=cab_b)

                def _estab_is_alvo(x):
                    s = str(x or "").strip()
                    try:
                        if float(s).is_integer():
                            s = str(int(float(s)))
                    except:
                        pass
                    return s in {"1081", "2103"}

                if "ESTABELECIMENTO" in df_b.columns:
                    df_b = df_b[df_b["ESTABELECIMENTO"].map(_estab_is_alvo)]
                else:
                    df_b = df_b.iloc[0:0]

                if not df_b.empty:
                    df_b["_DATA_NORM"] = pd.to_datetime(
                        df_b["DATA EMISSAO"].astype(str).str.strip(),
                        errors="coerce", dayfirst=False
                    ).dt.strftime("%d/%m/%Y")

                    df_b["Data do Desligamento"] = df_b["_DATA_NORM"].fillna("")
                    df_b["Colaborador que será desligado"] = df_b["NOME"].astype(str).fillna("")
                    df_b["Solicitação/ Pedido"] = df_b["MOTIVO DA RECISAO"].astype(str).fillna("")
                    df_b = df_b[["Data do Desligamento", "Colaborador que será desligado", "Solicitação/ Pedido"]]
                    df_b["_ORIGEM_ABA"] = "Dados de Desligados / Bloqueio de Acesso"
                    df_b["Descrição (Situação)"] = "Descrição (Situação)"
                    dfs.append(df_b)
            else:
                print("[DES/Dados] 'Bloqueio de Acesso' está vazia.")
        except Exception as e:
            print(f"[DES/Dados] Falha ao ler 'Dados de Desligados': {e}")

        if not dfs:
            print("[DES] Nenhuma aba de desligamentos com dados.")
            continue

        df_all = pd.concat(dfs, ignore_index=True)

        # Normaliza data das fontes para dd/mm/aaaa
        df_all["_DATA_NORM"] = df_all["Data do Desligamento"].apply(_parse_data_flex).dt.strftime("%d/%m/%Y")


        # Filtra pela data alvo desta iteração
        df_day = df_all[df_all["_DATA_NORM"] == data_alvo_str].copy()
        if df_day.empty:
            print(f"[DES] A data {data_alvo_str} não consta nas fontes {ABA_DES_LIST} + Bloqueio de Acesso.")
            qhc_para_base_mae_desligados(gc,data_alvo_str)
            time.sleep(10)
            continue

        # Cabeçalho do QHC
        linhas_qhc = qhc_ws.get_all_values()
        if not linhas_qhc:
            print("[QHC] Aba vazia.")
            continue
        cab_qhc = linhas_qhc[0]

        # Utilitários de cabeçalho
        def achar_coluna_por_predicados(*predicados):
            for i, h in enumerate(cab_qhc):
                nh = normaliza(h)
                if all(pred(nh) for pred in predicados):
                    return i + 1  # 1-based
            return None

        def contem(s): 
            s_norm = normaliza(s)
            return lambda x: s_norm in x

        def igual(s):  
            s_norm = normaliza(s)
            return lambda x: x == s_norm

        # Nome

        idx_qhc_nome_0based = next((i for i, h in enumerate(cab_qhc) if normaliza(h) == "NOME"), None)
        if idx_qhc_nome_0based is None:
            raise ValueError("[QHC] Coluna 'Nome' não encontrada.")

        # Colunas alvo
        col_situacao = (achar_coluna_por_predicados(contem("DESCRICAO"), contem("SITUAC")) or
                        achar_coluna_por_predicados(contem("SITUAC")))
        if col_situacao is None:
            raise ValueError("[QHC] 'Descrição (Situação)' não encontrada.")

        col_tipo_dispensa = (achar_coluna_por_predicados(igual("INICIATIVA")) or
                             achar_coluna_por_predicados(contem("INICIAT")))
        if col_tipo_dispensa is None:
            raise ValueError("[QHC] Coluna 'Iniciativa' não encontrada.")

        col_data_afast = (achar_coluna_por_predicados(contem("DATA"), contem("AFAST")) or
                          achar_coluna_por_predicados(contem("AFAST")))
        if col_data_afast is None:
            raise ValueError("[QHC] 'Data Afastamento' não encontrada.")

        col_data_atual = (achar_coluna_por_predicados(contem("ATUALIZAC")) or
                          achar_coluna_por_predicados(contem("ATUAL")))
        if col_data_atual is None:
            raise ValueError("[QHC] 'Data Atualização' não encontrada.")

        # Colunas de dias (datas no cabeçalho), o ano vem da data alvo
        dd_s, mm_s, yyyy_s = data_alvo_str.split("/")
        ano_desl = int(yyyy_s)

        cols_datas = []
        for j, h in enumerate(cab_qhc, start=1):
            dt = parse_header_date(h, ano_desl)
            if dt:
                cols_datas.append((j, dt))

        # Mapa: nome normalizado -> linhas do QHC
        map_nome_para_linha = {}
        for i, row in enumerate(linhas_qhc[1:], start=2):
            rotulo = (row[idx_qhc_nome_0based] if idx_qhc_nome_0based < len(row) else "").strip()
            nrot = normaliza(rotulo)
            if nrot:
                map_nome_para_linha.setdefault(nrot, []).append(i)

        # Data de atualização (hoje)
        try:
            tz = ZoneInfo("America/Sao_Paulo")
            data_hoje_str = datetime.now(tz).strftime("%d/%m/%Y")
        except Exception:
            data_hoje_str = datetime.now().strftime("%d/%m/%Y")

        # Agrupador de blocos consecutivos
        def blocos_consecutivos(cols):
            if not cols: return []
            cols = sorted(cols)
            blocos = []
            ini = prev = cols[0]
            for c in cols[1:]:
                if c == prev + 1:
                    prev = c
                else:
                    blocos.append((ini, prev))
                    ini = prev = c
            blocos.append((ini, prev))
            return blocos

        def linha_trabalhando_para(nome_norm):
            if nome_norm not in map_nome_para_linha:
                return None
            linhas_candidato = map_nome_para_linha[nome_norm]
            for lin in reversed(linhas_candidato):
                try:
                    situacao = (linhas_qhc[lin - 1][col_situacao - 1] or "").strip()
                except Exception:
                    situacao = ""
                if normaliza(situacao) == "TRABALHANDO":
                    return lin
            return linhas_candidato[-1]

        # Loop nos desligados do dia alvo
        atualizados = 0
        for _, row in df_day.iterrows():
            nome_colab = row["Colaborador que será desligado"]
            tipo_disp  = row["Solicitação/ Pedido"]
            data_d     = row["Data do Desligamento"]
            situacao = (linhas_qhc[_ - 1][col_situacao - 1] or "").strip()

            lin = linha_trabalhando_para(normaliza(nome_colab))
            if lin is None or pd.isna(lin):
                print(f"[QHC] Colaborador '{nome_colab}' não encontrado no QHC. Verificar grafia na planilha de solicitação ({row.get('_ORIGEM_ABA','')}).")
                time.sleep(2)
                continue

            try:
                val_situacao_atual = ""
                row_idx0 = int(lin) - 1
                col_idx0 = int(col_situacao) - 1
                if 0 <= row_idx0 < len(linhas_qhc) and 0 <= col_idx0 < len(linhas_qhc[row_idx0]):
                    val_situacao_atual = (linhas_qhc[row_idx0][col_idx0] or "").strip()
            except Exception:
                val_situacao_atual = ""

            status_norm = normaliza(val_situacao_atual)
            if status_norm in ("DEMITIDO", "EFETIVADO") or status_norm.startswith("EFETIV"):
                motivo_pulo = "Demitido" if status_norm == "DEMITIDO" else "Efetivado"
                print(f"[QHC] Colaborador: '{nome_colab}' [{lin}] já está {motivo_pulo}. [Pulado]")
                time.sleep(2)
                continue

            # Data alvo para zerar dias
            data_alvo_date = None
            try:
                dd, mm, yyyy = str(data_d).strip().split("/")
                data_alvo_date = date(int(yyyy), int(mm), int(dd))
            except Exception:
                pass

            # Se já estiver Transferido, não faz nada
            if status_norm == "TRANSFERIDO":
                print(f"[QHC] Colaborador: '{nome_colab}' [{lin}] está Transferido. [Pulado]")
                time.sleep(2)
                continue

            # Atualizações no QHC
            if normaliza(tipo_disp) != "EFETIVACAO":
                try:
                    qhc_ws.update_cell(int(lin), int(col_situacao), "Demitido")
                    qhc_ws.update_cell(int(lin), int(col_tipo_dispensa), normaliza_dispensa(tipo_disp))
                    qhc_ws.update_cell(int(lin), int(col_data_afast), data_d)
                    qhc_ws.update_cell(int(lin), int(col_data_atual), data_hoje_str)
                except Exception as e:
                    print(f"[QHC] Falha ao atualizar campos básicos '{nome_colab}' (linha {lin}): {e}")

                if data_alvo_date is not None:
                    cols_zerar = [j for (j, dt) in cols_datas
                                  if (dt.year == data_alvo_date.year and dt.month == data_alvo_date.month and dt >= data_alvo_date)]
                    if cols_zerar:
                        for c_ini, c_fim in blocos_consecutivos(cols_zerar):
                            ncols = c_fim - c_ini + 1
                            rng = f"{nome_coluna(c_ini)}{int(lin)}:{nome_coluna(c_fim)}{int(lin)}"
                            try:
                                qhc_ws.update(range_name=rng, values=[[0]*ncols], value_input_option="USER_ENTERED")
                            except Exception as e:
                                print(f"[QHC] Falha ao zerar {rng} (linha {lin}): {e}")

                atualizados += 1
                print(f"[QHC] Colaborador: '{nome_colab}' [{lin}] Marcado para Demitido. [Atualizado]")
                time.sleep(4)
            else:
                print(f"[QHC] Colaborador: '{nome_colab}' [{lin}] Marcado para Efetivação. [Pulado]")
            time.sleep(4)
        print(f"[✓] Processamento concluído para {data_alvo_str}. Linhas atualizadas: {atualizados}.")
        time.sleep(4)
        qhc_para_base_mae_desligados(gc,data_alvo_str)

# =========================== QUADRO HEAD COUNT PARA TO FY ====================================
def qhc_para_base_mae_desligados(gc, data_str: str):
    # 1) Abre QHC UMA vez (aba conforme a data base) e lê tudo
    aba_ativos = aba_qhc_ativos_from_data(data_str)
    qhc = gc.open_by_url(PLANILHA_QHC_URL).worksheet("H.C. TT") # Configurar aqui depois
    linhas = qhc.get_all_values()
    if not linhas or len(linhas) < 2:
        print("[QHC] Sem dados.")
        return 0

    head = linhas[0]
    dados = linhas[1:]

    # 2) Índices obrigatórios
    i_nome  = idx_por_nome(head, "Nome")
    i_mat   = idx_por_nome(head, "Matric")              # Matrícula/Matricula
    i_init  = idx_por_nome(head, "Iniciat")             # Iniciativa
    i_sit   = idx_por_nome(head, "Descricao", "Situac") or idx_por_nome(head, "Situac")
    i_afast = idx_por_nome(head, "Data", "Afast")
    i_class = idx_por_nome(head, "Tipo de Contrato")
    i_cargo = idx_por_nome(head, "Título Reduzido (Cargo)")
    i_area  = idx_por_nome(head, "Área")
    i_gen   = idx_por_nome(head, "Descrição (Sexo)")
    i_turno = idx_por_nome(head, "Descrição (Escala)")
    i_city  = idx_por_nome(head, "Cidade")
    i_sup   = idx_por_nome(head, "Supervisor")
    i_coord = idx_por_nome(head, "Coordenador")
    i_proce = idx_por_nome(head, "Processo")
    i_filia = idx_por_nome(head, "Apelido (Filial)")

    # Índice opcional: "Data Atualização" no QHC
    i_data_atual = (
        idx_por_nome(head, "Data", "Atualiz")
        or idx_por_nome(head, "Atualiz")
        or idx_por_nome(head, "Atualização")
    )

    for k, v in {"Nome": i_nome, "Matrícula": i_mat, "Iniciativa": i_init, "Situação": i_sit, "Data Afastamento": i_afast}.items():
        if v is None:
            raise ValueError(f"[QHC] Coluna '{k}' não encontrada.")

    # 3) "Agência" (opcional) + candidatos
    i_age = idx_por_nome(head, "Filial")
    cand_cols = [
        idx_por_nome(head, "Empresa"),
        idx_por_nome(head, "Projeto"),
        idx_por_nome(head, "Mao de Obra"),
        idx_por_nome(head, "Mão de Obra"),
        idx_por_nome(head, "C.Custo"),
        idx_por_nome(head, "Processo (C.C)"),
        idx_por_nome(head, "Processo"),
    ]
    cand_cols = [c for c in cand_cols if c is not None]

    def getv(row: list[str], idx: int | None) -> str:
        if idx is None:
            return ""
        return (row[idx] if idx < len(row) and row[idx] is not None else "").strip()

    def deduz_agencia_from_row(row: list[str]) -> str:
        if i_age is not None and i_age < len(row):
            s = (row[i_age] or "").strip()
            if s:
                if s in {"2103", "1081"}:
                    s = "FISIA"
                return s
        for c in cand_cols:
            if c < len(row):
                s = (row[c] or "").strip()
                if s:
                    txt = normaliza(s)
                    for key in AGENCIA_KEYS:
                        if normaliza(key) in txt:
                            return key
        txt_full = normaliza(" ".join([x or "" for x in row]))
        for key in AGENCIA_KEYS:
            if normaliza(key) in txt_full:
                return key
        return ""

    # 4) Gera a janela de datas: data_str + 4 dias pra trás (total 5)
    base_date = pd.to_datetime(data_str, dayfirst=True, errors="coerce")
    if pd.isna(base_date):
        raise ValueError(f"[QHC] data_str inválida: {data_str!r}")
    base_date = base_date.date()
    datas_check = [data_str]

    # 5) Mapeamentos para MOD/MOD+
    cargos_mod   = {"AJUDANTE DE MOTORISTA", "ASSIST. CARGA E DESCARGA", "ASSIST. DEPOSITO"}
    cargos_mod_p = {"ASSIST. DEVOLUCAO", "ASSIST. OPERACAO", "CONFERENTE", "OPERADOR DE EMPILHADEIRA", "TECN. PERSONALIZACAO"}
    cargos_moi = {"ANALISTA JR - INFRAESTRUTURA"}
    list_filial = {"CD 2103 | FISIA", "CD 2103 | FISIA HUB"} 

    # 6) Percorre as 5 datas SEM reabrir o QHC; acumula tudo em registros_total
    registros_total: list[list[str]] = []
    for alvo in datas_check:
        registros = []
        for r in dados:
            sit_norm = normaliza(getv(r, i_sit))
            if sit_norm != normaliza("DEMITIDO"):
                continue

            d_afast = getv(r, i_afast)
            if d_afast != alvo:
                continue

            # Data de atualização (preferencial) com fallback para Data de Afastamento
            d_atual_qhc = getv(r, i_data_atual) if i_data_atual is not None else ""
            dt_atual = pd.to_datetime(d_atual_qhc, dayfirst=True, errors="coerce")
            if pd.isna(dt_atual):
                dt_atual = pd.to_datetime(d_afast, dayfirst=True, errors="coerce")
            if pd.isna(dt_atual):
                print(f"[QHC->BASE] Linha sem Data Atualização/Afastamento válida para '{getv(r, i_nome)}'. [Pulado]")
                continue

            data_final_str = dt_atual.strftime("%d/%m/%Y")
            ano2_row = dt_atual.strftime("%y")
            mes_str_row = nome_mes_pt(dt_atual.date())

            classi      = getv(r, i_class)
            nome        = getv(r, i_nome)
            mat         = getv(r, i_mat)
            ini         = getv(r, i_init)
            cargo       = getv(r, i_cargo)
            area        = getv(r, i_area)
            genero      = getv(r, i_gen)
            turno       = getv(r, i_turno)
            cidade      = getv(r, i_city)
            supervisor  = getv(r, i_sup)
            coordenador = getv(r, i_coord)
            processo    = getv(r, i_proce)
            filial      = getv(r, i_filia)

            mods = ""
            if cargo in cargos_mod:
                mods = "MOD"
            elif cargo in cargos_mod_p:
                mods = "MOD+"

            age = deduz_agencia_from_row(r)
            
            if cargo in cargos_moi:
                print("MOI")
            
            else:
                if filial in list_filial and \
                    area == "WAREHOUSE":
                    registros.append([
                        ano2_row, mes_str_row, data_final_str,
                        mat, nome, age, classi, ini, cargo, area, genero, turno, cidade, supervisor, coordenador, processo, mods
                    ])

        if registros:
            print(f"[QHC] {alvo}: encontrados {len(registros)} desligado(s) no QHC.")
            registros_total.extend(registros)
        else:
            print(f"[QHC] Nenhum desligado em {alvo}.")

    if not registros_total:
        print("[BASE MÃE] Nenhum novo para inserir (janela de 5 dias vazia).")
        return 0

    # 7) Abre Base Mãe UMA vez, calcula duplicatas por (Data, Matrícula) e insere em lote
    mae = gc.open_by_url(PLANILHA_TO_FY).worksheet(ABA_TO_FY)
    existentes = mae.get_all_values()

    if existentes and len(existentes) > 1:
        cab = existentes[0]
        dados_mae = existentes[1:]
        try:
            i_data_mae = cab.index("Data")
        except ValueError:
            i_data_mae = 2
        try:
            i_matr_mae = cab.index("Matrícula")
        except ValueError:
            i_matr_mae = 3

        chaves_exist = {
            (row[i_data_mae].strip(), row[i_matr_mae].strip())
            for row in dados_mae
            if len(row) > max(i_data_mae, i_matr_mae)
        }
    else:
        chaves_exist = set()

    novos = [ln for ln in registros_total if (ln[2], ln[3]) not in chaves_exist]  # (Data, Matrícula)
    if not novos:
        print("[BASE MÃE] Nenhum novo para inserir (todos já existentes).")
        return 0

    mae.append_rows(novos, value_input_option="USER_ENTERED")
    print(f"[BASE MÃE] Inseridos {len(novos)} registro(s).")
    return len(novos)

# ==================== TO PARA PLANEJAMENTO ====================
def tofy_para_planejamento(
    gc,
    data: str,
    ihc_override: int | None = None,
    dry_run: bool = False,
):
    print(f"[TO] Verificando dados para a data {data}...")

    # ==================================================
    # 1) LEITURA → ABA TO
    # ==================================================
    to_sh = gc.open_by_url(PLANILHA_TO_URL)
    ws_to = to_sh.worksheet(ABA_TO_FY)

    linhas = ws_to.get_all_values()
    if not linhas or len(linhas) < 2:
        print("[TO] Aba TO (detalhada) está vazia!")
        return

    cab, dados = linhas[0], linhas[1:]
    df_all = pd.DataFrame(dados, columns=cab)

    for col in ["DATA","AGÊNCIA","CLASSIFICAÇÃO","INICIATIVA","CARGO",
                "ÁREA","GÊNERO","TURNO","MO","CIDADE","PROCESSO"]:
        if col not in df_all.columns:
            df_all[col] = ""
        df_all[col] = df_all[col].astype(str)

    df_all["_DATA_NORM"] = df_all["DATA"].apply(_parse_data_flex).dt.strftime("%d/%m/%Y")
    df_day = df_all[df_all["_DATA_NORM"] == data].copy()

    if df_day.empty:
        print(f"[TO] Nenhum desligado encontrado na ABA TO para {data}.")
        return

    print(f"[TO] Total de desligados no dia {data}: {len(df_day)}")

    # ==================================================
    # 2) CÁLCULOS
    # ==================================================
    mes = nome_mes_pt(datetime.strptime(data, "%d/%m/%Y"))
    TO_total = int(df_day.shape[0])
    IHC = int(ihc_override) if ihc_override is not None else TO_total
    TO_percent_val = TO_total / float(IHC) if IHC else ""

    ini_norm = normaliza_series(df_day["INICIATIVA"])

    efetivos    = int(df_day["CLASSIFICAÇÃO"].str.contains("EFETIV", case=False).sum())
    temporarios = int(df_day["CLASSIFICAÇÃO"].str.contains("TEMPOR", case=False).sum())
    espontaneo  = int(ini_norm.str.contains("ESPONTANEO").sum())
    forcado     = int(ini_norm.str.contains("FORCADO").sum())

    masc = int((df_day["GÊNERO"].apply(genero_normalizado) == "MASCULINO").sum())
    fem  = int((df_day["GÊNERO"].apply(genero_normalizado) == "FEMININO").sum())

    def grupo_from_row(r):
        mo = normaliza((r.get("MO") or "").strip())
        if mo in {"MOI","MOD","MOD+"}:
            return mo
        return classificar_grupo(r.get("CARGO") or "")

    grupos = df_day.apply(grupo_from_row, axis=1)
    moi  = int((grupos == "MOI").sum())
    mod  = int((grupos == "MOD").sum())
    modp = int((grupos == "MOD+").sum())

    turnos = df_day["TURNO"].apply(turno_normalizado)
    t1 = int((turnos == "1º").sum())
    t2 = int((turnos == "2º").sum())
    t3 = int((turnos == "3º").sum())
    t4 = int((turnos == "4º").sum())
    t5 = int((turnos == "5º").sum())
    t6 = int((turnos == "ADM").sum())
    t0 = int((turnos == "SEM DADOS").sum())

    efe_esp  = ((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("EFETIV")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("ESPONTANEO"))).sum()
    efe_for  = ((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("EFETIV")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("FORCADO"))).sum()
    temp_esp = ((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("TEMPOR")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("ESPONTANEO"))).sum()
    temp_for = ((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("TEMPOR")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("FORCADO"))).sum()
    
    canal_cont = {k: 0 for k in PLAN_TO_cab_canais}
    proc_col = "PROCESSO"
    if proc_col not in df_day.columns or df_day[proc_col].eq("").all():
        if "PROCESSOS" in df_day.columns:
            proc_col = "PROCESSOS"
    for k, v in df_day[proc_col].value_counts().items():
        canal = canal_normalizado(k)
        if canal not in canal_cont:
            canal = "OUTROS CANAIS"
        canal_cont[canal] += int(v)

    cidades_day = df_day["CIDADE"].apply(cidade_canonica)
    cid_cont = {k: 0 for k in PLAN_TO_cab_cidades}
    ag_cont = {k: 0 for k in AGENCIA_KEYS}
    TO_total = int(df_day.shape[0])
    TO_percent_val = ""
    
    for k, v in cidades_day.value_counts().items():
        if k not in cid_cont:
            k = OUTRAS_LABEL
        cid_cont[k] += int(v)
    
    for k, v in df_day["AGÊNCIA"].value_counts().items():
        tag = agencia_normalizada(k)
        if tag in ag_cont:
            ag_cont[tag] += int(v)
    try:
        if float(IHC) > 0:
            TO_percent_val = TO_total / float(IHC)
    except Exception:
        TO_percent_val = ""
    
    perc_efesp = round(efe_esp / float(IHC) * 100, 2)
    perc_efesp_str= str(perc_efesp).replace(".", ",") if "." in str(perc_efesp) else str(perc_efesp)
    perc_efesp = f"{perc_efesp_str}%"
    perc_efefo = round(efe_for / float(IHC) * 100, 2)
    perc_efefo_str= str(perc_efefo).replace(".", ",") if "." in str(perc_efefo) else str(perc_efefo)
    perc_efefo = f"{perc_efefo_str}%"
    perc_tempesp = round(temp_esp / float(IHC) * 100, 2)
    perc_tempesp_str= str(perc_tempesp).replace(".", ",") if "." in str(perc_tempesp) else str(perc_tempesp)
    perc_tempesp = f"{perc_tempesp_str}%"
    perc_tempfo = round(temp_for / float(IHC) * 100, 2)
    perc_tempfo_str= str(perc_tempfo).replace(".", ",") if "." in str(perc_tempfo) else str(perc_tempfo)
    perc_tempfo = f"{perc_tempfo_str}%"
    
    # ==================================================
    # 3) DESTINOS → RESUMO TO (BASE MÃE E TO FY)
    # ==================================================
    cab_plan = (
        PLAN_TO_cabecalho_ini +
        PLAN_TO_cab_canais +
        PLAN_TO_cab_cidades +
        PLAN_TO_cabecalho_fim +
        PLAN_TO_cab_adic +
        ["QUADRO EFETIVO", "QUADRO TEMPORÁRIO"]
    )

    # Base Mãe
    plan_mae = gc.open_by_url(PLANILHA_MAE_URL)
    area_plan_mae = ver_cabecalho(plan_mae, ABA_PLA_TO, cab_plan)

    # TO FY
    plan_fy = gc.open_by_url(PLANILHA_TO_FY)
    area_plan_fy = ver_cabecalho(plan_fy, ABA_PLA_TO, cab_plan)

    data_iso = datetime.strptime(data, "%d/%m/%Y").strftime("%d/%m/%Y")

    linha = [
        mes, data_iso, IHC, TO_total, TO_percent_val,
        efetivos, temporarios, espontaneo, forcado,
        masc, fem, moi, mod, modp, t1, t2, t3, t4, 
        t5, t6, t0
    ]

    for c in PLAN_TO_cab_canais:
        linha.append(int(canal_cont.get(c, 0)))
    for c in PLAN_TO_cab_cidades:
        linha.append(int(cid_cont.get(c, 0)))
    for a in AGENCIA_KEYS:
        linha.append(int(ag_cont.get(a, 0)))

    linha.extend([
        int(((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("EFETIV")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("ESPONTANEO"))).sum()),
        int(((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("EFETIV")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("FORCADO"))).sum()),
        int(((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("TEMPOR")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("ESPONTANEO"))).sum()),
        int(((df_day["CLASSIFICAÇÃO"].str.upper().str.contains("TEMPOR")) & (normaliza_series(df_day["INICIATIVA"]).str.contains("FORCADO"))).sum()),
        f"{perc_efesp}", f"{perc_efefo}", f"{perc_tempesp}", f"{perc_tempfo}"
    ])

    if dry_run:
        return linha

    def upsert(ws):
        colB = ws.get_values("B2:B")
        row = next((i for i, c in enumerate(colB, start=2) if c and c[0].strip() == data), None)
        header = ws.row_values(1)
        ncols = len(header)
        ln = (linha + [""] * ncols)[:ncols]

        if row:
            rng = f"A{row}:{nome_coluna(ncols)}{row}"
            ws.update(values=[ln], range_name=rng, value_input_option="USER_ENTERED")
        else:
            ws.append_row(ln, value_input_option="USER_ENTERED")

    upsert(area_plan_mae)
    upsert(area_plan_fy)

    print("[TO] Resumo TO atualizado na Base Mãe e no TO FY.")

    return TO_percent_val

# =========================== PROCESSAR DIA ====================================
def processar_dia(gc, data_str: str):
    tz = ZoneInfo("America/Sao_Paulo")
    _ = datetime.strptime(data_str, "%d/%m/%Y").replace(tzinfo=tz)
    print(f"[+] Iniciando processamento TO para {data_str}...")

    # Aba TO (DETALHADA) – apenas para garantir cabeçalho
    to_sh = gc.open_by_url(PLANILHA_TO_URL)
    to_ws = ver_cabecalho(to_sh, ABA_TO_FY)

    # 1) IHC diário
    ihc_base = buscar_ihc_base_mae(gc, data_str)

    # 2) MOD+ (não diário)
    modp_tot = None
    cont = buscar_qhc_contagens(gc, data_str)
    if cont:
        modp_tot = cont.get("MODP")

    # 3) Gera Resumo TO (Base Mãe + TO FY)
    percent = tofy_para_planejamento(
        gc,
        data_str,
        ihc_override=ihc_base,
        dry_run=False,
    )

    carry_accum.clear()
    print(f"[TO] Atualização TO para data {data_str} concluída.")
    return percent

# =========================== PRESENÇA PARA ABS =======================================
def normalizav(s: str) -> str:
    s = (s or "").strip().upper()
    resultado = []

    for ch in unicodedata.normalize("NFD", s):
        # mantém a cedilha
        if unicodedata.name(ch, "").startswith("COMBINING CEDILLA"):
            resultado.append(ch)
        # remove outros acentos
        elif unicodedata.category(ch) != "Mn":
            resultado.append(ch)

    return unicodedata.normalize("NFC", ''.join(resultado))

def etapa_lista_para_abs(gc, data_str, dd, mm, yyyy):
    lista_pre = gc.open_by_url(PLANILHA_PRE_URL)
    alvo = data_str.strip()
    dd, mm, yyyy = alvo.split("/")
    dt = date(int(yyyy), int(mm), int(dd))  # data da falta / dia sendo processado

    mes_tab = normalizav(nome_mes_pt(datetime.strptime(f"{dd}/{mm}/{yyyy}", "%d/%m/%Y")))
    lista_ws = lista_pre.worksheet(mes_tab)

    rows = lista_ws.get_all_values()
    if not rows:
        raise RuntimeError("LISTA DE PRESENÇA vazia.")

    header = rows[0]
    body = rows[1:]

    i_dia = achar_coluna_dia(header, dd, mm, yyyy)
    if i_dia is None:
        raise RuntimeError(f"Coluna do dia {dd}/{mm}/{yyyy} NÃO encontrada na planilha.")
    else:
        print(f"[+] Coluna do dia {dd}/{mm}/{yyyy} encontrada!")

    i_matr  = encontrar_coluna(header, "Matrícula")
    i_nome  = encontrar_coluna(header, "COLABORADOR")
    i_coord = encontrar_coluna(header, "COORDENADOR")
    i_sup   = encontrar_coluna(header, "SUPERVISOR")
    i_area  = encontrar_coluna(header, "ÁREA")
    i_setor = encontrar_coluna(header, "PROCESSO")
    i_cargo = encontrar_coluna(header, "CARGO")
    i_turno = encontrar_coluna(header, "TURNO")
    i_emp   = encontrar_coluna(header, "EMPRESA")
    i_cid   = encontrar_coluna(header, "CIDADE")
    i_ponto = encontrar_coluna(header, "PONTO")
    i_linha = encontrar_coluna(header, "LINHA")
    i_stat  = encontrar_coluna(header, "STATUS")
    i_demi  = encontrar_coluna(header, "DATA DEMISSÃO")

    print("[+] Dados obtidos!")
    get = lambda r, i: (r[i].strip() if (i is not None and i < len(r) and r[i]) else "")

    abs_sh = gc.open_by_url(PLANILHA_ABS_URL)
    abs_ws = ver_cabecalho(abs_sh, ABA_ABS, cabecalho=ABS_cabecalho, cols=30)

    # ---------- Função auxiliar para converter datas ----------
    def _to_date_obj(s: str):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None

    # ---------- Decidir se SIGLA deve ser DES ----------
    def deve_marcar_des_sigla(data_falta: date, data_demissao_str: str) -> bool:
        data_demissao = _to_date_obj(data_demissao_str)
        if not data_demissao:
            return False
        return data_falta >= data_demissao

    print("[+] Realizando manipulações e filtros!")
    existentes = abs_ws.get_all_values()
    cab = existentes[0] if existentes else ABS_cabecalho

    # garante cabeçalho correto
    if not existentes:
        abs_ws.update([ABS_cabecalho])
        existentes = [ABS_cabecalho]
        cab = ABS_cabecalho

    idx_m = cab.index("MATRÍCULA")
    idx_d = cab.index("DATA")
    linha_map = {}
    for i, row in enumerate(existentes[1:], start=2):
        if len(row) <= max(idx_m, idx_d):
            continue
        k = (row[idx_m].strip(), row[idx_d].strip())
        if k[0] and k[1]:
            linha_map[k] = i

    novos = []
    atualizacoes = []

    for r in body:
        if not any((c or "").strip() for c in r):
            continue

        matric = get(r, i_matr)
        nome   = get(r, i_nome)
        area   = get(r, i_area)
        marticulas = {"37009053"}

        if area == "TSP - Nike Store" and matric not in marticulas:
            continue

        if not matric or not nome:
            continue

        sigla_origem = get(r, i_dia)          # F, P, etc.
        area         = get(r, i_area)
        emp          = get(r, i_emp)
        status_orig  = get(r, i_stat) or "ATIVO"
        data_demiss  = get(r, i_demi)

        if deve_marcar_des_sigla(dt, data_demiss):
            sigla_final = "DES"
        else:
            sigla_final = sigla_origem
        status_final = status_orig
        
        registro = [
            matric, nome, get(r, i_coord), get(r, i_sup), area, get(r, i_setor),
            get(r, i_cargo), get(r, i_turno), emp, get(r, i_cid),
            get(r, i_ponto), get(r, i_linha), get(r, i_demi), data_str,
            status_final, sigla_final,
            nome_mes_pt(datetime.strptime(data_str, "%d/%m/%Y")),
            classificar_regime(emp), canal_normalizado(area)
        ]

        key = (matric, data_str)
        if key in linha_map:
            atualizacoes.append((linha_map[key], registro))
        else:
            novos.append(registro)

    # --- insere novos ---
    if novos:
        print(f"[+] Adicinando {len(novos)} dados novos na planilha ABS FY!")
        abs_ws.append_rows(novos, value_input_option="USER_ENTERED")

    # --- atualiza existentes ---
    if atualizacoes:
        last_col = nome_coluna(len(ABS_cabecalho))
        payload = []
        for rownum, reg in atualizacoes:
            payload.append({
                "range": f"A{rownum}:{last_col}{rownum}",
                "values": [reg]
            })
        abs_ws.batch_update(payload)
        print(f"[+] Atualizando {len(atualizacoes)} dados. Adicinando  {len(novos)} novos ")

    print("[+] Aplicando fórmulas")

    def _norm(s): 
        return normaliza(s).strip()

    try:
        idx_area_abs  = next(i for i, h in enumerate(cab) if _norm(h) in ("AREA", "ÁREA"))
        idx_canal_abs = next(i for i, h in enumerate(cab) if _norm(h) == "CANAL")
    except StopIteration:
        raise RuntimeError("Colunas 'ÁREA/AREA' e 'CANAL'não encontradas no ABS FY.")

    area_col_letter  = nome_coluna(idx_area_abs + 1)
    canal_col_letter = nome_coluna(idx_canal_abs + 1)

    rows_to_formula = [r for r, _ in atualizacoes]

    if novos:
        start_new = len(existentes) + 1
        rows_to_formula.extend(range(start_new, start_new + len(novos)))

    rows_to_formula = sorted(set(rows_to_formula))

    if rows_to_formula:
        payload = []
        for rnum in rows_to_formula:
            formula = montar_formula_canal(area_col_letter, rnum)
            payload.append({"range": f"{canal_col_letter}{rnum}", "values": [[formula]]})
        abs_ws.batch_update(payload, value_input_option="USER_ENTERED")

    print("[+] Processo [PRESENÇA para ABS FY] finalizado!")
    return abs_ws

def atualizar_whs_to_percent(gc, data_str: str, to_percent_str: str):
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz) if not data_str else datetime.strptime(data_str, "%d/%m/%Y").replace(tzinfo=tz)
    ABA_WHS = f"Indicadores {nome_mes_pt(hoje)}"
    if not to_percent_str:
        print("[WHS] TO(%) vazio/indefinido; nada a escrever.")
        return
    try:
        whs = gc.open_by_url(PLANILHA_WHS_URL)
        ws = whs.worksheet(ABA_WHS)
    except Exception as e:
        print(f"[WHS] Falha ao abrir a planilha/aba: {e}")
        return
    linhas = ws.get_all_values()
    if not linhas:
        print("[WHS] Aba vazia.")
        return
    cab = linhas[0]
    try:
        dd, mm, yyyy = data_str.split("/")
    except Exception:
        print(f"[WHS] data_str inválida: {data_str}")
        return
    try:
        idx_indic = next(i for i, h in enumerate(cab) if normaliza(h) == "INDICADORES")
    except StopIteration:
        idx_indic = 1
    jcol = achar_coluna_data_whs(cab, dd, mm, yyyy)
    if jcol is None:
        print(f"[WHS] Coluna da data {data_str} não encontrada no cabeçalho.")
        return
    alvo_textos = "% TO"
    alvo_norm = {normaliza(x) for x in alvo_textos}
    row_idx = None
    rotulo_encontrado = None
    for i, row in enumerate(linhas[1:], start=2):
        rotulo = (row[idx_indic] if idx_indic < len(row) else "").strip()
        if normaliza(rotulo) in alvo_norm or ("TO" in normaliza(rotulo) and "%" in normaliza(rotulo)):
            row_idx = i
            rotulo_encontrado = rotulo or "(vazio)"
            break
    if row_idx is None:
        print("[WHS] Linha do indicador '%TO' não encontrada na coluna 'Indicadores'.")
        return
    col_label = nome_coluna(jcol + 2)
    celula = f"{col_label}{row_idx}"
    def parse_percent_maybe(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "").replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None
    x = parse_percent_maybe(to_percent_str)
    if x is None:
        print(f"[WHS] Valor inválido para TO(%): {to_percent_str!r}")
        return
    if x > 1.0:
        x = x / 100.0
    try:
        ws.format(celula, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
    except Exception as e:
        print(f"[WHS] Aviso ao formatar {celula} como percentual: {e}")
    try:
        ws.update(range_name=celula, values=[[x]], value_input_option="USER_ENTERED")
        print(f"[WHS] Atualizado {celula} ({rotulo_encontrado}) com {x:.6f} (fração).")
    except Exception as e:
        print(f"[WHS] Erro ao atualizar {celula}: {e}")

def atualizar_whs_abs_percent(gc, data_str: str, to_percent_str: str):
    
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz) if not data_str else datetime.strptime(data_str, "%d/%m/%Y").replace(tzinfo=tz)
    ABA_WHS = f"Indicadores {nome_mes_pt(hoje)}"
    
    if not to_percent_str:
        print("[WHS] ABS(%) vazio/indefinido; nada a escrever.")
        return
    try:
        whs = gc.open_by_url(PLANILHA_WHS_URL)
        ws = whs.worksheet(ABA_WHS)
    except Exception as e:
        print(f"[WHS] Falha ao abrir a planilha/aba: {e}")
        return
    linhas = ws.get_all_values()
    if not linhas:
        print("[WHS] Aba vazia.")
        return
    cab = linhas[0]
    try:
        dd, mm, yyyy = data_str.split("/")
    except Exception:
        print(f"[WHS] data_str inválida: {data_str}")
        return
    try:
        idx_indic = next(i for i, h in enumerate(cab) if normaliza(h) == "INDICADORES")
    except StopIteration:
        idx_indic = 1
    jcol = achar_coluna_data_whs(cab, dd, mm, yyyy)
    if jcol is None:
        print(f"[WHS] Coluna da data {data_str} não encontrada no cabeçalho.")
        return
    alvo_textos = "% ABS"
    alvo_norm = {normaliza(x) for x in alvo_textos}
    row_idx = None
    rotulo_encontrado = None
    for i, row in enumerate(linhas[1:], start=2):
        rotulo = (row[idx_indic] if idx_indic < len(row) else "").strip()
        if normaliza(rotulo) in alvo_norm or ("ABS" in normaliza(rotulo) and "%" in normaliza(rotulo)):
            row_idx = i
            rotulo_encontrado = rotulo or "(vazio)"
            break
    if row_idx is None:
        print("[WHS] Linha do indicador '%ABS' não encontrada na coluna 'Indicadores'.")
        return
    col_label = nome_coluna(jcol + 2)
    celula = f"{col_label}{row_idx}"
    def parse_percent_maybe(v):
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip().replace("%", "").replace(" ", "").replace(",", ".")
        try:
            return float(s)
        except:
            return None
    x = parse_percent_maybe(to_percent_str)
    if x is None:
        print(f"[WHS] Valor inválido para ABS(%): {to_percent_str!r}")
        return
    if x > 1.0:
        x = x / 100.0
    try:
        ws.format(celula, {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
    except Exception as e:
        print(f"[WHS] Aviso ao formatar {celula} como percentual: {e}")
    try:
        ws.update(range_name=celula, values=[[x]], value_input_option="USER_ENTERED")
        print(f"[WHS] Atualizado {celula} ({rotulo_encontrado}) com {x:.6f} (fração).")
    except Exception as e:
        print(f"[WHS] Erro ao atualizar {celula}: {e}")

# ============================= ABS PARA PLANEJAMENTO =================================
def abs_para_planejamento(gc, abs_area, data: str, ihc_override: int | None = None):
    print(f"[+] Coletando dados da data {data}!")
    # Pega todas as linhas da planilha ABS FY
    linhas = abs_area.get_all_values()
    alvo = data.strip()
    dd, mm, yyyy = alvo.split("/")
    dt = date(int(yyyy), int(mm), int(dd))
    
    mes_str = nome_mes_pt(dt)
    
    def _sanitize_row_as_user_entered_date(row):
        out = []
        for v in row:
            # Se vier datetime ou Timestamp, manda como ISO (Sheets entende como data real)
            if isinstance(v, (datetime, pd.Timestamp)):
                out.append(v.strftime("%Y-%m-%d"))
            else:
                out.append(v)
        return out

    def _to_date_obj(s: str):
        s = (s or "").strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None
    
    # Verificação caso a planilha ABS FY esteja vazia
    if not linhas or len(linhas) < 2:
        print("Planilha ABS FY está vazia!")
        return

    area_cab = linhas[0]
    area_lin = linhas[1:]
    df = pd.DataFrame(area_lin, columns=area_cab)

    # FOR que coleta os dados necessários da planilha ABS FY
    for col in ["DATA","EMPRESA","CONTRATO","TURNO","CIDADE","CANAL","STATUS","SIGLA","CARGO","DATA DEMISSÃO"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)
    
    # Copia a data requisitada pelo sistema
    df = df[df["DATA"].str.strip() == data].copy()
    if df.empty:
        print(f"A data requisitada [{data}] não consta na planilha ABS FY")
        print("Envio de dados não realizado.")
        return
    
    # Preenche as Flags com os dados necessários para os cálculos
    df["faltas"] = df["SIGLA"].apply(verificar_falta).astype(int)
    df["atestados"] = df["SIGLA"].apply(verificar_atestado).astype(int)
    df["abs"] = (df["faltas"] + df["atestados"]).clip(0,1)
    df["escalado"] = df["SIGLA"].apply(verificar_escalacao).astype(int)
    df["vinculo"] = df["EMPRESA"].apply(lambda e:"EFETIVO" if "FISIA" in normaliza(e) else "TEMPORARIO")
    df["turno"] = df["TURNO"].apply(turno_normalizado)
    df["canal"] = df["CANAL"].apply(canal_normalizado)
    df["grupo"] = df["CARGO"].apply(classificar_grupo)
    df["status"] = df["STATUS"] # DESLIGADO ou ATIVO
    df["demissao"] = df["DATA DEMISSÃO"] # Data de demissão do colaborador
  
    # Coleta o número de ABS sendo Faltas ou Atestados no DataFrame
    df_abs = df[df["abs"] == 1].copy()
    
    # Filtra considerando a data de demissão
    def deve_considerar_falta(row):
        # Se está ATIVO, sempre considera
        if row["status"] != "DESLIGADO":
            return True
        
        # Se está DESLIGADO, verifica se a falta foi ANTES da demissão
        data_demissao = _to_date_obj(row["demissao"])
        
        # Se não tem data de demissão válida, descarta
        if data_demissao is None:
            return False
        
        # Comparação falta (dt) < data_demissao = considera
        return dt < data_demissao
    
    df_abs = df_abs[df_abs.apply(deve_considerar_falta, axis=1)].copy()
    
    # Coleta os demais dados para os calculos, salvando-os nas variáveis 
    IHC_local = int(df["escalado"].sum())
    IHC = int(ihc_override) if ihc_override is not None else IHC_local  # usa Base mãe se houver

    faltas      = int(df_abs["faltas"].sum())
    atestados   = int(df_abs["atestados"].sum())
    abs_total   = int(df_abs.shape[0]) 
    efetivos    = int((df_abs["vinculo"] == "EFETIVO").sum()) 
    temporarios = int((df_abs["vinculo"] == "TEMPORARIO").sum()) 
    moi         = int((df_abs["grupo"] == "MOI").sum())
    mod         = int((df_abs["grupo"] == "MOD").sum())
    modp        = int((df_abs["grupo"] == "MOD+").sum())
    t1          = int((df_abs["turno"] == "1º").sum())
    t2          = int((df_abs["turno"] == "2º").sum())
    t3          = int((df_abs["turno"] == "3º").sum())
    t4          = int((df_abs["turno"] == "4º").sum())
    t5          = int((df_abs["turno"] == "5º").sum())
    t6          = int((df_abs["turno"] == "ADM").sum())

    # ------------------ CIDADES (CONSERVADOR): FIXAS + "OUTRAS" ------------------
    # Mapa normalizado -> nome canônico (apenas das fixas)
    cidades_norm_map = { normaliza(c): c for c in CIDADES_FIXAS }

    # Contador para todas as colunas de cidade (fixas + OUTRAS)
    cont_cidades = { c: 0 for c in CIDADES_COLUNAS }

    # Conta as cidades das faltas/atestados; o que não é fixo vai para OUTRAS
    for raw_city, v in df_abs["CIDADE"].value_counts().items():
        chave = cidade_normalizada(str(raw_city))
        k = normaliza(chave)
        if k in cidades_norm_map:
            cont_cidades[cidades_norm_map[k]] += int(v)
        else:
            cont_cidades["OUTRAS CIDADES"] += int(v)

    # ------------------ CABEÇALHO COMPLETO (fixo) ------------------
    PLAN_agencias = ["ADECCO","DPX","FISIA","FENIX","SERTEC","MENDES"]
    cab_plan_sis = PLAN_cabecalho_ini + CIDADES_COLUNAS + PLAN_cab_canais + PLAN_agencias

    # Carrega o cabecalho completo da planilha de planejamento do google sheets 
    planilha_plan_on = gc.open_by_url(PLANILHA_MAE_URL)
    Planilha_plan_02 = gc.open_by_url(PLANILHA_ABS_URL)
    
    area_plan_on = ver_cabecalho(planilha_plan_on, ABA_PLA_ABS, cab_plan_sis, cols=max(120, len(cab_plan_sis)))
    area_plan_o2 = ver_cabecalho(Planilha_plan_02, ABA_PLA_ABS, cab_plan_sis, cols=max(120, len(cab_plan_sis)))
    print("[+] Realizando manipulação automática dos dados!")
    # Atualiza a primeira linha com cabeçalho fixo e limpa excedentes
    
    cabecalho_atual = area_plan_on.get_values("1:1")
    cabecalho_atual = cabecalho_atual[0] if cabecalho_atual else []
    
    area_plan_on.update([cab_plan_sis], range_name="1:1")
    area_plan_o2.update([cab_plan_sis], range_name="1:1")

    if len(cabecalho_atual) > len(cab_plan_sis):
        inicio_clear = nome_coluna(len(cab_plan_sis)+1) + "1" 
        fim_clear    = nome_coluna(len(cabecalho_atual)) + "1"
        area_plan_on.update([[""] * (len(cabecalho_atual)-len(cab_plan_sis))], range_name=f"{inicio_clear}:{fim_clear}")
        area_plan_o2.update([[""] * (len(cabecalho_atual)-len(cab_plan_sis))], range_name=f"{inicio_clear}:{fim_clear}")

    # --- Formatação das colunas (inclui DATA como data real no Sheets) ---
    last_col_label = nome_coluna(len(cab_plan_sis)) if cab_plan_sis else "A"
    if len(cab_plan_sis) >= 2:
        area_plan_on.format(f"B2:{last_col_label}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
        area_plan_o2.format(f"B2:{last_col_label}", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})

    # Garante a coluna A como DATA (dd/mm/yyyy)
    area_plan_on.format("A2:A", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}})
    area_plan_o2.format("A2:A", {"numberFormat": {"type": "DATE", "pattern": "dd/mm/yyyy"}})

    if "ABS(%)" in cab_plan_sis:
        abs_idx = cab_plan_sis.index("ABS(%)") + 1
        abs_col_label = nome_coluna(abs_idx)
        area_plan_on.format(f"{abs_col_label}2:{abs_col_label}", {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}})
        area_plan_o2.format(f"{abs_col_label}2:{abs_col_label}", {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}})

    abs_percent = ""
    try:
        ihc_num = float(IHC)
        if ihc_num > 0:
            abs_percent = round(abs_total / ihc_num * 100, 2)
            abs_percent_ = round(abs_total / ihc_num, 4)
            abs_percent_str = f"{abs_percent}%"
    except Exception:
        pass

    # 1) Converta a data para ISO (string). Nada de datetime dentro de 'linha'.
    data_iso = datetime.strptime(data, "%d/%m/%Y").strftime("%Y-%m-%d")

    # 2) Monte a linha normalmente, começando pela data ISO
    linha = [data_iso, mes_str, IHC, abs_total, abs_percent_, faltas, atestados,
            efetivos, temporarios, moi, mod, modp, t1, t2, t3, t4, t5, t6]

    # Cidades
    for c in CIDADES_COLUNAS:
        linha.append(int(cont_cidades.get(c, 0)))

    # Canais
    canal_cont = {k: 0 for k in PLAN_cab_canais}
    for k, v in df_abs["canal"].value_counts().items():
        if k in canal_cont:
            canal_cont[k] = int(v)
    for c in PLAN_cab_canais:
        linha.append(int(canal_cont.get(c, 0)))
   
    # Agências (EMPRESA)
    agencia_cont = {k: 0 for k in PLAN_agencias}
    for emp, v in df_abs["EMPRESA"].value_counts().items():
        emp_norm = normaliza(emp)
        for nome_ag in PLAN_agencias:
            if nome_ag in emp_norm:
                agencia_cont[nome_ag] += int(v)
                break
    for a in PLAN_agencias:
        linha.append(int(agencia_cont.get(a, 0)))

    if len(linha) < len(cab_plan_sis):
        linha += [""] * (len(cab_plan_sis) - len(linha))

    # 3) Sanitiza a linha para garantir que não há datetime "perdido"
    linha = _sanitize_row_as_user_entered_date(linha)

    # ---- UPSERT por DATA na coluna A ----
    colA = area_plan_on.get_values("A2:A")
    row_to_update = None

    # Compare datas como datas (aceita dd/mm/yyyy ou yyyy-mm-dd)
    alvo = _to_date_obj(data)  # data que você passou à função (dd/mm/yyyy)
    for idx, cel in enumerate(colA, start=2):
        val = (cel[0] if cel else "").strip()
        if _to_date_obj(val) == alvo:
            row_to_update = idx
            break

    if row_to_update:
        start = f"A{row_to_update}"
        end   = f"{nome_coluna(len(cab_plan_sis))}{row_to_update}"
        area_plan_on.update([linha], range_name=f"{start}:{end}",
                            value_input_option="USER_ENTERED")
        area_plan_o2.update([linha], range_name=f"{start}:{end}",
                            value_input_option="USER_ENTERED")
        print(f"[+] Dados da data {data} atualizados com sucesso na linha [A{row_to_update}]!")
    else:
        area_plan_on.append_row(linha, value_input_option="USER_ENTERED")
        area_plan_o2.append_row(linha, value_input_option="USER_ENTERED")
        print(f"[+] Dados da data {data} adicionados com sucesso!")

    return abs_percent_str

# ============================= RESUMOS MENSAIS =================================
def to_mes(gc, data: str):
    # Abre TO FY
    to_sh = gc.open_by_url(PLANILHA_TO_URL)
    to_ws = ver_cabecalho(to_sh, ABA_PLA_TO)
    
    vals_fmt = to_ws.get_all_values()
    rows = len(vals_fmt)
    cols = max(len(r) for r in vals_fmt) if rows else 0
    rng = f"A1:{gspread.utils.rowcol_to_a1(rows, cols)}"
    linhas = to_ws.get(rng, value_render_option='UNFORMATTED_VALUE')

    cab, dados = linhas[0], linhas[1:]
    df_all = pd.DataFrame(dados, columns=cab)

    for col in ["TO","I.H.C"]:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0)

    def to_decimal(x):
        if x in (None, "", " ",): return Decimal(0)
        try:
            return Decimal(str(x))
        except Exception:
            return Decimal(0)
        
    df_all["TO(%)"] = df_all["TO(%)"].apply(to_decimal)

    # 1) Agregações por mês
    agg = (df_all
           .groupby("MES", as_index=False)
           .agg(SOMA_TO=("TO","sum"),
                SOMA_IHC=("I.H.C","sum"),
                SOMA_TO_PCT=("TO(%)","sum"),
                CONT_IHC=("I.H.C","count")))
    
    agg["MEDIA_IHC"] = (agg["SOMA_IHC"] / agg["CONT_IHC"]).replace(0, np.nan)
    agg["TO_PCT"] = (agg["SOMA_TO"] / agg["MEDIA_IHC"]*100).fillna(0)

    agg["MES"] = pd.Categorical(agg["MES"], categories=ORDEM_MESES, ordered=True)
    agg = agg.sort_values("MES")
    agg["SOMA_TO_PCT"] = agg["SOMA_TO_PCT"] * 100
    df_final = agg.loc[:, ["MES","SOMA_TO","SOMA_TO_PCT"]].rename(
        columns={"MES":"MÊS","SOMA_TO":"TO","SOMA_TO_PCT":"TO(%)"}
    )
    
    df_final["TO"] = df_final["TO"].round(0).astype(int)
    df_final["TO(%)"] = (
        pd.to_numeric(df_final["TO(%)"], errors="coerce")
        .fillna(0)
        .round(DECIMAIS)
        .map(lambda x: f"{x:.{DECIMAIS}f}%".replace(".", ","))
    )


    # === Upsert na aba "Resumo Mensal" da mesma planilha ===
    upsert_resumo_mensal_to(to_sh, df_final, aba_nome="Resumo Mensal")
    print("\n[OK] Aba 'Resumo Mensal' atualizada.")

    return df_final

def abs_mes(gc):
    abs_sh = gc.open_by_url(PLANILHA_ABS_URL)
    abs_ws = ver_cabecalho(abs_sh, ABA_PLA_ABS)

    # Leia valores BRUTOS (sem formatação) para somar igual ao Excel
    vals_fmt = abs_ws.get_all_values()
    rows = len(vals_fmt)
    cols = max(len(r) for r in vals_fmt) if rows else 0
    rng = f"A1:{gspread.utils.rowcol_to_a1(rows, cols)}"
    linhas = abs_ws.get(rng, value_render_option='UNFORMATTED_VALUE')

    cab, dados = linhas[0], linhas[1:]
    df_all = pd.DataFrame(dados, columns=cab)

    # Numéricos
    for col in ["ABS","I.H.C","ATESTADOS","FALTAS"]:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce").fillna(0)

    # ABS(%) pode vir como fração ou como número; garanta Decimal p/ somar com mais precisão
    def to_decimal(x):
        if x in (None, "", " ",): return Decimal(0)
        try:
            return Decimal(str(x))
        except Exception:
            return Decimal(0)

    df_all["ABS(%)"] = df_all["ABS(%)"].apply(to_decimal)

    agrupado = (df_all
        .groupby("MES", as_index=False)
        .agg(SOMA_ABS=("ABS","sum"),
             SOMA_IHC=("I.H.C","sum"),
             SOMA_AT=("ATESTADOS","sum"),
             SOMA_F=("FALTAS","sum"),
             SOMA_ABS_PCT=("ABS(%)","sum"),
             CONT_IHC=("I.H.C","count"))
    )
    agrupado["MEDIA_IHC"] = (agrupado["SOMA_IHC"] / agrupado["CONT_IHC"]).replace(0, np.nan)
    agrupado["ABS_PCT"] = (agrupado["SOMA_ABS_PCT"] / agrupado["CONT_IHC"] * 100).fillna(0) 
    agrupado["MES"] = pd.Categorical(agrupado["MES"], categories=ORDEM_MESES, ordered=True)
    agrupado = agrupado.sort_values("MES")
    #agrupado["SOMA_ABS_PCT"] = agrupado["SOMA_ABS_PCT"]*100

    df_final = agrupado.loc[:, ["MES","SOMA_ABS","ABS_PCT","SOMA_F","SOMA_AT"]].rename(
        columns={"MES":"MÊS","SOMA_ABS":"ABS","ABS_PCT":"ABS(%)","SOMA_F":"FALTAS","SOMA_AT":"ATESTADOS"}
    )
    
    # ABS inteiro
    df_final["ABS"] = df_final["ABS"].round(0).astype(int)

    df_final["ABS(%)"] = df_final["ABS(%)"].apply(lambda d: float(d)) \
                                           .round(DECIMAIS) \
                                           .map(lambda x: f"{x:.{DECIMAIS}f}%".replace(".", ","))

    upsert_resumo_mensal_abs(abs_sh, df_final, aba_nome="Resumo Mensal")
    print("\n[ABS] Aba 'Resumo mensal' atualizada")

    return df_final

def upsert_resumo_mensal_to(to_sh: gspread.Spreadsheet, df_final: pd.DataFrame, aba_nome: str = "Resumo Mensal"):
    # Ano de referência (pegando do seu DATA_ALVO, se preferir passe como parâmetro)
    ano_ref = datetime.strptime("10/10/2025", "%d/%m/%Y").year

    # Mapa com chaves em lowercase (sem acento faz diferença? aqui mantemos com acento)
    mapa_datas_lc = {
        "janeiro":   f"01/01/{ano_ref}",
        "fevereiro": f"01/02/{ano_ref}",
        "março":     f"01/03/{ano_ref}",
        "marco":     f"01/03/{ano_ref}",
        "abril":     f"01/04/{ano_ref}",
        "maio":      f"01/05/{ano_ref}",
        "junho":     f"01/06/{ano_ref}",
        "julho":     f"01/07/{ano_ref}",
        "agosto":    f"01/08/{ano_ref}",
        "setembro":  f"01/09/{ano_ref}",
        "outubro":   f"01/10/{ano_ref}",
        "novembro":  f"01/11/{ano_ref}",
        "dezembro":  f"01/12/{ano_ref}",
    }

    def norm_mes_para_data(s: str) -> str:
        """Converte 'janeiro'/'Janeiro' etc. OU 'dd/mm/yyyy' para chave única 'dd/mm/yyyy'."""
        if s is None:
            return ""
        s = str(s).strip()
        # já é data?
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if pd.notna(dt):
            return dt.strftime("%d/%m/%Y")
        # tenta mapear nome do mês
        key = s.lower()
        return mapa_datas_lc.get(key, s)  # se não reconhecer, devolve s (evita perder dado)

    # Normaliza df_final
    df_final = df_final.copy()
    df_final["MÊS"] = df_final["MÊS"].astype(str).map(norm_mes_para_data)

    # Abre/Cria worksheet
    try:
        ws = to_sh.worksheet(aba_nome)
    except gspread.WorksheetNotFound:
        ws = to_sh.add_worksheet(title=aba_nome, rows=100, cols=10)
        ws.update('A1', [["MÊS","TO","TO(%)"]])

    # Lê existente
    try:
        df_exist = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    except Exception:
        valores = ws.get_all_values()
        if not valores:
            df_exist = pd.DataFrame(columns=["MÊS","TO","TO(%)"])
        else:
            cab, dados = valores[0], valores[1:]
            df_exist = pd.DataFrame(dados, columns=cab)

    # Normaliza existente para as mesmas colunas e mesmo formato de chave
    cols = ["MÊS","TO","TO(%)"]
    for c in cols:
        if c not in df_exist.columns:
            df_exist[c] = ""
    df_exist = df_exist[cols].copy()

    # Converte TO existente para int e MÊS para data-string padronizada
    with pd.option_context('future.no_silent_downcasting', True):
        df_exist["TO"] = pd.to_numeric(df_exist["TO"], errors="coerce").fillna(0).astype(int)
    df_exist["MÊS"] = df_exist["MÊS"].astype(str).map(norm_mes_para_data)

    # Concatena e mantém a ÚLTIMA ocorrência por MÊS (df_final tem prioridade)
    base = pd.concat([df_exist, df_final], ignore_index=True)
    base = (base
            .dropna(subset=["MÊS"])
            .drop_duplicates(subset=["MÊS"], keep="last"))

    # Ordena cronologicamente
    base["_ord"] = pd.to_datetime(base["MÊS"], format="%d/%m/%Y", dayfirst=True, errors="coerce")
    base = base.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)

    # Grava
    ws.clear()
    set_with_dataframe(ws, base, include_column_header=True)
    ws.format("A2:A", {"numberFormat": {"type": "DATE", "pattern": "mmmm"}})
    return base

def upsert_resumo_mensal_abs(to_sh: gspread.Spreadsheet, df_final: pd.DataFrame, aba_nome: str = "Resumo Mensal"):
    
    # === 2) Normaliza df_final ===
    df_final = df_final.copy()
    df_final["MÊS"] = df_final["MÊS"].astype(str).map(norm_mes_para_data)

    # garante colunas esperadas
    for col in ["ABS","ABS(%)","FALTAS","ATESTADOS"]:
        if col not in df_final.columns:
            df_final[col] = 0 if col in ("ABS","FALTAS","ATESTADOS") else ""

    # Números (mantém ABS(%) como string)
    with pd.option_context('future.no_silent_downcasting', True):
        for c in ["ABS","FALTAS","ATESTADOS"]:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").fillna(0).astype(int)

    try:
        ws = to_sh.worksheet(aba_nome)
    except gspread.WorksheetNotFound:
        ws = to_sh.add_worksheet(title=aba_nome, rows=200, cols=10)
        ws.update('A1', [["MÊS","ABS","ABS(%)","FALTAS","ATESTADOS"]])

    try:
        df_exist = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    except Exception:
        valores = ws.get_all_values()
        if not valores:
            df_exist = pd.DataFrame(columns=["MÊS","ABS","ABS(%)","FALTAS","ATESTADOS"])
        else:
            cab, dados = valores[0], valores[1:]
            df_exist = pd.DataFrame(dados, columns=cab)

    cols = ["MÊS","ABS","ABS(%)","FALTAS","ATESTADOS"]
    for c in cols:
        if c not in df_exist.columns:
            df_exist[c] = ""
    df_exist = df_exist[cols].copy()

    df_exist["MÊS"] = df_exist["MÊS"].astype(str).map(norm_mes_para_data)
    with pd.option_context('future.no_silent_downcasting', True):
        for c in ["ABS","FALTAS","ATESTADOS"]:
            df_exist[c] = pd.to_numeric(df_exist[c], errors="coerce").fillna(0).astype(int)

    base = pd.concat([df_exist, df_final], ignore_index=True)
    base = (base
            .dropna(subset=["MÊS"])
            .drop_duplicates(subset=["MÊS"], keep="last"))

    base["_ord"] = pd.to_datetime(base["MÊS"], format="%d/%m/%Y", dayfirst=True, errors="coerce")
    base = base.sort_values("_ord").drop(columns=["_ord"]).reset_index(drop=True)

    ws.clear()
    set_with_dataframe(ws, base, include_column_header=True)
    ws.format("A2:A", {"numberFormat": {"type": "DATE", "pattern": "mmmm"}})
    
    return base
# ============================ ATUALIZAR SIMULADOR ===============================

DEPENDENCIAS = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

PLANILHA_QHC_URL = "https://docs.google.com/spreadsheets/d/1wxwncI3t62D6vkIGiazVR-hoGnBCW2-CBgZJB5aruZ8/edit"
ABA_QHC = "Dinâmicas"

PLANILHA_SIMULADOR_URL = "https://docs.google.com/spreadsheets/d/1TjgzE29011UwYskqlwVxfC39d36lxZtdR1zRZDRg560/edit"
ABA_SIMULADOR = "Check Quadro"

SETOR_MAP_EXATO = {
    "INBOUND": {
        "DESCARREGAMENTO": "Descarregamento",
        "RECEBIMENTO": "Recebimento",
        "VOLANTE RECEBIMENTO": "Volante Recebimento",
        "CUBSCAN": "Cubscan",
        "VOLANTE RFID": "Volante RFID",
        "PORTAL RFID INBOUND": "Portal RFID Inbound",
        "INSERCAO ETIQUETA RFID": "Inserção etiqueta RFID",
        "VOLANTE BUFFER": "Volante Buffer",
        "CRIACAO CARTONAGEM": "Criação Cartonagem",
        "PEDAGIO": "Pedágio",
        "ROTEIRIZACAO": "Roteirização",
        "CARREGAMENTO": "Carregamento",
        "DESCARREGAMENTO CROSSDOCKING": "Descarregamento Crossdocking",
        "CONFERENCIA": "Conferência",
        "TRIAGEM P/ ENDERECAMENTO DIGITAL": "Triagem p/ endereçamento Digital",
        "VAS": "VAS",
        "ABS": "ABS",
    },
}

SETORES_MATCH_EXATO_OBRIGATORIO = [
    "CONFERENCIA MANUAL",
    "MOVIMENTACAO DE ESTOQUE E ARMAZENAGEM PK",
]

MAP_PROCESSO_SIM_PRA_QHC = {
    "REVERSA DIGITAL": ["REVERSA - DIGITAL"],
    "REVERSA E RTV": ["REVERSA - DIGITAL"],
    "ROTA SP": ["ROTA SP"],
    "WHOLESALE": ["WHOLESALE"],
    "AUDITORIA / INVENTARIO": ["AUDITORIA / INVENTARIO"],
}

# ==== Mapeamento específico de setores do Simulador -> setores do QHC
MAP_SETOR_REVERSA_RTV = {
    "REVERSA": {
        "processo_qhc": "Reversa - Digital",
        "setor_qhc": "Reversa RTV",
    },
}

def remove_acento(s: str) -> str:
    s = (s or "").strip().upper() 
    return ''.join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")

def normaliza(s: str) -> str:
    return remove_acento((s or "").strip().upper())

def to_int(v):
    try:
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return 0

# =====================================
# LEITURA E ESTRUTURAÇÃO DO QHC 
# ===================================== 

def listar_processos_setores():
    creds = _obter_creds()
    gc = gspread.authorize(creds)

    qhc_sh = gc.open_by_url(PLANILHA_QHC_URL)
    ws_qhc = qhc_sh.worksheet(ABA_QHC)
    qhc_vals = ws_qhc.get_all_values()

    if len(qhc_vals) < 3:
        print("[-] QHC sem dados suficientes")
        return {}

    dados_estruturados = {}

    for row in qhc_vals[2:]:
        if len(row) < 7:
            continue

        processo_raw = row[0].strip()
        proc_norm = normaliza(processo_raw)

        if "TOTAL GERAL" in proc_norm:
            print("[+] Encontrado 'TOTAL GERAL' - parando leitura do QHC")
            break

        setor_raw = row[1].strip()
        setor_norm = normaliza(setor_raw)

        processo = processo_raw
        setor = setor_raw

        '''# ROTA SP + WHOLESALE -> ROTA SP
        if proc_norm in ("ROTA SP", "WHOLESALE"):
            processo = "ROTA SP"
            setor = "ROTA SP"'''
        # PERSON -> Faturamento + Packing
        if setor_norm == "PERSON":
            setor = "Faturamento + Packing"

        if not processo or not setor:
            continue

        if "TOTAL" in normaliza(setor_raw):
            continue

        t1 = to_int(row[2])
        t2 = to_int(row[3])
        t3 = to_int(row[4])
        t4 = to_int(row[5])

        if processo not in dados_estruturados:
            dados_estruturados[processo] = {}

        if setor not in dados_estruturados[processo]:
            dados_estruturados[processo][setor] = {"T1": 0, "T2+T4": 0, "T3": 0}

        dados_estruturados[processo][setor]["T1"] += t1
        dados_estruturados[processo][setor]["T2+T4"] += t2
        dados_estruturados[processo][setor]["T3"] += t3

    print(f"[+] QHC: {len(dados_estruturados)} processos lidos")
    return dados_estruturados

def montar_indice_qhc(dados_estruturados: dict) -> dict:
    indice = {}
    for processo, setores in dados_estruturados.items():
        proc_norm = normaliza(processo)
        for setor in setores.keys():
            setor_norm = normaliza(setor)
            indice.setdefault(proc_norm, []).append((setor_norm, processo, setor))
    return indice

# ======================== 
# FUNÇÕES DE MATCH 
# ======================== 

def achar_match_qhc_inteligente(proc_sim: str,setor_sim: str,indice_qhc: dict,):
    proc_sim_norm = normaliza(proc_sim)
    setor_sim_norm = normaliza(setor_sim)

    candidatos_proc = []

    if proc_sim_norm in indice_qhc:
        candidatos_proc.append(proc_sim_norm)

    mapeados = MAP_PROCESSO_SIM_PRA_QHC.get(proc_sim_norm)
    if mapeados:
        for p in mapeados:
            p_norm = normaliza(p)
            if p_norm in indice_qhc and p_norm not in candidatos_proc:
                candidatos_proc.append(p_norm)

    if not candidatos_proc:
        candidatos_proc = list(indice_qhc.keys())

    melhor_match = (None, None)
    melhor_score = 0

    for proc_norm in candidatos_proc:
        for setor_norm_qhc, processo_orig, setor_orig in indice_qhc[proc_norm]:
            score = 0
            if setor_norm_qhc == setor_sim_norm:
                score = 100
            elif (
                setor_norm_qhc.startswith(setor_sim_norm)
                or setor_sim_norm.startswith(setor_norm_qhc)
            ):
                score = 90

            if score > melhor_score:
                melhor_score = score
                melhor_match = (processo_orig, setor_orig)

    if melhor_score >= 90:
        return melhor_match
    else:
        return (None, None)

def achar_setor_enxoval(dados_estruturados: dict):
    alvo = normaliza("Enxoval")
    for processo, setores in dados_estruturados.items():
        for setor in setores.keys():
            if normaliza(setor) == alvo:
                print(f"[+] Encontrado 'Enxoval' no processo '{processo}'")
                return processo, setor
    print("[-] Não encontrei setor 'Enxoval' no QHC")
    return None, None

def achar_triagem_arm_nike_store(dados_estruturados: dict):
    alvo = normaliza("Traigem Arm Nike Store")
    print(f"[*] Procurando setor especial (Triagem/Traigem) com alvo normalizado: '{alvo}'")

    for processo, setores in dados_estruturados.items():
        print(f"    - Processo QHC: '{processo}' (normalizado: '{normaliza(processo)}')")
        if normaliza(processo) == "INBOUND":
            print("    -> Processo INBOUND encontrado, varrendo setores...")
            for setor_nome in setores.keys():
                setor_norm = normaliza(setor_nome)
                print(f"    setor_qhc='{setor_nome}' (norm='{setor_norm}')")
                if alvo in setor_norm:
                    print(
                        f"[+] Traigem/Triagem Arm. Nike Store encontrada em "
                        f"'{processo}' (Inbound) como '{setor_nome}'"
                    )
                    return processo, setor_nome

    print("[*] Não achei no processo INBOUND, tentando em qualquer processo...")
    for processo, setores in dados_estruturados.items():
        for setor_nome in setores.keys():
            if alvo in normaliza(setor_nome):
                print(
                    f"[+] Traigem/Triagem Arm. Nike Store encontrada em "
                    f"'{processo}' (fallback) como '{setor_nome}'"
                )
                return processo, setor_nome

    print("[-] Não encontrei 'Traigem/Triagem Arm. Nike Store' no QHC")
    return None, None

def achar_match_qhc(
    proc_sim: str,
    setor_sim: str,
    dados_estruturados: dict,
    indice_qhc: dict,
):
    proc_sim_norm = normaliza(proc_sim)
    setor_sim_norm = normaliza(setor_sim)

    if setor_sim_norm in [normaliza(s) for s in SETORES_MATCH_EXATO_OBRIGATORIO]:
        print(f"[!] '{setor_sim}' exige match EXATO (processo + setor) - verificando...")
        if proc_sim in dados_estruturados and setor_sim in dados_estruturados[proc_sim]:
            print(f"[✓] Match exato encontrado para '{proc_sim} | {setor_sim}'")
            return proc_sim, setor_sim
        print(f"[X] Match exato NÃO encontrado para '{proc_sim} | {setor_sim}' - ignorando")
        return None, None

    if proc_sim_norm == "ADICIONAIS" and setor_sim_norm == normaliza("Projeto Enxoval"):
        print("[*] Caso especial: ADICIONAIS | Projeto Enxoval -> buscando 'Enxoval' no QHC...")
        proc_enx, setor_enx = achar_setor_enxoval(dados_estruturados)
        if proc_enx and setor_enx:
            return proc_enx, setor_enx
        else:
            return None, None

    if proc_sim_norm == "ADICIONAIS" and normaliza("Triagem Arm Nike Store") in setor_sim_norm:
        print(
            "[*] Caso especial: ADICIONAIS | Triagem Arm. Nike Store -> "
            "buscando 'Traigem Arm. Nike Store' no QHC..."
        )
        proc_tri, setor_tri = achar_triagem_arm_nike_store(dados_estruturados)
        if proc_tri and setor_tri:
            return proc_tri, setor_tri
        else:
            return None, None

    if proc_sim_norm == normaliza("REVERSA e RTV"):
        info_setor = MAP_SETOR_REVERSA_RTV.get(setor_sim_norm)
        if info_setor:
            proc_qhc = info_setor["processo_qhc"]
            setor_qhc = info_setor["setor_qhc"]
            print(
                f"[*] Caso especial: REVERSA e RTV | {setor_sim} -> "
                f"QHC: {proc_qhc} | {setor_qhc}"
            )
            if proc_qhc in dados_estruturados and setor_qhc in dados_estruturados[proc_qhc]:
                return proc_qhc, setor_qhc
            else:
                print(
                    "[-] Mapeamento especial REVERSA e RTV não encontrado no QHC "
                    f"({proc_qhc} | {setor_qhc})"
                )

    if proc_sim_norm in SETOR_MAP_EXATO:
        mapa_setores = SETOR_MAP_EXATO[proc_sim_norm]
        if setor_sim_norm in mapa_setores:
            setor_qhc = mapa_setores[setor_sim_norm]
            processo_qhc = proc_sim
            if (
                processo_qhc in dados_estruturados
                and setor_qhc in dados_estruturados[processo_qhc]
            ):
                return processo_qhc, setor_qhc

    return achar_match_qhc_inteligente(
        proc_sim,
        setor_sim,
        indice_qhc,
    )

# ================================= 
# PREENCHIMENTO DO SIMULADOR 
# =================================

def preencher_simulador(dados_estruturados, data_ref: str | None = None):
    if not dados_estruturados:
        print("[-] Nenhum dado estruturado recebido.")
        return

    if data_ref is None:
        try:
            tz = ZoneInfo("America/Sao_Paulo")
            hoje = datetime.now(tz)
        except Exception:
            hoje = datetime.now()
        data_ref = hoje.strftime("%d/%m")

    print(f"[+] Usando data de referência: {data_ref}")

    creds = _obter_creds()
    gc = gspread.authorize(creds)

    sim_sh = gc.open_by_url(PLANILHA_SIMULADOR_URL)
    ws_sim = sim_sh.worksheet(ABA_SIMULADOR)
    sim_vals = ws_sim.get_all_values()

    col_dia = None
    for i, row in enumerate(sim_vals[:10]):
        for j, v in enumerate(row):
            texto = (v or "").strip()
            if f"ACT - {data_ref}" in texto or f"ACT-{data_ref}" in texto:
                col_dia = j
                print(f"[+] Dia encontrado na coluna {j + 1}, linha {i + 1} -> '{texto}'")
                break
        if col_dia is not None:
            break

    if col_dia is None:
        print(f"[-] Não encontrei coluna do dia ACT - {data_ref}")
        return

    col_1t = col_dia
    col_2t = col_dia + 1
    col_3t = col_dia + 2

    # colunas fixas para ACT 1T, 2T, 3T (H, I, J)
    COL_ACT_1T_FIXO = 8   # H
    COL_ACT_2T_FIXO = 9   # I
    COL_ACT_3T_FIXO = 10  # J

    COL_SETOR = 22  # coluna W

    PROCESSOS_VALIDOS = [
        "Inbound",
        "Armazenagem - Digital",
        "PROCESSOS EXTRAS",
        "Outbound - Digital",
        "reversa digital",
        "Armazenagem - Nike Store",
        "PROCESSOS EXTRAS",
        "Outbound - Nike Store",
        "REVERSA e RTV",
        "ADICIONAIS",
    ]
    PROCESSOS_VALIDOS_NORM = [normaliza(p) for p in PROCESSOS_VALIDOS]

    indice_qhc = montar_indice_qhc(dados_estruturados)

    updates = []
    processo_atual = ""
    linhas_atualizadas = 0

    for i in range(4, len(sim_vals)):
        row = sim_vals[i]
        if len(row) <= COL_SETOR:
            continue

        cel = (row[COL_SETOR] or "").strip()
        if not cel:
            continue

        cel_norm = normaliza(cel)

        if "TOTAL" in cel_norm:
            processo_atual = ""
            continue

        if "ABS" in cel_norm and "TOTAL" in cel_norm:
            continue

        if cel_norm in PROCESSOS_VALIDOS_NORM:
            processo_atual = cel
            print(f"[+] Processo atual: {processo_atual} (linha {i + 1})")
            continue

        if not processo_atual:
            continue

        setor_linha = cel

        processo_qhc, setor_qhc = achar_match_qhc(
            processo_atual,
            setor_linha,
            dados_estruturados,
            indice_qhc,
        )

        # Se não houver match, ainda assim escreve 0 nas colunas ACT
        if not processo_qhc or not setor_qhc:
            print(f"[-] Sem match para: {processo_atual} | {setor_linha} -> preenchendo 0")
            dados_setor = {"T1": 0, "T2+T4": 0, "T3": 0}
        else:
            dados_setor = dados_estruturados[processo_qhc][setor_qhc]

        t1 = dados_setor["T1"]
        t2 = dados_setor["T2+T4"]
        t3 = dados_setor["T3"]

        r1 = i + 1

        # ACT do dia
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, col_1t + 1),
                "values": [[t1]],
            }
        )
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, col_2t + 1),
                "values": [[t2]],
            }
        )
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, col_3t + 1),
                "values": [[t3]],
            }
        )

        # Cópia do ACT para as colunas fixas H (1T), I (2T), J (3T)
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, COL_ACT_1T_FIXO),
                "values": [[t1]],
            }
        )
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, COL_ACT_2T_FIXO),
                "values": [[t2]],
            }
        )
        updates.append(
            {
                "range": gspread.utils.rowcol_to_a1(r1, COL_ACT_3T_FIXO),
                "values": [[t3]],
            }
        )

        linhas_atualizadas += 1
        print(
            f"[=] {processo_atual} | {setor_linha} -> "
            f"1T={t1} 2T={t2} 3T={t3} "
            f"(QHC: {processo_qhc} | {setor_qhc})"
        )

    if not updates:
        print("[-] Nenhuma linha casou entre QHC e Simulador.")
        return

    print(f"[+] Atualizando {len(updates)} células ({linhas_atualizadas} linhas)...")
    ws_sim.batch_update(updates)
    print(f"[✓] Concluído para a data {data_ref}")

# ============================= ATUALIZAR QUADRO FY =================================
def nome_coluna_at(n: int) -> str:
    letra = ""
    while n:
        n, r = divmod(n-1, 26)
        letra = chr(65 + r) + letra
    return letra

def col_a1_from_idx_zero_based(i0: int) -> str:
        return nome_coluna_at(i0 + 1)

def normaliza_at(s: str) -> str:
    """
    Deixa tudo MAIÚSCULO, sem acento e sem espaços extras.
    """
    s = str(s or "").strip().upper()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s
def idx_por_nome_at(header: list[str], *possiveis: str) -> int | None:
    """
    Procura o índice de uma coluna no cabeçalho usando vários nomes possíveis.
    Comparação normaliza_atda (maiúsculo, sem acento).
    """
    header_norm = [normaliza_at(h) for h in header]
    for nome in possiveis:
        alvo = normaliza_at(nome)
        for i, h in enumerate(header_norm):
            if h == alvo:
                return i
    return None

def buscar_qhc_contagens_at(data_str: str) -> dict | None:
    try:
        dd, mm, yyyy = data_str.split("/")
        creds = _obter_creds()
        gc = gspread.authorize(creds)

        qhc = gc.open_by_url(PLANILHA_QHC_URL)
        #
        # tivos = aba_qhc_ativos_from_data(data_str)
        aba_ativos = "H.C. TT"
        ws = qhc.worksheet("H.C. TT")
        #ws = qhc.worksheet(aba_ativos)

        linhas = ws.get_all_values()
        if not linhas or len(linhas) < 2:
            return {
                # IHC / MOD / FSOP
                "IHC": 0,
                "MODP": 0,
                "FSOP": 0.0,
                # ACT para quadro
                "ACT_INTERFACE_OP": 0,
                "ACT_FS_OP": 0.0,
                "ACT_MOD_MAIS": 0,
                "ACT_MOI": 0,
                # splits de efetivo / temporário
                "EFETIVO_MOD_MAIS": 0,
                "EFETIVO_MOD_MENOS": 0,
                "TEMP_MOD": 0,
            }

        head, dados = linhas[0], linhas[1:]

        # localizar colunas PRINCIPAIS (obrigatórias)
        i_sit   = idx_por_nome_at(head, "Descrição (Situação)", "Descrição", "Situação")
        i_cargo = idx_por_nome_at(head, "Título Reduzido (Cargo)", "Título Reduzido", "Cargo")
        i_area  = idx_por_nome_at(head, "Área")
        i_fil   = idx_por_nome_at(head, "Apelido (Filial)", "Apelido", "Filial")
        i_setor = idx_por_nome_at(head, "Setor")
        i_total = idx_por_nome_at(head, "Total")
        i_mo    = idx_por_nome_at(head, "Mão de Obra")
        
        for k, v in {
            "Descrição (Situação)": i_sit,
            "Título Reduzido (Cargo)": i_cargo,
            "Área": i_area,
            "Apelido (Filial)": i_fil,
            "Mão de Obra": i_mo
        }.items():
            if v is None:
                raise ValueError(f"[QHC] Coluna '{k}' não encontrada em '{aba_ativos}'.")

        # Tipo de Contrato = OPCIONAL
        i_tipo = idx_por_nome_at(
            head,
            "Tipo de Contrato",
            "Tipo de Contrato ",
            "TIPO DE CONTRATO",
            "Tipo contrato",
        )

        def n(x):
            try:
                return normaliza_at(x or "")
            except Exception:
                return (str(x or "")).strip().upper()

        # IHC = assistentes de depósito (variações comuns)
        cargos_ihc = {
            "ASSIS. DEPOSITO",
            "ASSIST. DEPOSITO",
            "ASSISTENTE DEPOSITO",
            "ASSISTENTE DE DEPÓSITO",
        }
        cargos_ihc_n = {n(c) for c in cargos_ihc}

        # Filiais válidas
        filiais_ok_n = {
            n("CD 2103 | FISIA HUB"),
            n("CD 2103 | FISIA"),
        }
        
        filiais_ok_n_2 = {
            n("CD 2103 | FISIA HUB"),
            n("CD 2103 | FISIA"),
        }

        filiais_lfp = {
            n("CD 2103 | FISIA"),
            n("CD 1080 | LOUVEIRA"),
            n("CS 7010 | PINHEIROS"),
        }
        
        # contadores
        ihc_cnt = 0          # quantidade de IHC
        ihc_jarinu = 0       # quantidade de IHC Jarinu
        ihc_transp = 0       # quantidade de IHC Transporte
        moi_transp = 0       # quantidade de MOI Transporte
        modp_jarinu = 0      # quantidade de MOD+ Jarinu
        modp_transp = 0      # quantidade de MOD+ Transporte
        moi_lp = 0
        moi_jar = 0          # quantidade de MOI Jarinu
        modp_cnt = 0         # quantidade de MOD+
        fs = 0.0             # soma de 'Total' dos IHC (FSOP)
        act_moi = 0          # ACT MOI (demais cargos)

        # novos contadores para o quadro headcount
        efet_mod_mais = 0    # EFETIVO MOD+
        efet_mod_menos = 0   # EFETIVO MOD-
        temp_mod = 0         # TEMPORÁRIO MOD (todos MOD)
        
        for row in dados:
            sit   = row[i_sit]   if i_sit   is not None and i_sit   < len(row) else ""
            cargo = row[i_cargo] if i_cargo is not None and i_cargo < len(row) else ""
            area  = row[i_area]  if i_area  is not None and i_area  < len(row) else ""
            fil   = row[i_fil]   if i_fil   is not None and i_fil   < len(row) else ""
            setor = row[i_setor] if i_setor is not None and i_setor < len(row) else ""
            tipo  = row[i_tipo]  if i_tipo is not None and i_tipo   < len(row) else ""
            mo    = row[i_mo]    if i_mo   is not None and i_mo     < len(row) else ""

            # filtros básicos
            if n(sit) != "TRABALHANDO":
                continue
            if n(area) != "WAREHOUSE":
                continue

            # coluna 'Total' pode não existir; se não existir, FSOP fica 0
            total_str = row[i_total] if (i_total is not None and i_total < len(row)) else "0"
            total_str = (total_str or "").replace(",", ".")
            
            if total_str == "":
                total_str = "0.0"
            try:
                total = float(total_str)
            except ValueError:
                total = 0.0

            cargo_n = n(cargo)
            tipo_n  = n(tipo) if i_tipo is not None else ""

            # --- flags de classificação ---
            is_ihc  = cargo_n in cargos_ihc_n

            # --- MOI (demais cargos ativos em WAREHOUSE 2103 que não são IHC nem MOD+) ---
            if (not is_ihc) and ((mo == "MOI") or (mo == "MOI GESTÃO")) and cargo != "JOVEM APRENDIZ - ADMINISTRATIVO":
                if n(fil) == "CD 2103 | FISIA":
                    act_moi += 1
                if n(fil) == "CD 1082 | JARINU":
                    moi_jar += 1
                if n(fil) == "CD 1080 | LOUVEIRA" or n(fil) == "CS 7010 | PINHEIROS":
                    moi_lp += 1
                if n(setor) == "ROTA SP":
                    moi_transp += 1

            if is_ihc:
                if n(fil) == "CD 2103 | FISIA":
                    ihc_cnt += 1            
                    fs += total
                if n(fil) == "CD 1082 | JARINU":
                    ihc_jarinu += 1
                if n(setor) == "ROTA SP":
                    ihc_transp += 1
            
            if mo == "MOD":
                if n(fil) == "CD 1082 | JARINU":
                    modp_jarinu += 1
                if n(fil) == "CD 2103 | FISIA":
                    modp_cnt += 1
                if n(setor) == "ROTA SP":
                    modp_transp += 1
                
            # --- Classificação MOD+ / MOD- por tipo de contrato (se a coluna existir) ---
            if i_tipo is not None  and cargo != "CD 1082 | JARINU" and n(fil) in filiais_ok_n_2:
                # EFETIVO
                if tipo_n == "EFETIVO":
                    if mo == "MOD ASS. DEP.":
                        efet_mod_menos += 1
                    elif mo == "MOD":
                        efet_mod_mais += 1

                # TEMPORÁRIO
                if tipo_n in {"TEMPORARIO", "TEMPORÁRIO"}  and cargo != "CD 1082 | JARINU" and n(fil) in filiais_ok_n_2:
                    if mo == "MOD ASS. DEP.":
                        temp_mod += 1

        # === 2) Localiza e garante colunas no Quadro FY ===
        mae = gc.open_by_url(PLANILHA_MAE_URL)
        ws_fy = mae.worksheet("Quadro FY - V2")
        header = ws_fy.row_values(1)

        i_act  = idx_por_nome_at(header, "ACT - Interface OP")
        i_act_modp = idx_por_nome_at(header, "ACT MOD+")
        i_act_moi = idx_por_nome_at(header, "ACT MOI")
        i_act_jar = idx_por_nome_at(header, "ACT OP JARINU")
        i_act_transp = idx_por_nome_at(header, "ACT OP ROTA SP") 
        i_modp_jar = idx_por_nome_at(header, "ACT MOD+ JARINU")
        i_moi_jar = idx_por_nome_at(header, "ACT MOI JARINU")
        i_moi_lp = idx_por_nome_at(header, "ACT MOI LOUVEIRA/PINHEIRO")
        i_moi_transp = idx_por_nome_at(header, "ACT MOI ROTA SP")
        i_modp_transp = idx_por_nome_at(header, "ACT MOD+ ROTA SP")
        i_efe_modp = idx_por_nome_at(header, "EFETIVO MOD+")
        i_efe_mod = idx_por_nome_at(header, "EFETIVO MOD-")
        i_temp_mod = idx_por_nome_at(header, "TEMPORÁRIO MOD")
        
        if i_act is None:
            raise ValueError("[Quadro FY] Coluna 'ACT (Interface)' não encontrada.")
        
        col_act_a1    = col_a1_from_idx_zero_based(i_act)
        col_act_modp_a1= col_a1_from_idx_zero_based(i_act_modp)
        col_moi_a1    = col_a1_from_idx_zero_based(i_act_moi)
        col_act_jar_a1 = col_a1_from_idx_zero_based(i_act_jar)
        col_modp_jar_a1 = col_a1_from_idx_zero_based(i_modp_jar)
        col_moi_jar_a1 = col_a1_from_idx_zero_based(i_moi_jar)
        col_moi_lp_a1 = col_a1_from_idx_zero_based(i_moi_lp) 
        col_act_transp_a1 = col_a1_from_idx_zero_based(i_act_transp)
        col_modp_transp_a1 = col_a1_from_idx_zero_based(i_modp_transp)
        col_moi_transp_a1 = col_a1_from_idx_zero_based(i_moi_transp)
        col_efe_modp_a1   = col_a1_from_idx_zero_based(i_efe_modp)
        col_efe_mod_a1   = col_a1_from_idx_zero_based(i_efe_mod)
        col_temp_mod_a1   = col_a1_from_idx_zero_based(i_temp_mod)
        
        # === 3) Acha a linha do dia (coluna 'Dia' inicia com dd/mm) ===
        
        colA = ws_fy.col_values(2)
        alvo = f"{dd}/{mm}"
        row_idx = None
        
        for i, v in enumerate(colA[1:], start=2):
            if str(v).strip().startswith(alvo):
                row_idx = i
                break
        if row_idx is None:
            raise ValueError(f"[Quadro FY] Linha do dia iniciando com '{alvo}' não encontrada.")

        # === 4) Atualiza ACT de hoje ===
        cel_act = f"{col_act_a1}{row_idx}"
        cel_act_modp = f"{col_act_modp_a1}{row_idx}"
        cel_act_moi = f"{col_moi_a1}{row_idx}"
        cel_act_jar = f"{col_act_jar_a1}{row_idx}"
        cel_modp_jar = f"{col_modp_jar_a1}{row_idx}"
        cel_moi_jar = f"{col_moi_jar_a1}{row_idx}"
        cel_act_transp = f"{col_act_transp_a1}{row_idx}"
        cel_modp_transp = f"{col_modp_transp_a1}{row_idx}"
        cel_moi_transp = f"{col_moi_transp_a1}{row_idx}"
        cel_moi_lp = f"{col_moi_lp_a1}{row_idx}"
        cel_efe_modp = f"{col_efe_modp_a1}{row_idx}"
        cel_efe_mod = f"{col_efe_mod_a1}{row_idx}"
        cel_temp_mod = f"{col_temp_mod_a1}{row_idx}"

        ws_fy.update_acell(cel_act, ihc_cnt)
        ws_fy.update_acell(cel_act_modp, modp_cnt)
        ws_fy.update_acell(cel_act_moi,act_moi)
        ws_fy.update_acell(cel_act_jar, ihc_jarinu)
        ws_fy.update_acell(cel_modp_jar, modp_jarinu)
        ws_fy.update_acell(cel_moi_jar, moi_jar)
        ws_fy.update_acell(cel_act_transp, ihc_transp)
        ws_fy.update_acell(cel_modp_transp, modp_transp)
        ws_fy.update_acell(cel_moi_transp, moi_transp)
        ws_fy.update_acell(cel_moi_lp, moi_lp)
        ws_fy.update_acell(cel_efe_modp, efet_mod_mais)
        ws_fy.update_acell(cel_efe_mod, efet_mod_menos)
        ws_fy.update_acell(cel_temp_mod, temp_mod)
        
        print(f"[Quadro FY] (HOJE) Atualizado {cel_act} com {ihc_cnt}.")

        return {
            # chaves antigas
            "IHC": int(ihc_cnt),
            "MODP": int(modp_cnt),
            "FSOP": round(fs, 4),

            # chaves no formato ACT para quadro
            "ACT_INTERFACE_OP": int(ihc_cnt),      # ACT - Interface OP
            "ACT_FS_OP": round(fs, 4),            # ACT F.S. OP
            "ACT_MOD_MAIS": int(modp_cnt),        # ACT MOD+
            "ACT_MOI": int(act_moi),              # ACT MOI

            # splits para quadro headcount
            "EFETIVO_MOD_MAIS": int(efet_mod_mais),
            "EFETIVO_MOD_MENOS": int(efet_mod_menos),
            "TEMP_MOD": int(temp_mod),            # TEMPORÁRIO MOD
        }

    except Exception as e:
        print(f"[QHC] Falha ao obter contagens no QHC: {e}")
        return None
# ============================ FATOR SALARIAL ===================================
def to_float(v):
    """
    Converte valores do Sheets tipo:
    "46,00" -> 46.0
    "" -> 0.0
    """
    try:
        s = str(v).strip()
        if not s:
            return 0.0
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        return 0.0

def achar_coluna(headers: list[str], nome: str) -> int | None:
    """
    Retorna índice 1-based da coluna que contém exatamente o header nome.
    """
    nome_norm = (nome or "").strip().upper()
    for idx, h in enumerate(headers, start=1):
        if (h or "").strip().upper() == nome_norm:
            return idx
    return None


# ======================================
# 1) LER FATOR SALARIAL (MOD e MOD+)
# ======================================

PLANILHA_FATOR_SALARIAL_URL = "https://docs.google.com/spreadsheets/d/1v30qbfn5gBkqk1chcUloD024yVVd5Vv0lwzrrTarCgA/edit"
ABA_FATOR_SALARIAL = "Tabela dinâmica 1"

def ler_fatores_salarial():
    creds = _obter_creds()
    gc = gspread.authorize(creds)

    sh = gc.open_by_url(PLANILHA_FATOR_SALARIAL_URL)
    ws = sh.worksheet(ABA_FATOR_SALARIAL)

    # MOD (tabela B->G)
    mod_total = to_float(ws.acell("G13").value)
    mod_final = mod_total

    # MOD+ (tabela I->N)
    modp_total = to_float(ws.acell("N16").value)
    modp_final = modp_total
    
    mod_jarinu = to_float(ws.acell("G15").value)
    modp_jarinu = to_float(ws.acell("N19").value)

    return mod_final, modp_final, mod_jarinu, modp_jarinu


def atualizar_base_mae_fator_salarial(data_str: str | None = None):
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz)

    if not data_str:
        data_dt = hoje
    else:
        d, m, a = data_str.split("/")
        data_dt = datetime(int(a), int(m), int(d), tzinfo=tz)

    ddmm = data_dt.strftime("%d/%m")

    # lê os fatores
    fs_mod, fs_modp, fs_jarinu_m, fs_jarinu_p = ler_fatores_salarial()
    print(f"[HeadCount] Fator Salarial MOD={fs_mod:.2f} | MOD+={fs_modp:.2f}")

    creds = _obter_creds()
    gc = gspread.authorize(creds)

    sh = gc.open_by_url(PLANILHA_MAE_URL)
    ws = sh.worksheet(ABA_TO)

    valores = ws.get_all_values()
    if not valores or len(valores) < 2:
        print("[-] Base Mãe sem dados suficientes.")
        return

    headers = valores[0]

    col_dia = achar_coluna(headers, "Dia")
    col_act_fs_mod = achar_coluna(headers, "ACT F.S. MOD")
    col_act_fs_modp = achar_coluna(headers, "ACT F.S. MOD+")
    col_act_fs_mod_jarinu = achar_coluna(headers, "ACT F.S. JARINU MOD")
    col_act_fs_modp_jarinu = achar_coluna(headers, "ACT F.S. JARINU MOD+")

    if not col_dia or not col_act_fs_mod or not col_act_fs_modp:
        print("[-] Não encontrei as colunas necessárias na Base Mãe.")
        print(f"    Dia={col_dia} | ACT F.S. MOD={col_act_fs_mod} | ACT F.S. MOD+={col_act_fs_modp}")
        return

    linha_alvo = None
    for i in range(1, len(valores)):  # começa na linha 2
        dia_cell = (valores[i][col_dia - 1] or "").strip()
        if dia_cell.startswith(ddmm):
            linha_alvo = i + 1
            break

    if not linha_alvo:
        print(f"[-] Não encontrei a data {ddmm} na coluna Dia.")
        return

    ws.update_cell(linha_alvo, col_act_fs_mod, round(fs_mod, 2))
    ws.update_cell(linha_alvo, col_act_fs_modp, round(fs_modp, 2))
    ws.update_cell(linha_alvo, col_act_fs_mod_jarinu, round(fs_jarinu_m, 2))
    ws.update_cell(linha_alvo, col_act_fs_modp_jarinu, round(fs_jarinu_p, 2))
    
    print(f"[✓] Base Mãe atualizada para {ddmm} (linha {linha_alvo})")

# ============================= PRESENÇA POR TURNO ===============================
def obter_gc():
    try:
        data = CREDENCIAL.strip()
        info = json.loads(data)

        creds = Credentials.from_service_account_info(
            info,
            scopes=DEPENDENCIAS
        )

        return gspread.authorize(creds)

    except Exception as e:
        print("Credencial não obtida")
        print(e)
        return None

def atualizar_presentes(data_str: str | None = None):
    gc = obter_gc()
    if gc is None:
        raise RuntimeError("Falha ao autenticar no Google Sheets.")
    data_ref = data_str

    print(f"Processando data: {data_ref}")

    # -------- ABS FY 2026 --------
    abs_sh = gc.open_by_url(PLANILHA_ABS_URL)
    abs_ws = abs_sh.worksheet("ABS")

    abs_df = pd.DataFrame(abs_ws.get_all_records())

    abs_df["DATA"] = pd.to_datetime(abs_df["DATA"], dayfirst=True, errors="coerce")
    data_dt = pd.to_datetime(data_ref, dayfirst=True)

    # Normaliza cargo
    abs_df["CARGO_NORM"] = abs_df["CARGO"].astype(str).str.upper().str.strip()

    # ---- MOD (Assist. Depósito) ----
    df_mod = abs_df[
        (abs_df["DATA"] == data_dt) &
        (abs_df["SIGLA"].astype(str).str.upper().str.strip() == "P") &
        (abs_df["CARGO_NORM"] == "ASSIST. DEPOSITO")
    ]

    cont_mod = (
        df_mod.groupby("TURNO")["MATRÍCULA"]
        .nunique()
        .to_dict()
    )

    mod_p1 = cont_mod.get("1° TURNO", 0)
    mod_p2 = cont_mod.get("2° TURNO", 0)
    mod_p3 = cont_mod.get("3° TURNO", 0)
    mod_p4 = cont_mod.get("4° TURNO", 0)

    # ---- MOD+ (cargos do set MOD_M) ----
    df_mod_plus = abs_df[
        (abs_df["DATA"] == data_dt) &
        (abs_df["SIGLA"].astype(str).str.upper().str.strip() == "P") &
        (abs_df["CARGO_NORM"].isin(MOD_M))
    ]

    cont_mod_plus = (
        df_mod_plus.groupby("TURNO")["MATRÍCULA"]
        .nunique()
        .to_dict()
    )

    modp_p1 = cont_mod_plus.get("1° TURNO", 0)
    modp_p2 = cont_mod_plus.get("2° TURNO", 0)
    modp_p3 = cont_mod_plus.get("3° TURNO", 0)
    modp_p4 = cont_mod_plus.get("4° TURNO", 0)

    print("MOD (Assist Depósito):", mod_p1, mod_p2, mod_p3, mod_p4)
    print("MOD+:", modp_p1, modp_p2, modp_p3, modp_p4)


    # -------- BASE MÃE 2026 --------
    base_sh = gc.open_by_url(PLANILHA_MAE_URL)
    base_ws = base_sh.worksheet("Quadro FY - V2")

    base_df = pd.DataFrame(base_ws.get_all_records())

    # Coluna "Dia" está no formato: 02/01 - sex.
    base_df["DATA_BASE"] = (
        base_df["Dia"]
        .str.extract(r"(\d{2}/\d{2})")[0]
        + f"/{data_dt.year}"
    )

    linha = base_df.index[
        pd.to_datetime(base_df["DATA_BASE"], dayfirst=True) == data_dt
    ]

    if len(linha) == 0:
        raise RuntimeError("Data não encontrada na Base Mãe.")

    row = linha[0] + 2  # +2 por causa do cabeçalho

    base_ws.update(
        range_name=f"AP{row}:AS{row}",
        values=[[modp_p1, modp_p2, modp_p3, modp_p4]]
    )

    base_ws.update(
        range_name=f"AU{row}:AX{row}",
        values=[[mod_p1, mod_p2, mod_p3, mod_p4]]
    )
    print("Base Mãe atualizada com sucesso!")
# ============================= ABS DETALHES =====================================
def carregar_presenca(gc, aba_mes: str) -> pd.DataFrame:
    """
    Carrega TODOS os colaboradores ativos do mês
    usando a coluna COLABORADOR como fonte oficial do nome.
    """
    sh = gc.open_by_url(PLANILHA_PRE_URL)
    ws = sh.worksheet(aba_mes.upper())

    linhas = ws.get_all_values()
    if len(linhas) < 2:
        raise RuntimeError(f"Aba {aba_mes} da Presença está vazia.")

    cab, dados = linhas[0], linhas[1:]
    df = pd.DataFrame(dados, columns=cab)

    if "COLABORADOR" not in df.columns:
        raise RuntimeError("Coluna 'COLABORADOR' não encontrada na Presença.")

    df["NOME"] = (
        df["COLABORADOR"]
        .astype(str)
        .str.strip()
    )

    df["__NOME_KEY"] = df["NOME"].str.upper()

    df = df[df["NOME"] != ""]
    df = df.drop_duplicates(subset="__NOME_KEY")

    return df[["NOME", "__NOME_KEY"]]

# ================= ABS =================

def carregar_abs(gc, ano_ref: int) -> pd.DataFrame:
    sh = gc.open_by_url(PLANILHA_ABS_URL)
    ws = sh.worksheet("ABS")

    linhas = ws.get_all_values()
    cab, dados = linhas[0], linhas[1:]
    df = pd.DataFrame(dados, columns=cab)

    df["DATA_DT"] = pd.to_datetime(
        df["DATA"],
        dayfirst=True,
        errors="coerce"
    )

    # remove linhas sem data válida
    df = df[df["DATA_DT"].notna()]

    # agora sim, filtra por ano
    df = df[df["DATA_DT"].dt.year == ano_ref]


    df["__NOME_KEY"] = (
        df["NOME"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["SIGLA"] = df["SIGLA"].astype(str).str.strip().str.upper()

    return df
def maior_sequencia_faltas(valores):
    max_seq = 0
    atual = 0

    for v in valores:
        if v in ("F", "AT"):
            atual += 1
            max_seq = max(max_seq, atual)
        else:
            atual = 0

    return max_seq


def sequencia_atual_faltas(valores):
    atual = 0
    for v in reversed(valores):
        if v in ("F", "AT"):
            atual += 1
        else:
            break
    return atual

# ================= ANÁLISE =================

def carregar_status_qhc(gc) -> pd.DataFrame:
    """
    Carrega Status, Data Admissão, Turno e Agência
    a partir do QHC (Ativos Dezembro 2025),
    escolhendo a melhor linha por colaborador.
    """
    sh = gc.open_by_url(PLANILHA_QHC_URL)

    try:
        ws = sh.worksheet("H.C. TT")
    except gspread.WorksheetNotFound:
        raise RuntimeError("Aba 'Ativos Dezembro 2025' não encontrada no QHC.")

    linhas = ws.get_all_values()
    cab, dados = linhas[0], linhas[1:]
    df = pd.DataFrame(dados, columns=cab)

    # ================= NORMALIZA COLUNAS =================
    def find_col(chaves):
        for c in df.columns:
            u = c.upper()
            if any(k in u for k in chaves):
                return c
        return None

    col_nome   = find_col(["NOME"])
    col_status = find_col(["SITUA"])
    col_adm    = find_col(["DATA ADMISS"])
    col_turno  = find_col(["TURNO", "ESCALA"])
    col_ag     = find_col(["FILIAL", "AGÊNCIA", "APELIDO"])

    if not col_nome or not col_status:
        raise RuntimeError("Colunas essenciais não encontradas no QHC.")

    # ================= LIMPEZA =================
    df["__NOME_KEY"] = (
        df[col_nome]
        .astype(str)
        .str.replace(r"\(.*?\)", "", regex=True)  # remove apelidos ( KEVIN )
        .str.strip()
        .str.upper()
    )

    df["Status_QHC"] = df[col_status].astype(str).str.strip()
    df["Data Admissão"] = df[col_adm] if col_adm else ""
    df["Turno"] = df[col_turno] if col_turno else ""
    df["Agência"] = df[col_ag].astype(str).str.strip() if col_ag else ""
    df.loc[df["Agência"] == "2103", "Agência"] = "FISIA"

    df = df[df["__NOME_KEY"] != ""]

    # ================= ESCOLHA DA MELHOR LINHA =================
    def escolher_linha(grupo):
        statuses = grupo["Status_QHC"].str.upper()

        if (statuses == "TRABALHANDO").any():
            return grupo[statuses == "TRABALHANDO"].iloc[0]

        if (statuses == "AFASTADO").any():
            return grupo[statuses == "AFASTADO"].iloc[0]

        if (statuses == "TRANSFERIDO").any():
            return grupo[statuses == "TRANSFERIDO"].iloc[0]

        return None

    registros = []
    for _, g in df.groupby("__NOME_KEY"):
        linha = escolher_linha(g)
        if linha is not None:
            registros.append(linha)

    df_final = pd.DataFrame(registros)

    return df_final[
        [
            "__NOME_KEY",
            "Status_QHC",
            "Data Admissão",
            "Turno",
            "Agência",
        ]
    ]


def gerar_abs_analise(gc, data_ref: str):

    # ================= DATA DE REFERÊNCIA =================
    dt_ref = pd.to_datetime(data_ref, dayfirst=True, errors="raise")

    ano_ref = dt_ref.year
    mes_num = dt_ref.month

    MAPA_MESES_INV = {
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
        12: "DEZEMBRO",
    }

    mes_presenca = MAPA_MESES_INV[mes_num]

    print("🔹 Carregando Presença...")
    df_presenca = carregar_presenca(gc, mes_presenca)

    print("🔹 Carregando ABS...")
    df_abs = carregar_abs(gc, ano_ref)

    print("🔹 Carregando Status do QHC...")
    df_qhc_status = carregar_status_qhc(gc)

    df_out = df_presenca.copy()

    # ================= DATAS DO ANO =================
    
    datas = pd.date_range(
        start=date(ano_ref, 1, 1),
        end=date(ano_ref, 12, 31),
        freq="D"
    )

    # ================= HISTÓRICO DIÁRIO =================
    hist = (
        df_abs
        .pivot_table(
            index="__NOME_KEY",
            columns="DATA_DT",
            values="SIGLA",
            aggfunc="last"
        )
        .reindex(columns=datas, fill_value="")
    )
    hist.columns = [d.strftime("%d/%m/%Y") for d in datas]

    # ================= MÉTRICAS =================
    abs_sum = (
        df_abs[df_abs["SIGLA"].isin(["F", "AT"])]
        .groupby("__NOME_KEY")
        .size()
    )

    df_out["ABS"] = (
        df_out["__NOME_KEY"]
        .map(abs_sum)
        .fillna(0)
        .astype(int)
    )

    total_dias = (
        df_abs
        .groupby("__NOME_KEY")
        .size()
    )

    # ================= JUNTA HISTÓRICO =================
    df_out = (
        df_out
        .set_index("__NOME_KEY")
        .join(hist)
        .reset_index()
    )

    # ================= JUNTA STATUS DO QHC =================
    df_out = df_out.merge(
        df_qhc_status,
        on="__NOME_KEY",
        how="left"
    )

    df_out["Status"] = df_out["Status_QHC"].fillna("")
    df_out = df_out.drop(columns=["Status_QHC"])

    # ================= STATUS GERAL / ATUAL =================

    col_datas = [
        c for c in df_out.columns
        if c not in [
            "NOME", "Status", "Data Admissão", "Turno", "Agência",
            "Tempo de Casa", "ABS", "STATUS GERAL", "STATUS ATUAL", "% ABS"
        ]
    ]

    df_out["STATUS GERAL"] = df_out[col_datas].apply(
        lambda r: maior_sequencia_faltas(r.values),
        axis=1
    )

    def status_atual_linha(r):
        # remove dias futuros vazios
        valores_validos = [v for v in r.values if v != ""]
        return sequencia_atual_faltas(valores_validos)

    df_out["STATUS ATUAL"] = df_out[col_datas].apply(
        status_atual_linha,
        axis=1
    )

    # ================= TEMPO DE CASA =================
    hoje = date.today()

    # coluna técnica (NÃO vai para o output)
    df_out["_DATA_ADM_DT"] = pd.to_datetime(
        df_out["Data Admissão"],
        dayfirst=True,
        errors="coerce"
    )

    df_out["Tempo de Casa"] = (
        hoje - df_out["_DATA_ADM_DT"].dt.date
    ).apply(lambda x: x.days if pd.notna(x) else np.nan)


    # formatação final para exibição
    df_out["Data Admissão"] = (
        df_out["_DATA_ADM_DT"]
        .dt.strftime("%d/%m/%Y")
        .fillna("")
    )

    # remove coluna técnica
    df_out = df_out.drop(columns=["_DATA_ADM_DT"])

    df_out["% ABS"] = (
        (df_out["ABS"]
        / df_out["Tempo de Casa"])
        # * 100
    ).fillna(0).round(4)

    df_out["ABS"] = (
        df_out["__NOME_KEY"]
        .map(abs_sum)
        .fillna(0)
        .astype(int)
    )
    # ================= ORGANIZAÇÃO FINAL DAS COLUNAS =================

    # Remove chave técnica
    df_out = df_out.drop(columns=["__NOME_KEY"])

    colunas_fixas = [
        "NOME",
        "Status",
        "Data Admissão",
        "Turno",
        "Agência",
        "Tempo de Casa",
        "ABS",
        "STATUS GERAL",
        "STATUS ATUAL",
        "% ABS",
    ]

    # Garante existência das colunas futuras
    for c in colunas_fixas:
        if c not in df_out.columns:
            df_out[c] = ""

    colunas_datas = [
        c for c in df_out.columns
        if c not in colunas_fixas
    ]

    df_out = df_out[colunas_fixas + colunas_datas]

    # ================= OUTPUT =================
    sh = gc.open_by_url(PLANILHA_ABS_URL)
    try:
        ws = sh.worksheet("ABS Análise")
    except:
        ws = sh.add_worksheet(
            "ABS Análise",
            rows=2000,
            cols=len(df_out.columns)
        )

    ws.clear()
    set_with_dataframe(ws, df_out)

    print(f"ABS Análise gerada com {len(df_out)} colaboradores.")

# ============================= IHC DETALHES =====================================
PLANILHA_DESTINO_URL = "https://docs.google.com/spreadsheets/d/1M44kE_8flXkl450ubcEp8cuVvwI4M5DcQYT0Q-a2TOo/edit"

def gerar_ihc_detalhes():
    """
    Acessa o QHC 2025, aplica filtros e cria a aba 'IHC Detalhes' na planilha de destino
    """
    # Autenticação
    creds = _obter_creds()
    gc = gspread.authorize(creds)
    
    # Data e mês atual
    tz = ZoneInfo("America/Sao_Paulo")
    hoje = datetime.now(tz)
    data_str = hoje.strftime("%d/%m/%Y")
    mes_atual = nome_mes_pt(hoje)
    ano_atual = hoje.year
    
    # Nome da aba de ativos
    #aba_ativos = f"Ativos {mes_atual} {ano_atual}"
    aba_ativos = "H.C. TT" 

    print(f"[+] Acessando planilha QHC 2025, aba: {aba_ativos}")
    
    # Abre a planilha de origem (QHC 2025)
    qhc_sh = gc.open_by_url(PLANILHA_QHC_URL)
    ws_ativos = qhc_sh.worksheet(aba_ativos)
    
    # Lê todos os dados
    linhas = ws_ativos.get_all_values()
    if not linhas or len(linhas) < 2:
        print("[-] Aba vazia ou sem dados")
        return
    
    head = linhas[0]
    dados = linhas[1:]
    
    # Encontra índices das colunas necessárias
    i_apelido = idx_por_nome(head, "Apelido", "Filial") or idx_por_nome(head, "Apelido")
    i_area = idx_por_nome(head, "Área") or idx_por_nome(head, "Area")
    i_situacao = idx_por_nome(head, "Descrição", "Situação") or idx_por_nome(head, "Situação")
    i_cidade = idx_por_nome(head, "Cidade")
    i_filial = idx_por_nome(head, "Filial")
    i_mao_obra = idx_por_nome(head, "Mão de Obra") or idx_por_nome(head, "Mao de Obra")
    i_escala = idx_por_nome(head, "Descrição", "Escala") or idx_por_nome(head, "Escala")
    i_cargo = idx_por_nome(head, "Título Reduzido (Cargo)") or idx_por_nome(head, "Titulo Reduzido (Cargo)")
    
    # Verifica se todas as colunas foram encontradas
    if None in [i_apelido, i_area, i_situacao, i_cidade, i_mao_obra, i_escala]:
        print("[-] Erro: Algumas colunas não foram encontradas")
        print(f"Apelido: {i_apelido}, Área: {i_area}, Situação: {i_situacao}")
        print(f"Cidade: {i_cidade}, Mão de Obra: {i_mao_obra}, Escala: {i_escala}")
        return
    
    print("[+] Aplicando filtros...")
    
    # Aplica filtros
    filiais_alvo = {"CD 2103 | FISIA", "CD 2103 | FISIA HUB"}
    filiais_alvo_2 = {"CD 2103 | FISIA", "CD 2103 | FISIA HUB"}
    dados_filtrados = []
    dados_filtrados_2 = []
    
    for row in dados:
        if len(row) <= max(i_apelido, i_area, i_situacao):
            continue
        
        apelido = (row[i_apelido] if i_apelido < len(row) else "").strip()
        area = (row[i_area] if i_area < len(row) else "").strip()
        situacao = (row[i_situacao] if i_situacao < len(row) else "").strip()
        cargo = normaliza(row[i_cargo] if i_cargo < len(row) else "")
        # Filtros: Apelido, Área e Situação
        if apelido in filiais_alvo and \
           normaliza(area) == "WAREHOUSE" and \
           normaliza(situacao) == "TRABALHANDO" and \
           cargo != "JOVEM APRENDIZ - ADMINISTRATIVO":
           dados_filtrados.append(row)

    print(f"[+] Total de registros após filtros: {len(dados_filtrados)}")

    for row in dados:
        if len(row) <= max(i_apelido, i_area, i_situacao):
            continue
        
        apelido = (row[i_apelido] if i_apelido < len(row) else "").strip()
        area = (row[i_area] if i_area < len(row) else "").strip()
        situacao = (row[i_situacao] if i_situacao < len(row) else "").strip()
        cargo = normaliza(row[i_cargo] if i_cargo < len(row) else "")
        if apelido in filiais_alvo_2 and \
           normaliza(area) == "WAREHOUSE" and \
           normaliza(situacao) == "TRABALHANDO" and \
           cargo != "JOVEM APRENDIZ - ADMINISTRATIVO":
           dados_filtrados_2.append(row) 

    print(f"[+] Total de registros após filtros: {len(dados_filtrados_2)}")

     
    # Cria DataFrame para análise
    df = pd.DataFrame(dados_filtrados, columns=head)
    df_2 = pd.DataFrame(dados_filtrados_2, columns=head)
    
    # Contadores
    ihc_total = len(df)
    
    # Cidades
    cidades_map = {
        "BRAGANÇA PAULISTA": 0, "VARGEM": 0, "EXTREMA": 0, "ITAPEVA": 0,
        "JOANOPOLIS": 0, "CAMANDUCAIA": 0, "PIRACAIA": 0, "PINHALZINHO": 0,
        "GUARULHOS": 0, "ATIBAIA": 0, "OUTRAS CIDADES": 0
    }
    
    for _, row in df_2.iterrows():
        apelido = (row[i_apelido] if i_apelido < len(row) else "").strip()
        mao = normaliza(row.iloc[i_mao_obra] if i_mao_obra < len(row) else "")
        cidade = normaliza(row.iloc[i_cidade] if i_cidade < len(row) else "")
        cargo = normaliza(row.iloc[i_cargo] if i_cargo < len(row) else "")
        encontrou = False
        if "MOI" in mao:
            for c in cidades_map.keys():
                
                    if c != "OUTRAS CIDADES" and normaliza(c) in cidade:
                        cidades_map[c] += 1
                        encontrou = True
                        break
            if not encontrou:
                cidades_map["OUTRAS CIDADES"] += 1
        else:
            if apelido != "CD 1082 | JARINU":
                for c in cidades_map.keys():
                
                    if c != "OUTRAS CIDADES" and normaliza(c) in cidade:
                        cidades_map[c] += 1
                        encontrou = True
                        break
                if not encontrou:
                    cidades_map["OUTRAS CIDADES"] += 1

    
    # Agências (da coluna Filial)
    agencias_map = {
        "ADECCO": 0, "DPX": 0, "FISIA": 0, "FENIX": 0, "SERTEC": 0, "MENDES": 0, "OUTRAS AGENCIAS": 0
    }
    
    for _, row in df_2.iterrows():
        apelido = (row[i_apelido] if i_apelido < len(row) else "").strip()
        mao = normaliza(row.iloc[i_mao_obra] if i_mao_obra < len(row) else "")
        filial = normaliza(row.iloc[i_filial] if i_filial < len(row) else "")
        if "MOI" in mao:
            # Verifica se contém "2103" e trata como FISIA
            if "2103" in filial:
                agencias_map["FISIA"] += 1
            else:
                # Verifica se a agência está na lista conhecida
                encontrou_agencia = False
                for ag in agencias_map.keys():
                    if ag != "OUTRAS AGENCIAS" and normaliza(ag) in filial:
                        agencias_map[ag] += 1
                        encontrou_agencia = True
                        break
                # Se não encontrou nenhuma agência conhecida, conta como OUTRAS AGENCIAS
                if not encontrou_agencia:
                    agencias_map["OUTRAS AGENCIAS"] += 1
        else:
            if apelido != "CD 1082 | JARINU":
                if "2103" in filial:
                    agencias_map["FISIA"] += 1
                else:
                    # Verifica se a agência está na lista conhecida
                    encontrou_agencia = False
                    for ag in agencias_map.keys():
                        if ag != "OUTRAS AGENCIAS" and normaliza(ag) in filial:
                            agencias_map[ag] += 1
                            encontrou_agencia = True
                            break
                    # Se não encontrou nenhuma agência conhecida, conta como OUTRAS AGENCIAS
                    if not encontrou_agencia:
                        agencias_map["OUTRAS AGENCIAS"] += 1
    dados_filtrados = []
    # Mão de Obra
    mod = mod_plus = moi_temp = 0
    for _, row in df.iterrows():
        mao = normaliza(row.iloc[i_mao_obra] if i_mao_obra < len(row) else "")
        cargo = normaliza(row.iloc[i_cargo] if i_cargo < len(row) else "")
        if "MOD ASS. DEP" in mao or "MOD ASS DEP" in mao:
            mod += 1
        elif mao == "MOD":
            mod_plus += 1
        if "MOI" in mao:
            if "JOVEM APRENDIZ - ADMINISTRATIVO" in cargo:
                continue
            else:
                moi_temp += 1
    dados_filtrados_2 = []
    # Mão de Obra
    moi = 0
    for _, row in df_2.iterrows():
        mao = normaliza(row.iloc[i_mao_obra] if i_mao_obra < len(row) else "")
        cargo = normaliza(row.iloc[i_cargo] if i_cargo < len(row) else "")
        if "MOI" in mao:
            if "JOVEM APRENDIZ - ADMINISTRATIVO" in cargo:
                continue
            else:
                moi += 1

    ihc_total += (moi - moi_temp)
    # Turnos
    turnos_map = {"1º": 0, "2º": 0, "3º": 0, "4º": 0, "5º": 0, "ADM": 0}
    
    for _, row in df_2.iterrows():
        apelido = (row[i_apelido] if i_apelido < len(row) else "").strip()
        mao = normaliza(row.iloc[i_mao_obra] if i_mao_obra < len(row) else "")
        escala = normaliza(row.iloc[i_escala] if i_escala < len(row) else "")
        
        if "MOI" in mao:
            if "1" in escala or "PRIMEIRO" in escala:
                turnos_map["1º"] += 1
            elif "2" in escala or "SEGUNDO" in escala:
                turnos_map["2º"] += 1
            elif "3" in escala or "TERCEIRO" in escala:
                turnos_map["3º"] += 1
            elif "4" in escala or "QUARTO" in escala:
                turnos_map["4º"] += 1
            elif "5" in escala or "QUINTO" in escala:
                turnos_map["5º"] += 1
            elif "ADM" in escala:
                turnos_map["ADM"] += 1
        else:
            if "1" in escala or "PRIMEIRO" in escala:
                turnos_map["1º"] += 1
            elif "2" in escala or "SEGUNDO" in escala:
                turnos_map["2º"] += 1
            elif "3" in escala or "TERCEIRO" in escala:
                turnos_map["3º"] += 1
            elif "4" in escala or "QUARTO" in escala:
                turnos_map["4º"] += 1
            elif "5" in escala or "QUINTO" in escala:
                turnos_map["5º"] += 1
            elif "ADM" in escala:
                turnos_map["ADM"] += 1
        
    # Monta a linha de dados
    linha_dados = [
        data_str,  # DATA
        mes_atual,  # MÊS
        ihc_total,  # IHC
        cidades_map["BRAGANÇA PAULISTA"],
        cidades_map["VARGEM"],
        cidades_map["EXTREMA"],
        cidades_map["ITAPEVA"],
        cidades_map["JOANOPOLIS"],
        cidades_map["CAMANDUCAIA"],
        cidades_map["PIRACAIA"],
        cidades_map["PINHALZINHO"],
        cidades_map["GUARULHOS"],
        cidades_map["ATIBAIA"],
        cidades_map["OUTRAS CIDADES"],
        agencias_map["ADECCO"],
        agencias_map["DPX"],
        agencias_map["FISIA"],
        agencias_map["FENIX"],
        agencias_map["SERTEC"],
        agencias_map["MENDES"],
        agencias_map["OUTRAS AGENCIAS"],
        mod,  # MOD
        mod_plus,  # MOD+
        moi,  # MOI
        turnos_map["1º"],
        turnos_map["2º"],
        turnos_map["3º"],
        turnos_map["4º"],
        turnos_map["5º"],
        turnos_map["ADM"]
    ]
    
    # Cabeçalho da aba IHC Detalhes
    cabecalho = [
        "DATA", "MÊS", "IHC", "BRAGANÇA PAULISTA", "VARGEM", "EXTREMA", "ITAPEVA",
        "JOANOPOLIS", "CAMANDUCAIA", "PIRACAIA", "PINHALZINHO", "GUARULHOS", "ATIBAIA",
        "OUTRAS CIDADES", "ADECCO", "DPX", "FISIA", "FENIX", "SERTEC", "MENDES",
        "OUTRAS AGENCIAS", "MOD", "MOD+", "MOI", "1º", "2º", "3º", "4º", "5º", "ADM"
    ]
    
    print(f"[+] Acessando planilha de destino para IHC Detalhes")
    
    # Abre a planilha de destino
    destino_sh = gc.open_by_url(PLANILHA_DESTINO_URL)
    
    # Cria ou atualiza a aba IHC Detalhes na planilha de destino
    try:
        ws_ihc = destino_sh.worksheet("IHC Detalhes")
        print("[+] Aba 'IHC Detalhes' encontrada na planilha de destino")
    except:
        ws_ihc = destino_sh.add_worksheet(title="IHC Detalhes", rows=1000, cols=31)
        ws_ihc.update([cabecalho], range_name="1:1")
        print("[+] Aba 'IHC Detalhes' criada na planilha de destino")
    
    # Verifica se já existe registro para hoje
    valores_existentes = ws_ihc.get_all_values()
    if len(valores_existentes) > 1:
        # Procura pela data
        linha_atualizar = None
        for i, row in enumerate(valores_existentes[1:], start=2):
            if row[0] == data_str:
                linha_atualizar = i
                break
        
        if linha_atualizar:
            # Atualiza linha existente
            ws_ihc.update(f"A{linha_atualizar}:AD{linha_atualizar}", [linha_dados])
            print(f"[+] Linha {linha_atualizar} atualizada com sucesso!")
        else:
            # Adiciona nova linha
            ws_ihc.append_row(linha_dados)
            print("[+] Nova linha adicionada com sucesso!")
    else:
        # Primeira inserção
        ws_ihc.update([cabecalho], range_name="1:1")
        ws_ihc.append_row(linha_dados)
        print("[+] Primeira linha adicionada com sucesso!")
    
    print(f"\n[✓] Processo concluído!")
    print(f"IHC Total: {ihc_total}")
    print(f"MOD: {mod}, MOD+: {mod_plus}, MOI: {moi}")

# ============================= FUNÇÃO  =================================
#                         ATUALIZA REFEITÓRIO
# =======================================================================


def remove_acento_r(s):
    if not isinstance(s, str):
        return ""
    return "".join(
        ch for ch in unicodedata.normalize("NFD", s)
        if unicodedata.category(ch) != "Mn"
    )

def normaliza_r(s):
    return remove_acento_r((s or "").strip()).upper()

def achar_coluna_r(headers, nome):
    alvo = normaliza_r(nome)
    for i, h in enumerate(headers):
        if normaliza_r(h) == alvo:
            return i
    return None

def achar_linha_hoje(ws_controle):
    linhas = ws_controle.get_all_values()
    header = linhas[0]

    i_data = achar_coluna_r(header, "DATA PROGRAMAÇÃO")

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")

    for idx, row in enumerate(linhas[1:], start=2):
        if row[i_data] == hoje:
            return idx

    return None

def achar_coluna_bloco(head_grupo, head_turno, nome_grupo, nome_turno):

    for i, (g, t) in enumerate(zip(head_grupo, head_turno)):

        if normaliza_r(g) == normaliza_r(nome_grupo) and normaliza_r(t) == normaliza_r(nome_turno):
            return i

    return None

def expandir_merge(header):

    novo = []
    atual = ""

    for h in header:
        if h.strip():
            atual = h
        novo.append(atual)

    return novo

def contar_headcount(gc):
    sh = gc.open_by_key(PLANILHA_HEADCOUNT_ID)
    ws = sh.worksheet(ABA_HEADCOUNT)

    linhas = ws.get_all_values()

    head, dados = linhas[0], linhas[1:]

    i_situa = achar_coluna_r(head, "Descrição (Situação)")
    i_setor = achar_coluna_r(head, "Setor")
    i_turno = achar_coluna_r(head, "Descrição (Escala)")
    i_cargo    = achar_coluna_r(head, "Título Reduzido (Cargo)")
    i_area     = achar_coluna_r(head, "Área")
    
    ihc_rota_t1 = 0
    ihc_rota_t2 = 0
    ihc_rota_t3 = 0
    ihc_jovm_t1 = 0
    ihc_jovm_t2 = 0

    def n(x): 
            try:
                return normaliza_r(x or "")
            except NameError:
                return (str(x or "")).strip().upper()
            
    for row in dados:
            cargo = row[i_cargo] if i_cargo is not None and i_cargo < len(row) else ""
            sit   = row[i_situa] if i_situa is not None and i_situa < len(row) else ""
            setor = row[i_setor] if i_setor is not None and i_setor < len(row) else ""
            turno = row[i_turno] if i_turno is not None and i_turno < len(row) else ""
            area  = row[i_area]  if i_area is not None and  i_area  < len(row) else ""

            if n(sit) != "TRABALHANDO":
               continue
            
            if n(cargo) == "JOVEM APRENDIZ - ADMINISTRATIVO" and n(area) == "WAREHOUSE":
                if turno == "1° TURNO":
                    ihc_jovm_t1 += 1
                if turno == "2° TURNO":
                    ihc_jovm_t2 += 1

            if n(setor) == "ROTA SP":
                if turno == "3° TURNO": 
                    ihc_rota_t3 += 1
                if turno == "2° TURNO": 
                    ihc_rota_t2 += 1
                if turno == "1° TURNO": 
                    ihc_rota_t1 += 1
    
    return ihc_rota_t1, ihc_rota_t2, ihc_rota_t3, ihc_jovm_t1, ihc_jovm_t2

def contar_diaristas_presentes(gc):

    sh = gc.open_by_key(PLANILHA_DIARISTAS_ID)
    ws = sh.worksheet(ABA_DIARISTAS)

    linhas = ws.get_all_values()
    head_grupo_raw = linhas[0]
    head_grupo = expandir_merge(head_grupo_raw)
    head_turno = linhas[2]
    dados = linhas[3:]
    head_dia = linhas[0]

    i_t1 = achar_coluna_bloco(head_grupo, head_turno, "Quantidade Presentes", "1T")
    i_t2 = achar_coluna_bloco(head_grupo, head_turno, "Quantidade Presentes", "2T")
    i_t3 = achar_coluna_bloco(head_grupo, head_turno, "Quantidade Presentes", "3T")
    i_data = achar_coluna_r(head_dia, "DIA")

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")
    
    for row in dados:

        turno1 = int(row[i_t1] or 0)
        turno2 = int(row[i_t2] or 0)
        turno3 = int(row[i_t3] or 0)
        data   = row[i_data] if i_data is not None and i_data < len(row) else ""
        
        if data == hoje:
            return turno1, turno2, turno3
    

def escrever_base_mae(gc, rota_t1, rota_t2, rota_t3, diar_t1, diar_t2, diar_t3, jovem_t1, jovem_t2):

    sh = gc.open_by_key(PLANILHA_BASEMAE_ID)
    ws = sh.worksheet(ABA_DESTINO)

    linhas = ws.get_all_values()
    head = linhas[0]

    linha = achar_linha_hoje(ws)

    if not linha:
        print("Data de hoje não encontrada na Base Mãe")
        return

    def col(nome):
        idx = achar_coluna_r(head, nome)
        if idx is None:
            raise Exception(f"Coluna não encontrada: {nome}")
        return idx + 1

    ws.update_cell(linha, col("ROTA SP 1T"), rota_t1)
    ws.update_cell(linha, col("ROTA SP 2T"), rota_t2)
    ws.update_cell(linha, col("ROTA SP 3T"), rota_t3)

    ws.update_cell(linha, col("DIARISTAS 1T"), diar_t1)
    ws.update_cell(linha, col("DIARISTAS 2T"), diar_t2)
    ws.update_cell(linha, col("DIARISTAS 3T"), diar_t3)

    ws.update_cell(linha, col("JOVEM APRENDIZ 1T"), jovem_t1)
    ws.update_cell(linha, col("JOVEM APRENDIZ 2T"), jovem_t2)

    print("Refeitório atualizado com sucesso")

# ============================= FUNÇÃO PRINCIPAL =================================
'''if __name__ == "__main__":

    creds = _obter_creds()
    gc = gspread.authorize(creds)

    DATA_ALVO = "01/03/2026" # Coloque uma data específica se preceisar. Exemplo: "02/02/2026"
    tz = ZoneInfo("America/Sao_Paulo")

    hoje = datetime.now(tz) if not DATA_ALVO else datetime.strptime(
        DATA_ALVO, "%d/%m/%Y"
    ).replace(tzinfo=tz)

    ontem = hoje - timedelta(days=1)

    dd = f"{hoje.day:02d}"
    mm = f"{hoje.month:02d}"
    yyyy = str(hoje.year)
    data_str = f"{dd}/{mm}/{yyyy}"

    ABA_WHS = f"Indicadores {nome_mes_pt(hoje)}"
    abs_ws = gc.open_by_url(PLANILHA_ABS_URL).worksheet(ABA_ABS)
    
    abs_para_planejamento(
            gc,
            etapa_lista_para_abs(gc, data_str, dd, mm, yyyy),
            data_str,
            buscar_ihc_base_mae(gc, data_str)
        ) # Atualiza ABS diário do dia atual e anterior
    time.sleep(5)

    atualizar_presentes(data_str) # Atualiza presentes no CD
    time.sleep(5)

    #des_para_qhc(data_str)  # Atualiza os Desligados
    #time.sleep(5)
    
    preencher_simulador(listar_processos_setores())  # Atualiza o Simulador
    time.sleep(5)

    processar_dia(gc, data_str) # Atualiza Desligados inicial
    time.sleep(5)

    atualizar_base_mae_fator_salarial(data_str) # Atualiza Fator Salarial
    time.sleep(5)

    buscar_qhc_contagens_at(data_str)  # Atualiza o Quadro FY - v2
    time.sleep(5)

    for data in [ontem, hoje]:
        dd = f"{data.day:02d}"
        mm = f"{data.month:02d}"
        yyyy = str(data.year)
        data_str = f"{dd}/{mm}/{yyyy}"

        abs_para_planejamento(
            gc,
            etapa_lista_para_abs(gc, data_str, dd, mm, yyyy),
            data_str,
            buscar_ihc_base_mae(gc, data_str)
        ) # Atualiza ABS diário do dia atual e anterior
        time.sleep(5)

        atualizar_presentes(data_str) # Atualiza presentes no CD
        time.sleep(5)

    dd = f"{hoje.day:02d}"
    mm = f"{hoje.month:02d}"
    yyyy = str(hoje.year)
    data_str = f"{dd}/{mm}/{yyyy}"

    processar_dia(gc, data_str) # Atualiza números de Turnover
    time.sleep(5)

    gerar_abs_analise(gc, data_str) # Atualiza análise de ABS
    time.sleep(5)

    gerar_ihc_detalhes() # Atualiza detalhes de IHC
    time.sleep(5)

    abs_mes(gc) # Atualiza ABS de resumo mensal
    time.sleep(5)

    to_mes(gc, data_str)

    rota_t1, rota_t2, rota_t3, jovem_t1, jovem_t2 = contar_headcount(gc)
    diar_t1, diar_t2, diar_t3 = contar_diaristas_presentes(gc)
    escrever_base_mae(gc, rota_t1, rota_t2, rota_t3, diar_t1, diar_t2, diar_t3, jovem_t1, jovem_t2) # Atualiza TO de resumo mensal'''
