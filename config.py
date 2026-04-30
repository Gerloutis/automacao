import os
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# BANCO DE DADOS
# =========================================================
DATABASE_URL = os.getenv("DATABASE_URL")
SOLICITACOES_TABLE = os.getenv("SOLICITACOES_TABLE", "solicitacoes_colaborador")
ATESTADOS_TABLE = os.getenv("ATESTADOS_TABLE", "atestados_colaborador")
MAX_ATESTADO_BYTES = int(os.getenv("MAX_ATESTADO_BYTES", str(5 * 1024 * 1024)))

# =========================================================
# SEGURANÇA
# =========================================================
SECRET_KEY = os.getenv("SECRET_KEY", "troque-isso-em-producao")

# =========================================================
# GOOGLE SHEETS
# =========================================================
PLANILHA_PRESENCA_ID = os.getenv("PLANILHA_PRESENCA_ID", "1Qv9mI_vo0yA987Kabn-bUM6XaQq2IOs4dLZKAzwU8P8")
HC_DEFAULT_SHEET_ID = os.getenv("HC_DEFAULT_SHEET_ID", "1VAuoQarh9M96VQnJt85444Asw2hWZoHlb82EmTYlnyw")
HC_DEFAULT_TAB_NAME = os.getenv("HC_DEFAULT_TAB_NAME", "H.C. TT")
HC_MAX_PANELS = 3
HC_PANEL_IDS = [f"painel_{i}" for i in range(1, HC_MAX_PANELS + 1)]

# =========================================================
# DOMÍNIO DO NEGÓCIO
# =========================================================
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
    12: "DEZEMBRO",
}

STATUS_PRESENCA = ["P", "F", "AT", "PA", "HE", "FC", "FBH", "S", "AF", "FE", "DES"]

TIPOS_SOLICITACAO = {
    "alterar_linha_ponto": {
        "label": "Alterar linha e ponto",
        "destino": "ADM",
    },
    "trocar_gestao": {
        "label": "Trocar de gestão",
        "destino": "ADM",
    },
    "solicitar_desligamento": {
        "label": "Solicitar desligamento",
        "destino": "RH",
    },
    "solicitar_efetivacao": {
        "label": "Solicitar efetivação",
        "destino": "RH",
    },
    "solicitar_promocao": {
        "label": "Solicitar promoção",
        "destino": "RH",
    },
    "adicionar_atestado": {
        "label": "Adicionar atestado",
        "destino": "ADM",
    },
}
