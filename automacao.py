import os
import json
import gspread
from google.oauth2.service_account import Credentials

def enviar_para_planilha(texto):

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credenciais_json = os.environ.get("GOOGLE_CREDENTIALS")

    credenciais_dict = json.loads(credenciais_json)

    creds = Credentials.from_service_account_info(
        credenciais_dict,
        scopes=scope
    )

    client = gspread.authorize(creds)

    planilha = client.open_by_key("1sF_wMbYWsne2LVf_DHzhABFQSjlxAFBpaPtKd6vqIbE")

    aba = planilha.sheet1

    aba.append_row([texto])

    return "Texto enviado com sucesso!"

