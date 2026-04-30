import os
import json
import threading
import gspread
from google.oauth2.service_account import Credentials

_gc = None
_lock = threading.Lock()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_gc() -> gspread.Client:
    """
    Retorna um cliente gspread autenticado (singleton com thread-lock).
    As credenciais vêm da variável de ambiente GOOGLE_CREDENTIALS (JSON string).
    """
    global _gc
    with _lock:
        if _gc is None:
            credenciais_json = os.environ.get("GOOGLE_CREDENTIALS")
            if not credenciais_json:
                raise RuntimeError("Variável de ambiente GOOGLE_CREDENTIALS não definida.")
            credenciais_dict = json.loads(credenciais_json)
            creds = Credentials.from_service_account_info(credenciais_dict, scopes=SCOPES)
            _gc = gspread.authorize(creds)
    return _gc


def ensure_gc() -> gspread.Client:
    """Atalho público — garante que o cliente está inicializado."""
    return get_gc()
