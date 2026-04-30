from flask import session


# =========================================================
# VERIFICAÇÕES DE SESSÃO
# =========================================================

def usuario_logado() -> bool:
    return "usuario" in session


def _cargo_normalizado() -> str:
    return str(session.get("cargo", "")).strip().lower()


def usuario_supervisor() -> bool:
    return (
        usuario_logado()
        and session.get("tipo") == "operacao"
        and _cargo_normalizado() == "supervisor"
    )


def usuario_adm() -> bool:
    return usuario_logado() and _cargo_normalizado() in {
        "adm", "administracao", "administração", "administrador"
    }


def usuario_rh() -> bool:
    return usuario_logado() and _cargo_normalizado() in {"rh", "recursos humanos"}


def usuario_planejamento() -> bool:
    return session.get("usuario") == "gerle" and session.get("tipo") == "planejamento"
