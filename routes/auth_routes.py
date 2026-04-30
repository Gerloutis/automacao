from flask import Blueprint, render_template, request, redirect, session, url_for
from database import get_connection
from routes.auth import usuario_planejamento
from utils.helpers import safe_str

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/")
def login():
    return render_template("login.html")


@auth_bp.route("/entrar", methods=["POST"])
def entrar():
    usuario = request.form["usuario"].strip()
    senha = request.form["senha"].strip()

    # Usuário de planejamento hardcoded (migrar para BD futuramente)
    if usuario == "gerle" and senha == "123":
        session["usuario"] = usuario
        session["tipo"] = "planejamento"
        session["cargo"] = "planejamento"
        session["nome"] = "Gerle"
        return redirect(url_for("planejamento.planejamento"))

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

    session["usuario"]   = resultado[1]
    session["tipo"]      = "operacao"
    session["cargo"]     = resultado[3]
    session["nome"]      = resultado[4]
    session["matricula"] = resultado[5]

    cargo = safe_str(resultado[3]).lower()
    if cargo == "supervisor":
        return redirect(url_for("supervisor.operacao"))
    if cargo in {"adm", "administracao", "administração", "administrador"}:
        return redirect(url_for("adm.administracao"))
    if cargo in {"rh", "recursos humanos"}:
        return redirect(url_for("rh.rh"))

    return redirect(url_for("supervisor.operacao"))


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
