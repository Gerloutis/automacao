import os
import psycopg2
from flask import Flask, render_template, request, redirect, session, url_for
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = "chave_super_secreta"

# 🔗 conexão com Railway
def get_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"), sslmode="require")


# =========================
# LOGIN
# =========================
@app.route("/")
def login():
    return render_template("login.html")


@app.route("/entrar", methods=["POST"])
def entrar():

    usuario = request.form["usuario"]
    senha = request.form["senha"]

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM login WHERE usuario = %s AND senha = %s",
        (usuario, senha)
    )

    resultado = cursor.fetchone()

    cursor.close()
    conn.close()

    if resultado:
        session["usuario"] = usuario
        return redirect(url_for("operacao"))

    return "Usuário ou senha inválidos"


# =========================
# ÁREA LOGADA
# =========================
@app.route("/operacao")
def operacao():

    if "usuario" not in session:
        return redirect(url_for("login"))

    return f"Bem-vindo, {session['usuario']}! 🚀"


# =========================
# LOGOUT
# =========================
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# =========================
# START
# =========================
if __name__ == "__main__":
    app.run(debug=True)
