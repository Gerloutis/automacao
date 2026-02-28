import os
import psycopg2
from flask import Flask, render_template, request, redirect, session, url_for
from dotenv import load_dotenv

print(os.getenv("DATABASE_URL"))
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

    return render_template("operacao.html")


# =========================
# LOGOUT
# =========================
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/presenca")
def presenca():
    return "<h1>Tela de Presença</h1>"

@app.route("/insumos")
def insumos():
    return "<h1>Tela de Insumos</h1>"
    
# =========================
# START
# =========================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))





