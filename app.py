import os
import psycopg2
from flask import Flask, render_template, request, redirect, session, url_for
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

app = Flask(__name__)
app.secret_key = "chave_super_secreta"

DATABASE_URL = os.getenv("DATABASE_URL")

# 🔗 psycopg2 (login)
def get_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

# 🔗 SQLAlchemy (pandas)
engine = create_engine(DATABASE_URL)


# =========================
# LOGIN
# =========================
@app.route("/")
def login():
    return render_template("login.html")

@app.route("/planejamento")
def planejamento():

    if not usuario_logado():
        return redirect(url_for("login"))

    return render_template("planejamento.html")
    
@app.route("/entrar", methods=["POST"])
def entrar():

    usuario = request.form["usuario"]
    senha = request.form["senha"]

    # LOGIN ESPECIAL PARA PLANEJAMENTO
    if usuario == "gerle" and senha == "123":
        session["usuario"] = usuario
        session["tipo"] = "planejamento"
        return redirect(url_for("planejamento"))

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
        session["tipo"] = "operacao"
        return redirect(url_for("operacao"))

    return "Usuário ou senha inválidos"
# =========================
# PROTEÇÃO
# =========================
def usuario_logado():
    return "usuario" in session


# =========================
# ÁREA LOGADA
# =========================
@app.route("/operacao")
def operacao():

    if not usuario_logado():
        return redirect(url_for("login"))

    return render_template("operacao.html")


# =========================
# CONFIGURAÇÕES
# =========================
@app.route("/configuracoes")
def configuracoes():

    if not usuario_logado():
        return redirect(url_for("login"))

    return render_template("configuracoes.html")


# =========================
# IMPORTAR EXCEL
# =========================
@app.route("/importar_colaboradores", methods=["POST"])
def importar_colaboradores():

    if not usuario_logado():
        return redirect(url_for("login"))

    arquivo = request.files["arquivo"]

    df = pd.read_excel(arquivo)

    df.columns = df.columns.str.strip()
    
    df = df[[
        "MATRÍCULA",
        "COLABORADOR",
        "COORDENADOR",
        "SUPERVISOR",
        "CARGO",
        "TURNO",
        "ÁREA",
        "PROCESSO",
        "STATUS",
        "Data Admissão",
        "Data Demissão",
        "EMPRESA"
    ]]
    
    df.columns = [
        "matricula",
        "nome",
        "coordenador",
        "supervisor",
        "cargo",
        "turno",
        "area",
        "setor",
        "status",
        "data_admissao",
        "data_demissao",
        "empresa"
    ]
    df.to_sql(
        "colaboradores",
        engine,
        if_exists="append",
        index=False
    )

    return "✅ Colaboradores importados com sucesso!"


# =========================
# OUTRAS TELAS
# =========================
@app.route("/presenca")
def presenca():
    return "<h1>Tela de Presença</h1>"


@app.route("/insumos")
def insumos():
    return "<h1>Tela de Insumos</h1>"


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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


