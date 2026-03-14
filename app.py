import os
import psycopg2
from flask import Flask, render_template, request, redirect, session, url_for, jsonify
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import io
import threading
import time
from contextlib import redirect_stdout
from datetime import date, timedelta

load_dotenv()

app = Flask(__name__)
app.secret_key = "chave_super_secreta"

DATABASE_URL = os.getenv("DATABASE_URL")

# 🔗 psycopg2 (login)
def get_connection():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

# 🔗 SQLAlchemy (pandas)
engine = create_engine(DATABASE_URL)

try:
    from planejamento import (
        get_gc,
        processar_dia,
        _unique_preservando_ordem,
        des_para_qhc,
        qhc_para_base_mae_desligados,
        atualizar_act_quadro_fy,
        atualizar_whs_to_percent,
        to_mes,
        buscar_ihc_base_mae as abs_buscar_ihc,
        etapa_lista_para_abs,
        abs_para_planejamento as abs_para_plan,
        atualizar_whs_abs_percent,
        abs_mes
    )
except Exception as e:
    print("Erro ao importar app_planejamento:", e)
    get_gc = None
    processar_dia = None
    _unique_preservando_ordem = None
    des_para_qhc = None
    qhc_para_base_mae_desligados = None
    atualizar_act_quadro_fy = None
    atualizar_whs_to_percent = None
    to_mes = None
    abs_buscar_ihc = None
    etapa_lista_para_abs = None
    abs_para_plan = None
    atualizar_whs_abs_percent = None
    abs_mes = None

_gc = None
_lock = threading.Lock()
to_percent_cache = {}

def ensure_gc():
    global _gc
    with _lock:
        if _gc is None:
            if get_gc is None:
                raise RuntimeError("Função get_gc não foi carregada do app_planejamento.")
            _gc = get_gc()
    return _gc

def usuario_planejamento():
    return session.get("usuario") == "gerle" and session.get("tipo") == "planejamento"


# =========================
# LOGIN
# =========================
@app.route("/")
def login():
    return render_template("login.html")

@app.route("/planejamento")
def planejamento():
    if not usuario_planejamento():
        return redirect(url_for("login"))
    return render_template("planejamento.html")

@app.route("/verify", methods=["GET"])
def verify():
    if not usuario_planejamento():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        gc = ensure_gc()
        _ = bool(gc)
        return jsonify({"ok": True, "msg": "Credenciais e conexão OK."})
    except Exception as e:
        return jsonify({
            "ok": False,
            "msg": "Falha na verificação.",
            "detail": str(e)
        }), 500

@app.route("/run", methods=["POST"])
def run():
    if not usuario_planejamento():
        return jsonify({"ok": False, "log": "Não autorizado."}), 401

    payload = request.get_json(force=True, silent=True) or {}
    tasks = payload.get("tasks") or []

    if isinstance(tasks, str):
        tasks = [tasks]

    tasks = [t.strip() for t in tasks if str(t).strip()]
    if not tasks:
        return jsonify({"ok": False, "log": "Nenhuma tarefa selecionada."}), 400

    datas = []
    if isinstance(payload.get("datas"), list) and payload["datas"]:
        datas = [str(d).strip() for d in payload["datas"] if str(d).strip()]
    elif payload.get("data"):
        datas = [str(payload["data"]).strip()]

    if not datas:
        return jsonify({"ok": False, "log": "Informe ao menos uma data."}), 400

    if _unique_preservando_ordem:
        datas = _unique_preservando_ordem(datas)

    order = [
        "des_qhc",
        "qhc_base_mae",
        "act_quadro_fy",
        "to_planejamento",
        "whs_to",
        "presenca_abs",
        "resumo_to",
        "resumo_abs"
    ]

    tasks_ordered = [t for t in order if t in tasks]
    buf = io.StringIO()

    try:
        gc = ensure_gc()

        with redirect_stdout(buf):
            print(f"[UI] Tarefas selecionadas: {', '.join(tasks_ordered)}")

            for d in datas:
                print(f"[UI] === Data {d} ===")

                for t in tasks_ordered:
                    try:
                        if t == "to_planejamento":
                            if processar_dia:
                                to_percent_cache[d] = processar_dia(gc, d)
                            else:
                                print("[ERRO] processar_dia não encontrada.")

                        elif t == "des_qhc":
                            if des_para_qhc:
                                des_para_qhc(d)
                            else:
                                print("[ERRO] des_para_qhc não encontrada.")

                        elif t == "qhc_base_mae":
                            if qhc_para_base_mae_desligados:
                                qhc_para_base_mae_desligados(gc, d)
                            else:
                                print("[ERRO] qhc_para_base_mae_desligados não encontrada.")

                        elif t == "act_quadro_fy":
                            if atualizar_act_quadro_fy:
                                atualizar_act_quadro_fy(gc, d)
                            else:
                                print("[ERRO] atualizar_act_quadro_fy não encontrada.")

                        elif t == "whs_to":
                            if atualizar_whs_to_percent:
                                to_val = to_percent_cache.get(d)
                                if to_val is None and processar_dia:
                                    to_val = processar_dia(gc, d)
                                    to_percent_cache[d] = to_val
                                atualizar_whs_to_percent(gc, d, to_val)
                            else:
                                print("[ERRO] atualizar_whs_to_percent não encontrada.")

                        elif t == "presenca_abs":
                            if not (abs_buscar_ihc and etapa_lista_para_abs and abs_para_plan and atualizar_whs_abs_percent):
                                print("[ERRO] Funções de ABS não foram encontradas.")
                            else:
                                def _parse_data(dstr):
                                    dd_s, mm_s, yy_s = dstr.split("/")
                                    return date(int(yy_s), int(mm_s), int(dd_s))

                                def _fmt_data(dobj):
                                    return f"{dobj.day:02d}/{dobj.month:02d}/{dobj.year}"

                                def _prev_nao_domingo(dstr):
                                    atual = _parse_data(dstr)
                                    prev = atual - timedelta(days=1)
                                    if prev.weekday() == 6:
                                        prev -= timedelta(days=1)
                                    return _fmt_data(prev)

                                def _rodar_para_data(d_exec):
                                    dd, mm, yyyy = d_exec.split("/")
                                    print(f"\n[ABS] ===== Processando {d_exec} =====")
                                    ihc_ext = abs_buscar_ihc(gc, d_exec)

                                    if ihc_ext is None:
                                        print("[ABS] IHC não encontrado na Base Mãe.")
                                    else:
                                        abs_ws = etapa_lista_para_abs(gc, d_exec, dd, mm, yyyy)
                                        abs_percent_str = abs_para_plan(gc, abs_ws, d_exec, ihc_override=ihc_ext)
                                        atualizar_whs_abs_percent(gc, d_exec, abs_percent_str)

                                hoje = _parse_data(d)
                                wd = hoje.weekday()

                                if wd == 0:
                                    d_sabado = _prev_nao_domingo(d)
                                    d_sexta = _fmt_data(hoje - timedelta(days=3))
                                    _rodar_para_data(d_sexta)
                                    _rodar_para_data(d_sabado)
                                    _rodar_para_data(d)
                                else:
                                    d_anterior = _prev_nao_domingo(d)
                                    _rodar_para_data(d_anterior)
                                    _rodar_para_data(d)

                        elif t == "resumo_to":
                            if to_mes:
                                to_mes(gc, d)
                            else:
                                print("[ERRO] to_mes não encontrada.")

                        elif t == "resumo_abs":
                            if abs_mes:
                                abs_mes(gc)
                            else:
                                print("[ERRO] abs_mes não encontrada.")

                        else:
                            print(f"[ERRO] Task desconhecida: {t}")

                        time.sleep(1)

                    except Exception as e:
                        print(f"[ERRO] Falha em '{t}' ({d}): {e}")

            print("[FIM] Lote concluído.")

    except Exception as e:
        return jsonify({"ok": False, "log": f"Falha geral: {e}\n{buf.getvalue()}"}), 500

    return jsonify({"ok": True, "log": buf.getvalue()}), 200
    
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


