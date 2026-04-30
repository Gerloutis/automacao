import pandas as pd
from flask import Blueprint, render_template, request, redirect, session, url_for, jsonify

from routes.auth import usuario_adm, usuario_logado
from services.solicitacoes import (
    buscar_solicitacoes,
    buscar_solicitacao_por_id,
    atualizar_status_solicitacao,
    formatar_solicitacoes_para_template,
)
from utils.helpers import safe_str
from database import engine

adm_bp = Blueprint("adm", __name__)


@adm_bp.route("/administracao")
def administracao():
    if not usuario_adm():
        return redirect(url_for("auth.login"))

    pendentes = formatar_solicitacoes_para_template(
        buscar_solicitacoes(destino_setor="ADM", status="PENDENTE", limite=200)
    )
    historico = formatar_solicitacoes_para_template(
        buscar_solicitacoes(destino_setor="ADM", limite=200)
    )
    return render_template(
        "administracao.html",
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        pendentes=pendentes,
        historico=historico,
    )


@adm_bp.route("/solicitacoes/<int:solicitacao_id>/decidir", methods=["POST"])
def decidir_solicitacao(solicitacao_id):
    if not usuario_adm():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        payload  = request.get_json(force=True) or {}
        acao     = safe_str(payload.get("acao")).upper()
        resposta = safe_str(payload.get("resposta"))

        solicitacao = buscar_solicitacao_por_id(solicitacao_id)
        if not solicitacao:
            return jsonify({"ok": False, "msg": "Solicitação não encontrada."}), 404
        if solicitacao.get("status") != "PENDENTE":
            return jsonify({"ok": False, "msg": "Essa solicitação já foi tratada."}), 400

        destino = safe_str(solicitacao.get("destino_setor")).upper()
        if destino != "ADM":
            return jsonify({"ok": False, "msg": "Apenas ADM pode tratar essa solicitação."}), 403
        if acao not in {"APROVAR", "RECUSAR"}:
            return jsonify({"ok": False, "msg": "Ação inválida."}), 400

        novo_status = "APROVADA" if acao == "APROVAR" else "RECUSADA"
        atualizar_status_solicitacao(solicitacao_id, novo_status, resposta, dict(session))
        return jsonify({"ok": True, "msg": f"Solicitação {novo_status.lower()} com sucesso."})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao tratar solicitação: {e}"}), 500


@adm_bp.route("/importar_colaboradores", methods=["POST"])
def importar_colaboradores():
    if not usuario_logado():
        return redirect(url_for("auth.login"))

    arquivo = request.files["arquivo"]
    df = pd.read_excel(arquivo)
    df.columns = df.columns.str.strip()

    df = df[[
        "MATRÍCULA", "COLABORADOR", "COORDENADOR", "SUPERVISOR", "CARGO", "TURNO",
        "ÁREA", "PROCESSO", "STATUS", "Data Admissão", "Data Demissão", "EMPRESA",
    ]]
    df.columns = [
        "matricula", "nome", "coordenador", "supervisor", "cargo", "turno",
        "area", "setor", "status", "data_admissao", "data_demissao", "empresa",
    ]
    df.to_sql("colaboradores", engine, if_exists="append", index=False)
    return "✅ Colaboradores importados com sucesso!"


@adm_bp.route("/insumos")
def insumos():
    return "<h1>Tela de Insumos</h1>"
