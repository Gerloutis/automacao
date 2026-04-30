from flask import Blueprint, render_template, request, redirect, session, url_for, jsonify

from routes.auth import usuario_rh
from services.solicitacoes import (
    buscar_solicitacoes,
    buscar_solicitacao_por_id,
    atualizar_status_solicitacao,
    formatar_solicitacoes_para_template,
)
from utils.helpers import safe_str

rh_bp = Blueprint("rh", __name__)


@rh_bp.route("/rh")
def rh():
    if not usuario_rh():
        return redirect(url_for("auth.login"))

    pendentes = formatar_solicitacoes_para_template(
        buscar_solicitacoes(destino_setor="RH", status="PENDENTE", limite=200)
    )
    historico = formatar_solicitacoes_para_template(
        buscar_solicitacoes(destino_setor="RH", limite=200)
    )
    return render_template(
        "rh.html",
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        pendentes=pendentes,
        historico=historico,
    )


@rh_bp.route("/solicitacoes/rh/<int:solicitacao_id>/decidir", methods=["POST"])
def decidir_solicitacao_rh(solicitacao_id):
    if not usuario_rh():
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
        if destino != "RH":
            return jsonify({"ok": False, "msg": "Apenas RH pode tratar essa solicitação."}), 403
        if acao not in {"APROVAR", "RECUSAR"}:
            return jsonify({"ok": False, "msg": "Ação inválida."}), 400

        novo_status = "APROVADA" if acao == "APROVAR" else "RECUSADA"
        atualizar_status_solicitacao(solicitacao_id, novo_status, resposta, dict(session))
        return jsonify({"ok": True, "msg": f"Solicitação {novo_status.lower()} com sucesso."})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao tratar solicitação: {e}"}), 500
