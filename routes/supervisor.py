import io
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, session, url_for, jsonify, send_file

from config import STATUS_PRESENCA, TIPOS_SOLICITACAO
from routes.auth import usuario_supervisor, usuario_logado
from services.presenca import (
    carregar_presenca_supervisor,
    carregar_supervisores_disponiveis,
    calcular_estatisticas_colaborador,
    calcular_estatisticas_equipe,
    montar_painel_presenca_mensal,
    buscar_colaborador_por_matricula,
)
from services.solicitacoes import (
    buscar_solicitacoes,
    criar_solicitacao_bd,
    mapear_dados_solicitados,
    validar_solicitacao,
    formatar_solicitacoes_para_template,
    montar_resumo_solicitacoes,
    marcar_solicitacoes_visualizadas_supervisor,
)
from services.atestados import (
    buscar_atestados_supervisor,
    buscar_atestado_por_id,
    salvar_atestado_bd,
    validar_arquivo_atestado,
    formatar_atestados_para_template,
)
from utils.helpers import safe_str, parse_data_br

supervisor_bp = Blueprint("supervisor", __name__)


def _normalizar_nome_pessoa(valor):
    return " ".join(safe_str(valor).upper().split())


def _linha_eh_do_supervisor(row, nome_supervisor, usuario_supervisor_val=None):
    nome_linha = _normalizar_nome_pessoa(row.get("COLABORADOR", ""))
    supervisor_nome = _normalizar_nome_pessoa(nome_supervisor)
    usuario_sup = safe_str(usuario_supervisor_val)

    if nome_linha and supervisor_nome and nome_linha == supervisor_nome:
        return True

    matricula_linha = safe_str(row.get("MATRÍCULA", ""))
    if usuario_sup and matricula_linha and matricula_linha == usuario_sup:
        return True

    return False


# =========================================================
# TELAS
# =========================================================

@supervisor_bp.route("/operacao")
def operacao():
    if not usuario_logado():
        return redirect(url_for("auth.login"))
    return render_template("operacao.html")


@supervisor_bp.route("/configuracoes")
def configuracoes():
    if not usuario_logado():
        return redirect(url_for("auth.login"))
    return render_template("configuracoes.html")


@supervisor_bp.route("/presenca")
def presenca():
    if not usuario_supervisor():
        return redirect(url_for("auth.login"))

    try:
        nome_supervisor = session.get("nome")
        df, ws, coluna_dia = carregar_presenca_supervisor(nome_supervisor)

        colaboradores = []
        if not df.empty:
            for _, row in df.iterrows():
                if _linha_eh_do_supervisor(
                    row,
                    nome_supervisor=session.get("nome"),
                    usuario_supervisor_val=session.get("usuario"),
                ):
                    continue

                status_hoje = row.get(coluna_dia, "") if coluna_dia else ""
                estatisticas = calcular_estatisticas_colaborador(row)
                colaboradores.append({
                    "matricula":   safe_str(row.get("MATRÍCULA", "")),
                    "colaborador": safe_str(row.get("COLABORADOR", "")),
                    "cargo":       safe_str(row.get("CARGO", "")),
                    "area":        safe_str(row.get("ÁREA", "")),
                    "cidade":      safe_str(row.get("CIDADE", "")),
                    "turno":       safe_str(row.get("TURNO", "")),
                    "supervisor":  safe_str(row.get("SUPERVISOR", "")),
                    "coordenador": safe_str(row.get("COORDENADOR", "")),
                    "setor":       safe_str(row.get("PROCESSO", "")),
                    "linha":       safe_str(row.get("LINHA", "")),
                    "ponto":       safe_str(row.get("PONTO", "")),
                    "empresa":     safe_str(row.get("EMPRESA", "")),
                    "status_hoje": safe_str(status_hoje),
                    "obs_hoje":    "",
                    "desligado":   safe_str(row.get("STATUS", "")).upper() == "DESLIGADO",
                    **estatisticas,
                })

        matriculas = [c["matricula"] for c in colaboradores if c["matricula"]]
        minhas_solicitacoes = formatar_solicitacoes_para_template(
            buscar_solicitacoes(solicitado_por_usuario=session.get("usuario"), limite=200)
        )
        resumo_solicitacoes = montar_resumo_solicitacoes(minhas_solicitacoes)
        meus_atestados = formatar_atestados_para_template(
            buscar_atestados_supervisor(session.get("usuario"), limite=50)
        )
        supervisores_disponiveis = [
            nome for nome in carregar_supervisores_disponiveis()
            if safe_str(nome).upper() != safe_str(nome_supervisor).upper()
        ]

        return render_template(
            "presenca.html",
            supervisor=nome_supervisor,
            usuario=session.get("usuario"),
            coluna_dia=coluna_dia,
            data_hoje=datetime.now().strftime("%d/%m/%Y"),
            colaboradores=colaboradores,
            matriculas=matriculas,
            status_opcoes=STATUS_PRESENCA,
            tipos_solicitacao=[
                {"valor": k, "label": v["label"], "destino": v["destino"]}
                for k, v in TIPOS_SOLICITACAO.items()
            ],
            supervisores_disponiveis=supervisores_disponiveis,
            minhas_solicitacoes=minhas_solicitacoes,
            resumo_solicitacoes=resumo_solicitacoes,
            meus_atestados=meus_atestados,
        )
    except Exception as e:
        return f"Erro ao carregar presença: {e}"


@supervisor_bp.route("/estatisticas")
def estatisticas_supervisor():
    if not usuario_supervisor():
        return redirect(url_for("auth.login"))

    try:
        nome_supervisor = session.get("nome")
        df, _, _ = carregar_presenca_supervisor(nome_supervisor)
        dados = calcular_estatisticas_equipe(df)
        painel_mensal = montar_painel_presenca_mensal(df)
        return render_template(
            "estatisticas.html",
            supervisor=nome_supervisor,
            usuario=session.get("usuario"),
            data_hoje=datetime.now().strftime("%d/%m/%Y"),
            painel_mensal=painel_mensal,
            **dados,
        )
    except Exception as e:
        return f"Erro ao carregar estatísticas: {e}"


# =========================================================
# PRESENÇA — API
# =========================================================

@supervisor_bp.route("/salvar_presencas", methods=["POST"])
def salvar_presencas():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        dados = request.get_json(force=True)
        presencas = dados.get("presencas", [])

        if not presencas:
            return jsonify({"ok": False, "msg": "Nenhuma presença recebida."}), 400

        nome_supervisor = session.get("nome")
        df, ws, coluna_dia = carregar_presenca_supervisor(nome_supervisor)

        if df.empty:
            return jsonify({"ok": False, "msg": "Nenhum colaborador encontrado."}), 404
        if not coluna_dia:
            return jsonify({"ok": False, "msg": "Coluna do dia atual não encontrada."}), 404

        todos_valores = ws.get_all_values()
        cabecalho = [str(c).strip() for c in todos_valores[0]]

        if "MATRÍCULA" not in cabecalho or "SUPERVISOR" not in cabecalho or "COLABORADOR" not in cabecalho:
            return jsonify({"ok": False, "msg": "Colunas obrigatórias não encontradas."}), 404

        col_idx          = cabecalho.index(coluna_dia) + 1
        idx_matricula    = cabecalho.index("MATRÍCULA")
        idx_supervisor   = cabecalho.index("SUPERVISOR")
        idx_colaborador  = cabecalho.index("COLABORADOR")

        linhas_por_matricula = {}
        linha_supervisor = None
        supervisor_nome_norm = _normalizar_nome_pessoa(nome_supervisor)

        for i, linha in enumerate(todos_valores[1:], start=2):
            mat       = str(linha[idx_matricula]).strip()   if idx_matricula   < len(linha) else ""
            sup       = str(linha[idx_supervisor]).strip().upper() if idx_supervisor < len(linha) else ""
            nome_colab = str(linha[idx_colaborador]).strip() if idx_colaborador < len(linha) else ""

            if sup == str(nome_supervisor).strip().upper():
                if mat:
                    linhas_por_matricula[mat] = i
                if _normalizar_nome_pessoa(nome_colab) == supervisor_nome_norm:
                    linha_supervisor = i

        atualizacoes = 0
        for item in presencas:
            matricula = str(item.get("matricula", "")).strip()
            status    = str(item.get("status",    "")).strip().upper()
            if not matricula or status not in STATUS_PRESENCA:
                continue
            linha_planilha = linhas_por_matricula.get(matricula)
            if not linha_planilha:
                continue
            ws.update_cell(linha_planilha, col_idx, status)
            atualizacoes += 1

        if linha_supervisor:
            ws.update_cell(linha_supervisor, col_idx, "P")
            atualizacoes += 1

        return jsonify({
            "ok": True,
            "msg": f"{atualizacoes} presença(s) salva(s) com sucesso. Supervisor marcado automaticamente como P.",
        })
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


# =========================================================
# SOLICITAÇÕES — API
# =========================================================

@supervisor_bp.route("/solicitacoes/nova", methods=["POST"])
def nova_solicitacao():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        if request.content_type and "multipart/form-data" in request.content_type.lower():
            payload = {
                "matricula":        request.form.get("matricula"),
                "tipo_solicitacao": request.form.get("tipo_solicitacao"),
                "justificativa":    request.form.get("justificativa"),
                "dados_solicitados": {
                    "data_inicio":    request.form.get("data_inicio"),
                    "quantidade_dias": request.form.get("quantidade_dias"),
                    "observacao":     request.form.get("observacao"),
                },
            }
            upload = request.files.get("arquivo")
        else:
            payload = request.get_json(force=True) or {}
            upload  = None

        matricula    = safe_str(payload.get("matricula"))
        tipo         = safe_str(payload.get("tipo_solicitacao"))
        justificativa = safe_str(payload.get("justificativa"))

        colaborador = buscar_colaborador_por_matricula(session.get("nome"), matricula)
        if not colaborador:
            return jsonify({"ok": False, "msg": "Colaborador não encontrado para este supervisor."}), 404

        dados_solicitados = mapear_dados_solicitados(tipo, payload.get("dados_solicitados") or {})
        erro = validar_solicitacao(tipo, dados_solicitados, justificativa)
        if erro:
            return jsonify({"ok": False, "msg": erro}), 400

        if tipo == "adicionar_atestado":
            erro_arquivo = validar_arquivo_atestado(upload)
            if erro_arquivo:
                return jsonify({"ok": False, "msg": erro_arquivo}), 400

            data_inicio = parse_data_br(dados_solicitados.get("data_inicio"))
            if not data_inicio:
                return jsonify({"ok": False, "msg": "Data inicial inválida."}), 400

            hoje = datetime.now().date()
            if data_inicio > hoje:
                return jsonify({"ok": False, "msg": "A data inicial do atestado não pode ser futura."}), 400
            if (hoje - data_inicio).days > 31:
                return jsonify({"ok": False, "msg": "Por enquanto, envie atestados de até 31 dias atrás."}), 400

            quantidade_dias = int(str(dados_solicitados.get("quantidade_dias") or "1"))
            solicitacao_id  = criar_solicitacao_bd(colaborador, tipo, justificativa, dados_solicitados, dict(session))
            registro = salvar_atestado_bd(
                colaborador=colaborador,
                data_referencia=data_inicio,
                quantidade_dias=quantidade_dias,
                observacao=dados_solicitados.get("observacao", ""),
                upload=upload,
                session=dict(session),
                solicitacao_id=solicitacao_id,
            )
            return jsonify({
                "ok": True,
                "msg": f"Solicitação #{solicitacao_id} de atestado enviada com sucesso para ADM.",
                "id": solicitacao_id,
                "anexo_id": registro.get("id"),
            })

        solicitacao_id = criar_solicitacao_bd(colaborador, tipo, justificativa, dados_solicitados, dict(session))
        return jsonify({
            "ok": True,
            "msg": f"Solicitação #{solicitacao_id} enviada com sucesso para {TIPOS_SOLICITACAO[tipo]['destino']}.",
            "id": solicitacao_id,
        })
    except ValueError as e:
        return jsonify({"ok": False, "msg": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao criar solicitação: {e}"}), 500


@supervisor_bp.route("/solicitacoes/minhas", methods=["GET"])
def minhas_solicitacoes():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        solicitacoes = formatar_solicitacoes_para_template(
            buscar_solicitacoes(solicitado_por_usuario=session.get("usuario"), limite=200)
        )
        return jsonify({
            "ok": True,
            "solicitacoes": solicitacoes,
            "resumo": montar_resumo_solicitacoes(solicitacoes),
        })
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao buscar solicitações: {e}"}), 500


@supervisor_bp.route("/solicitacoes/marcar-visualizadas", methods=["POST"])
def marcar_solicitacoes_visualizadas():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        total = marcar_solicitacoes_visualizadas_supervisor(session.get("usuario"))
        return jsonify({"ok": True, "total_atualizadas": total})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao marcar notificações: {e}"}), 500


# =========================================================
# ATESTADOS — API
# =========================================================

@supervisor_bp.route("/atestados/meus", methods=["GET"])
def meus_atestados():
    if not usuario_supervisor():
        return jsonify({"ok": False, "msg": "Não autorizado."}), 401

    try:
        atestados = formatar_atestados_para_template(
            buscar_atestados_supervisor(session.get("usuario"), limite=100)
        )
        return jsonify({"ok": True, "atestados": atestados})
    except Exception as e:
        return jsonify({"ok": False, "msg": f"Erro ao buscar atestados: {e}"}), 500


@supervisor_bp.route("/atestados/<int:atestado_id>/arquivo", methods=["GET"])
def baixar_arquivo_atestado(atestado_id):
    if not usuario_logado():
        return redirect(url_for("auth.login"))

    registro = buscar_atestado_por_id(atestado_id)
    if not registro:
        return "Atestado não encontrado.", 404

    if usuario_supervisor() and registro.get("supervisor_usuario") != session.get("usuario"):
        return "Não autorizado.", 403

    arquivo = registro.get("arquivo")
    if arquivo is None:
        return "Arquivo não encontrado.", 404

    if hasattr(arquivo, "tobytes"):
        arquivo = arquivo.tobytes()

    return send_file(
        io.BytesIO(arquivo),
        mimetype=registro.get("tipo_arquivo") or "application/octet-stream",
        as_attachment=False,
        download_name=registro.get("nome_arquivo") or f"atestado_{atestado_id}",
    )


@supervisor_bp.route("/atestados/novo", methods=["POST"])
def novo_atestado():
    return jsonify({"ok": False, "msg": "Use /solicitacoes/nova para enviar atestado para aprovação da ADM."}), 400
