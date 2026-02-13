from flask import Flask, render_template, request
from automacao import enviar_para_planilha
import os

app = Flask(__name__)

@app.route("/", methods=["GET", "POST"])
def home():

    mensagem = ""

    if request.method == "POST":
        texto = request.form.get("texto")

        if texto:
            mensagem = enviar_para_planilha(texto)

    return render_template("login.html", mensagem=mensagem)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

