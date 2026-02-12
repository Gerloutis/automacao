from flask import Flask, render_template, redirect, request
from automacao import executar
import os

app = Flask(__name__)

@app.before_request
def redirect_to_www():
    if request.host == "agtechdigital.com.br":
        return redirect("https://www.agtechdigital.com.br" + request.full_path)

@app.route("/")
def home():
    resultado = executar()
    return render_template("index.html", resultado=resultado)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
