from flask import Flask, render_template, redirect, request
from automacao import executar
import os

app = Flask(__name__)

@app.before_request
def force_https():
    if request.headers.get("X-Forwarded-Proto") == "http":
        return redirect(request.url.replace("http://", "https://"))

@app.route("/")
def home():
    resultado = executar()
    return render_template("index.html", resultado=resultado)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
