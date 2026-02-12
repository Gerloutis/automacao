from flask import Flask, render_template
from automacao import executar
import os

app = Flask(__name__)

@app.route("/")
def home():
    resultado = executar()
    return render_template("index.html", resultado=resultado)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)


