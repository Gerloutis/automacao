from flask import Flask, render_template
from automacao import executar

app = Flask(__name__)

@app.route("/")
def home():
    resultado = executar()
    return render_template("index.html", resultado=resultado)

if __name__ == "__main__":
    app.run(debug=True)
