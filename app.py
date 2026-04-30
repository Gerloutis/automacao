import os
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# CRIAÇÃO DO APP
# =========================================================
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "troque-isso-em-producao")

# =========================================================
# BLUEPRINTS
# =========================================================
from plan import plan_bp
from routes.auth_routes import auth_bp
from routes.supervisor import supervisor_bp
from routes.adm import adm_bp
from routes.rh import rh_bp

app.register_blueprint(plan_bp)
app.register_blueprint(auth_bp)
app.register_blueprint(supervisor_bp)
app.register_blueprint(adm_bp)
app.register_blueprint(rh_bp)

# =========================================================
# INICIALIZAÇÃO DO BANCO
# =========================================================
from database import inicializar_tabela_atestados
inicializar_tabela_atestados()

# =========================================================
# ENTRY POINT
# =========================================================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
