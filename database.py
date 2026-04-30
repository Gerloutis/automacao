import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from config import DATABASE_URL, ATESTADOS_TABLE

# =========================================================
# ENGINE COM POOL (SQLAlchemy)
# Reutiliza conexões em vez de abrir uma nova a cada request
# =========================================================
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,          # conexões mantidas abertas
    max_overflow=10,      # conexões extras em pico
    pool_timeout=30,      # segundos para esperar uma conexão livre
    pool_pre_ping=True,   # testa a conexão antes de usar (evita conexões mortas)
)


def get_connection():
    """
    Retorna uma conexão direta psycopg2 (para operações com RealDictCursor).
    Sempre usar em bloco try/finally para garantir fechamento.

    Exemplo de uso:
        conn = get_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cur.execute("SELECT ...")
            rows = cur.fetchall()
            conn.commit()
        finally:
            cur.close()
            conn.close()
    """
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def inicializar_tabela_atestados():
    """Garante que a tabela de atestados existe com todas as colunas necessárias."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ATESTADOS_TABLE} (
                id                BIGSERIAL PRIMARY KEY,
                solicitacao_id    BIGINT,
                matricula         VARCHAR(50)  NOT NULL,
                colaborador_nome  TEXT         NOT NULL,
                supervisor_usuario VARCHAR(100),
                supervisor_nome   TEXT,
                data_referencia   DATE         NOT NULL,
                quantidade_dias   INTEGER      NOT NULL DEFAULT 1,
                observacao        TEXT,
                nome_arquivo      TEXT         NOT NULL,
                tipo_arquivo      VARCHAR(120) NOT NULL,
                tamanho_bytes     BIGINT       NOT NULL,
                arquivo           BYTEA        NOT NULL,
                criado_em         TIMESTAMP    NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{ATESTADOS_TABLE}_matricula_data
            ON {ATESTADOS_TABLE} (matricula, data_referencia DESC)
            """
        )
        # Colunas adicionadas em versões posteriores (idempotente)
        cur.execute(
            f"ALTER TABLE {ATESTADOS_TABLE} ADD COLUMN IF NOT EXISTS solicitacao_id BIGINT"
        )
        cur.execute(
            f"ALTER TABLE {ATESTADOS_TABLE} ADD COLUMN IF NOT EXISTS quantidade_dias INTEGER NOT NULL DEFAULT 1"
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()
