import requests
import re
import json
import pandas as pd
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import os

# =======================================================
# Config PostgreSQL
# (troque pelos seus dados de conexão)
# =======================================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", 5432),
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

def extrair_pagina(url):
    """Extrai produtos da página usando JSON embutido no HTML"""
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code != 200:
        return []

    html = r.text
    pattern = r"gtag\('event','view_item_list',(\{.*?\})\);"
    match = re.search(pattern, html, re.DOTALL)

    if not match:
        return []

    raw_json = match.group(1).replace("'", '"')

    try:
        data = json.loads(raw_json)
        return data.get("items", [])
    except:
        return []

# =======================================================
# Entrada fixa (pode virar parâmetro do job)
# =======================================================
categoria = "https://www.jocar.com.br/acabamentos-externos/aerofolio/?PG=1"
base_url = categoria.split("?PG=")[0] + "?PG={}"
pagina = 1
todos_produtos = []

while True:
    url = base_url.format(pagina)
    print(f"📄 Extraindo página {pagina}...")
    items = extrair_pagina(url)

    if not items:
        break

    todos_produtos.extend(items)
    pagina += 1
    time.sleep(1)  # respeitar servidor

# =======================================================
# Salvar no PostgreSQL
# =======================================================
if todos_produtos:
    df = pd.DataFrame(todos_produtos)

    # Renomear colunas principais
    df.rename(columns={
        "item_id": "id",
        "item_name": "produto",
        "price": "preco",
        "currency": "moeda"
    }, inplace=True)

    # Adicionar timestamp de inserção
    df["data_extracao"] = datetime.utcnow()

    # Conectar ao banco
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Criar tabela caso não exista
    cur.execute("""
        CREATE TABLE IF NOT EXISTS produtos_aerofolio (
            id TEXT,
            produto TEXT,
            preco NUMERIC,
            moeda TEXT,
            data_extracao TIMESTAMP
        )
    """)

    # Inserir em lote
    cols = ["id", "produto", "preco", "moeda", "data_extracao"]
    values = [tuple(x) for x in df[cols].to_numpy()]

    insert_query = f"INSERT INTO produtos_aerofolio ({','.join(cols)}) VALUES %s"
    execute_values(cur, insert_query, values)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ {len(values)} produtos salvos no PostgreSQL!")

else:
    print("⚠️ Nenhum produto encontrado.")
