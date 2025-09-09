#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consulta massiva de CNPJs usando BrasilAPI
Retorna JSON completo + planilhas resumidas (cnpj, nome, uf)
Com cache local e funções reutilizáveis
"""

import os
import re
import json
import time
import argparse
import logging
from typing import List, Dict, Any
import requests
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

API_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# --------------------------
# Utilitários CNPJ
# --------------------------
def clean_cnpj(s: str) -> str:
    return re.sub(r"\D", "", str(s)) if s else ""

def validate_cnpj(cnpj: str) -> bool:
    c = re.sub(r"\D", "", str(cnpj))
    if len(c) != 14 or c == c[0] * 14:
        return False
    def calc(digs, weights):
        s = sum(int(d) * w for d, w in zip(digs, weights))
        r = s % 11
        return '0' if r < 2 else str(11 - r)
    w1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    w2 = [6] + w1
    return c[-2:] == calc(c[:12], w1) + calc(c[:12] + calc(c[:12], w1), w2)

# --------------------------
# I/O: leitura de arquivo
# --------------------------
def read_input_file(path: str) -> List[str]:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, header=None, dtype=str)
        raw = df.iloc[:, 0].astype(str).tolist()
    elif ext == ".csv":
        df = pd.read_csv(path, header=None, dtype=str)
        raw = df.iloc[:, 0].astype(str).tolist()
    else:
        with open(path, "r", encoding="utf-8") as f:
            raw = [line.strip() for line in f if line.strip()]
    return [clean_cnpj(r) for r in raw if r]

# --------------------------
# Cache JSON
# --------------------------
def load_cache(path: str) -> Dict[str, Any]:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache: Dict[str, Any], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

# --------------------------
# Consulta API BrasilAPI
# --------------------------
def consulta_cnpj(cnpj: str, timeout: int = 14) -> dict:
    url = API_URL.format(cnpj=cnpj)
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# --------------------------
# Funções Reutilizáveis
# --------------------------
def consulta_completa(cnpj: str, cache_path="cache.json", force=False) -> dict:
    cache = load_cache(cache_path)
    if not validate_cnpj(cnpj):
        return {"cnpj": cnpj, "error": "CNPJ inválido"}
    if cnpj in cache and not force:
        return cache[cnpj]
    try:
        api_json = consulta_cnpj(cnpj)
        cache[cnpj] = api_json
    except Exception as e:
        api_json = {"cnpj": cnpj, "error": str(e)}
        cache[cnpj] = api_json
    save_cache(cache, cache_path)
    return api_json

def consulta_uf(cnpj: str, cache_path="cache.json", force=False) -> dict:
    full = consulta_completa(cnpj, cache_path, force)
    if "error" in full:
        return full
    return {
        "cnpj": full.get("cnpj", ""),
        "nome": full.get("razao_social", full.get("nome", "")),
        "uf": full.get("uf", "")
    }

def processar_lista(cnpjs: List[str], cache_path="cache.json",
                    wait_between=0.2, pause_every=10, pause_seconds=2) -> List[dict]:
    resultados = []
    for i, cnpj in enumerate(cnpjs, start=1):
        resultado = consulta_uf(cnpj, cache_path)
        resultados.append(resultado)
        logging.info(f"{i}/{len(cnpjs)} - {resultado.get('cnpj', '???')}")

        # Pausa entre consultas
        time.sleep(wait_between)
        if i % pause_every == 0:
            logging.info(f"Pausa de {pause_seconds}s após {i} consultas")
            time.sleep(pause_seconds)
    return resultados

# --------------------------
# Orquestrador
# --------------------------
def run(args):
    cnpjs = read_input_file(args.input)
    logging.info(f"Lidos {len(cnpjs)} CNPJs")
    resultados = processar_lista(cnpjs, args.cache, args.wait_between, args.pause_every, args.pause_seconds)

    # --------------------------
    # Salvar arquivos
    # --------------------------
    os.makedirs(args.out_dir, exist_ok=True)

    # JSON completo
    with open(os.path.join(args.out_dir, "results.json"), "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    # DataFrame resumido (apenas cnpj, nome, uf)
    df = pd.DataFrame(resultados)
    df.to_excel(os.path.join(args.out_dir, "results.xlsx"), index=False)
    df.to_csv(os.path.join(args.out_dir, "results.csv"), index=False, sep=";")

    logging.info("Planilhas (resumidas) e JSON (completo) salvos com sucesso.")

# --------------------------
# CLI
# --------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Consulta CNPJs via BrasilAPI")
    p.add_argument("--input", required=True, help="Arquivo .xlsx, .csv ou .txt com CNPJs")
    p.add_argument("--cache", default="cache.json", help="Arquivo de cache")
    p.add_argument("--out-dir", default="out", help="Diretório de saída")
    p.add_argument("--wait-between", type=float, default=0.2, help="Tempo entre consultas (s)")
    p.add_argument("--pause-every", type=int, default=10, help="Pausa a cada N consultas")
    p.add_argument("--pause-seconds", type=float, default=2.0, help="Duração da pausa (s)")
    p.add_argument("--force", action="store_true", help="Força nova consulta mesmo se existir no cache")
    return p.parse_args()

if __name__ == "__main__":
    run(parse_args())
    