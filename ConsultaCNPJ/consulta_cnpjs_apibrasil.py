#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consulta massiva de CNPJs usando APIBrasil (Jonathan)
Endpoint: POST /dados/cnpj
Regras:
 - Pausa de 0.2s entre consultas
 - Pausa de 2s a cada 10 consultas
 - Cache local em JSON
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

API_URL = "https://gateway.apibrasil.io/api/v2/dados/cnpj"

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
# Consulta API
# --------------------------
def consulta_cnpj(cnpj: str, bearer: str, device: str, timeout: int = 15) -> Any:
    headers = {
        "Authorization": f"Bearer {bearer}",
        "DeviceToken": device,
        "Content-Type": "application/json"
    }
    payload = {"cnpj": cnpj}
    resp = requests.post(API_URL, headers=headers, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# --------------------------
# Extrair campos úteis
# --------------------------
def find_value(obj: dict, keys: List[str]):
    if not isinstance(obj, dict):
        return None
    for k in keys:
        if k in obj and obj[k]:
            return obj[k]
    for v in obj.values():
        if isinstance(v, dict):
            res = find_value(v, keys)
            if res:
                return res
    return None

def extract_minimal(api_json: Any) -> Dict[str, Any]:
    if not api_json:
        return {"razao_social": None, "situacao": None, "uf": None}
    obj = api_json if isinstance(api_json, dict) else {}
    return {
        "razao_social": find_value(obj, ["razao_social", "nome", "nome_empresarial", "nome_fantasia"]),
        "situacao": find_value(obj, ["situacao", "situacao_cadastral", "status"]),
        "uf": find_value(obj, ["uf", "estado", "sigla_uf"])
    }

# --------------------------
# Orquestrador
# --------------------------
def run(args):
    cnpjs = read_input_file(args.input)
    logging.info(f"Lidos {len(cnpjs)} CNPJs")

    cache = load_cache(args.cache)
    results = []
    query_count = 0

    for i, cnpj in enumerate(cnpjs, start=1):
        if not validate_cnpj(cnpj):
            logging.warning(f"{i}/{len(cnpjs)} - CNPJ inválido: {cnpj}")
            results.append({"CNPJ": cnpj, "Razao Social": None, "Situacao Cadastral": "CNPJ inválido", "UF": None})
            continue

        if cnpj in cache and not args.force:
            api_json = cache[cnpj]
            logging.info(f"{i}/{len(cnpjs)} - {cnpj} (cache)")
        else:
            try:
                api_json = consulta_cnpj(cnpj, args.bearer, args.device, timeout=args.timeout)
                cache[cnpj] = api_json
                save_cache(cache, args.cache)
                logging.info(f"{i}/{len(cnpjs)} - {cnpj} (ok)")
            except Exception as e:
                api_json = {"__error__": str(e)}
                cache[cnpj] = api_json
                save_cache(cache, args.cache)
                logging.error(f"{i}/{len(cnpjs)} - {cnpj} (erro: {e})")
            time.sleep(args.wait_between)

        minimal = extract_minimal(api_json)
        results.append({
            "CNPJ": cnpj,
            "Razao Social": minimal.get("razao_social"),
            "Situacao Cadastral": minimal.get("situacao"),
            "UF": minimal.get("uf")
        })

        query_count += 1
        if query_count % args.pause_every == 0:
            logging.info(f"Pausa de {args.pause_seconds}s...")
            time.sleep(args.pause_seconds)

    df = pd.DataFrame(results, columns=["CNPJ", "Razao Social", "Situacao Cadastral", "UF"])
    os.makedirs(args.out_dir, exist_ok=True)
    df.to_excel(os.path.join(args.out_dir, "results.xlsx"), index=False)
    df.to_csv(os.path.join(args.out_dir, "results.csv"), index=False)
    logging.info("Planilhas salvas em /out")
    save_cache(cache, args.cache)

# --------------------------
# CLI
# --------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Consulta CNPJs via APIBrasil")
    p.add_argument("--input", required=True, help="Arquivo .xlsx, .csv ou .txt com CNPJs")
    p.add_argument("--bearer", required=True, help="Bearer Token da APIBrasil")
    p.add_argument("--device", required=True, help="Device Token da APIBrasil")
    p.add_argument("--cache", default="cache.json", help="Arquivo de cache")
    p.add_argument("--out-dir", default="out", help="Diretório de saída")
    p.add_argument("--wait-between", type=float, default=0.2, help="Tempo entre consultas (s)")
    p.add_argument("--pause-every", type=int, default=10, help="Pausa a cada N consultas")
    p.add_argument("--pause-seconds", type=float, default=2.0, help="Duração da pausa (s)")
    p.add_argument("--timeout", type=int, default=15, help="Timeout HTTP")
    p.add_argument("--force", action="store_true", help="Força nova consulta mesmo se existir no cache")
    return p.parse_args()

if __name__ == "__main__":
    run(parse_args())
