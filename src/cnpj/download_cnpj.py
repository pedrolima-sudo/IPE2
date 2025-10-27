"""
MÓDULO DE DOWNLOAD DOS ARQUIVOS DO CNPJ
=======================================

Baixa os arquivos .zip do índice da RFB (dados abertos CNPJ), com retries e logging.

Autor: Pedro Henrique Lima Silva (essa parte é toda mérito do gpt)
Data de criação: 15/10/2025
Última modificação: 18/10/2025
"""
from __future__ import annotations

import argparse
import re
import time
from collections.abc import Iterable
from pathlib import Path
from typing import List

import httpx
from loguru import logger

# Hosts padrão para o índice de arquivos CNPJ
DEFAULT_INDEX_URLS = [
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/",
    "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/",
]


# ----------------------------- HTTP util -----------------------------

def _get_text(url: str, timeout: float = 60.0, retries: int = 3) -> str:
    """GET com retries/backoff simples, retorna .text; levanta erro se esgotar."""
    headers = {"User-Agent": "egressos-portal/1.0 (+github)"}
    exc = None
    for i in range(retries):
        try:
            with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
                r = client.get(url)
                r.raise_for_status()
                return r.text
        except httpx.HTTPError as e:
            exc = e
            logger.warning(f"GET falhou ({e.__class__.__name__}): {e}. Tentando novamente…")
            time.sleep(2 ** i)  # 1s, 2s, 4s
    assert exc is not None
    raise exc


def _stream_download(url: str, dest: Path, timeout: float = 120.0) -> None:
    """Baixa com streaming para arquivo destino."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": "egressos-portal/1.0 (+github)"}
    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        with client.stream("GET", url) as r:
            r.raise_for_status()
            total = int(r.headers.get("Content-Length", "0"))
            tmp = dest.with_suffix(dest.suffix + ".part")
            downloaded = 0
            with open(tmp, "wb") as w:
                for chunk in r.iter_bytes():
                    if chunk:
                        w.write(chunk)
                        downloaded += len(chunk)
            tmp.replace(dest)
    logger.info(f"OK: {dest.name} ({total/1_048_576:.1f} MiB aprox.)")


# ----------------------------- Parsing do índice -----------------------------

def _pick_working_index(index_url: str | None) -> str:
    """Se index_url vier None, tenta os defaults até achar um que responde."""
    candidates = [index_url] if index_url else DEFAULT_INDEX_URLS
    last_err = None
    for base in candidates:
        try:
            _ = _get_text(base, timeout=30, retries=2)
            return base if base.endswith("/") else base + "/"
        except Exception as e:
            last_err = e
            logger.debug(f"Host indisponível: {base} ({e})")
    raise RuntimeError(f"Nenhum host de índice respondeu. Último erro: {last_err}")


def list_months(index_url: str | None = None) -> List[str]:
    """
    Retorna meses disponíveis no índice (ex.: ['2025-05','2025-06',...]).
    """
    base = _pick_working_index(index_url)
    html = _get_text(base)
    # diretórios AAAA-MM/
    months = sorted(set(re.findall(r'href=["\'](\d{4}-\d{2})/["\']', html)))
    if not months:
        raise RuntimeError("Não encontrei diretórios AAAA-MM no índice da RFB.")
    logger.info(f"Meses no índice {base}: {months[-3:] if len(months)>3 else months}")
    return months


def _month_url(index_url: str | None, month: str) -> str:
    base = _pick_working_index(index_url)
    return f"{base}{month}/"


def _natural_key(name: str) -> tuple:
    parts = re.split(r"(\d+)", name)
    key = []
    for part in parts:
        if not part:
            continue
        if part.isdigit():
            key.append(int(part))
        else:
            key.append(part.lower())
    return tuple(key)


def _normalize_prefixes(prefixes: str | Iterable[str] | None) -> tuple[str, ...] | None:
    """
    Normaliza o(s) prefixo(s) informado(s) para comparação case-insensitive.
    Aceita:
        - string única
        - string com separadores ';' ou ','
        - iterável de strings
    Retorna tupla em lowercase ou None (equivalente a "todos").
    """
    if prefixes is None:
        return None

    raw_parts: list[str] = []
    if isinstance(prefixes, str):
        raw_parts = re.split(r"[;,]", prefixes)
    else:
        for item in prefixes:
            if not item:
                continue
            if isinstance(item, str):
                raw_parts.extend(re.split(r"[;,]", item))
            else:
                raw_parts.append(str(item))

    cleaned: list[str] = []
    for part in raw_parts:
        if not part:
            continue
        token = part.strip()
        if not token:
            continue
        if token.lower() in {"all", "todos", "*"}:
            return None
        cleaned.append(token.lower())

    return tuple(cleaned) or None


def list_files(index_url: str | None, month: str, prefix: str | Iterable[str] | None = None) -> List[str]:
    """
    Lista nomes de arquivos .zip dentro de AAAA-MM.
    - Se `prefix` for None/"all", retorna todos os .zip.
    - Se string (ou iterável), filtra pelos prefixos (case-insensitive).
    """
    url = _month_url(index_url, month)
    html = _get_text(url)
    files = sorted(set(re.findall(r'href=["\']([^"\']+\.zip)["\']', html)), key=_natural_key)

    normalized = _normalize_prefixes(prefix)
    if normalized:
        files = [f for f in files if any(f.lower().startswith(p) for p in normalized)]
        if not files:
            logger.warning(f"Nenhum arquivo com prefixo(s) {normalized} em {url}")
    elif not files:
        logger.warning(f"Nenhum arquivo .zip encontrado em {url}")
    return files


# ----------------------------- CLI principal -----------------------------

def download_many(
    index_url: str | None,
    month: str,
    out_dir: Path,
    prefix: str | Iterable[str] | None = None,
    max_files: int = -1,
) -> Path:
    """
    Baixa os arquivos .zip do mês para `out_dir/AAAA-MM/`.
    Se `prefix` (string, iterável ou lista separada por vírgulas) for informado, baixa apenas os arquivos
    que começam com os prefixos informados.
    max_files: -1 para todos; ou um inteiro para limitar (útil para testes).
    """
    normalized = _normalize_prefixes(prefix)
    files = list_files(index_url, month, prefix=normalized)
    if max_files is not None and max_files >= 0:
        files = files[:max_files]

    month_dir = out_dir / month
    month_dir.mkdir(parents=True, exist_ok=True)

    base = _month_url(index_url, month)
    detail = ""
    if normalized:
        detail = " com prefixo(s) " + ", ".join(normalized)
    logger.info(f"Baixando {len(files)} arquivo(s){detail} de {base} → {month_dir}")
    for name in files:
        url = base + name
        dst = month_dir / name
        if dst.exists() and dst.stat().st_size > 0:
            logger.info(f"Pulando (já existe): {dst.name}")
            continue
        logger.info(f"GET {url}")
        _stream_download(url, dst)
    return month_dir


def main():
    ap = argparse.ArgumentParser(description="Baixa arquivos .zip do CNPJ (Dados Abertos RFB).")
    ap.add_argument("--index-url", default=None, help="URL base do índice (deixe vazio para autodetectar).")
    ap.add_argument("--month", default="latest", help="Mês AAAA-MM ou 'latest'.")
    ap.add_argument("--out", default="data/raw/cnpj", help="Diretório base de saída.")
    ap.add_argument(
        "--prefix",
        default=None,
        help="Prefixo(s) desejado(s). Aceita múltiplos separados por vírgula. Use 'all' para baixar todos.",
    )
    ap.add_argument("--max-files", type=int, default=-1, help="-1 para todos; ou limite (ex.: 3).")
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    months = list_months(args.index_url)
    month = months[-1] if args.month.lower() == "latest" else args.month
    if month not in months:
        raise SystemExit(f"Mês '{month}' não encontrado no índice. Disponíveis: {months[-5:]}")

    download_many(args.index_url, month, out_dir, prefix=args.prefix, max_files=args.max_files)
    logger.success("Concluído.")


if __name__ == "__main__":
    main()
