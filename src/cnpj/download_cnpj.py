from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
from pathlib import Path
import re
import httpx
from bs4 import BeautifulSoup
from loguru import logger

# Índice oficial (publicação mensal)
DEFAULT_INDEX_URL = "https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/"

@dataclass
class CNPJFile:
    url: str
    name: str


def _get(url: str, timeout: float = 30.0) -> str:
    headers = {"User-Agent": "egressos-portal/1.0 (github.com)"}
    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.text


def list_months(index_url: str = DEFAULT_INDEX_URL) -> list[str]:
    """Lista subpastas AAAA-MM publicadas no índice oficial."""
    html = _get(index_url)
    soup = BeautifulSoup(html, "lxml")
    months = []
    for a in soup.select("a"):
        href = a.get("href", "")
        if re.fullmatch(r"\d{4}-\d{2}/", href):
            months.append(href.rstrip("/"))
    months_sorted = sorted(months)
    logger.info(f"Meses encontrados ({len(months_sorted)}): {months_sorted[-3:]}")
    return months_sorted


def list_files(month: str, index_url: str = DEFAULT_INDEX_URL, prefix_filter: str | None = None) -> list[CNPJFile]:
    """Lista arquivos de um mês específico (ex.: '2024-09').
    prefix_filter: por exemplo 'Socios' ou 'Estabelecimentos'.
    """
    base = index_url.rstrip("/") + f"/{month}/"
    html = _get(base)
    soup = BeautifulSoup(html, "lxml")
    out: list[CNPJFile] = []
    for a in soup.select("a"):
        name = a.get_text(strip=True)
        if not name or not name.lower().endswith(".zip"):
            continue
        if prefix_filter and not name.lower().startswith(prefix_filter.lower()):
            continue
        out.append(CNPJFile(url=base + name, name=name))
    logger.info(f"Arquivos listados para {month} ({prefix_filter or 'todos'}): {len(out)}")
    return out


def download_files(files: Iterable[CNPJFile], dest_dir: Path, overwrite: bool = False) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        for f in files:
            out = dest_dir / f.name
            if out.exists() and not overwrite:
                logger.info(f"Já existe (skip): {out}")
                saved.append(out)
                continue
            logger.info(f"Baixando: {f.url}")
            with client.stream("GET", f.url) as r:
                r.raise_for_status()
                with open(out, "wb") as w:
                    for chunk in r.iter_bytes():
                        w.write(chunk)
            saved.append(out)
    return saved
