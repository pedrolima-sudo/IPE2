"""
MÓDULO DE UTILITÁRIOS PARA LIMPEZA E NORMALIZAÇÃO DE DADOS
=============================================================

o módulo clean.py fornece funções para limpeza e normalização de strings, como nomes.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

import re
from unidecode import unidecode


def normalize_name(name: str) -> str:
    if name is None:
        return ""
    s = unidecode(str(name)).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()
