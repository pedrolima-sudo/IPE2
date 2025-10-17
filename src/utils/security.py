"""
MÓDULO DE UTILITÁRIOS DE SEGURANÇA
=================================

O módulo security.py fornece funções para operações de segurança, como hashing de identificadores.

Autor: Pedro Henrique Lima Silva
Data de criação: 15/10/2025
Última modificação: 16/10/2025
"""

import hmac
import hashlib


def hash_identifier(value: str, salt: str) -> str:
    """Gera um hash HMAC-SHA256 estável para identificar a pessoa sem expor CPF.
    - value: string original (ex.: CPF sem máscara)
    - salt: segredo do .env (CPF_SALT)
    """
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""
    return hmac.new(salt.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
