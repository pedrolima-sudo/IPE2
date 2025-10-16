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
