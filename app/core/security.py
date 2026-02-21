from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader
from app.core.config import get_settings

settings = get_settings()

# Define o nome do header que deve conter o token
API_KEY_NAME = "x-api-token"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    """
    Verifica se o token recebido no header 'x-api-token' corresponde ao token configurado.
    """
    if api_key_header == settings.API_ACCESS_TOKEN:
        return api_key_header
    
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Token de acesso inválido ou ausente."
    )
