import logging
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from app.core.config import get_settings
from app.core.security import get_api_key
from app.services.query_engine import query_engine
from app.services.storage_service import storage_service
from fastapi.responses import RedirectResponse
import duckdb

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

settings = get_settings()

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description="API Lakehouse para consulta de arquivos Parquet armazenados no Supabase S3"
)

@app.get("/", include_in_schema=False)
def health_check():
    return {"status": "ok", "service": "parquet-s3-api"}

@app.get("/api/v1/tables", dependencies=[Depends(get_api_key)])
def list_tables():
    """
    Lista todas as tabelas (arquivos .parquet) disponíveis no bucket.
    """
    try:
        tables = query_engine.list_tables()
        return {"tables": tables, "count": len(tables)}
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/data/{table_name}", dependencies=[Depends(get_api_key)])
def get_table_data(table_name: str, limit: int = 100):
    """
    Retorna os dados de uma tabela (arquivo parquet) do S3.
    
    Args:
        table_name (str): Nome da tabela/arquivo (sem extensão .parquet)
        limit (int): Limite de registros (default: 100)
    """
    try:
        filename = f"{table_name}.parquet"
        logger.info(f"Consultando arquivo: {filename} com limit {limit}")
        
        # Chama a engine DuckDB
        data = query_engine.get_parquet_data(filename, limit)
        
        return {
            "table": table_name,
            "count": len(data),
            "data": data
        }
    except duckdb.IOException as e:
        logger.error(f"Erro de I/O (arquivo não encontrado ou erro S3): {e}")
        raise HTTPException(status_code=404, detail=f"Tabela '{table_name}' não encontrada ou erro de acesso ao S3.")
    except Exception as e:
        logger.error(f"Erro interno: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/download/{table_name}", dependencies=[Depends(get_api_key)])
def download_table(table_name: str):
    """
    Gera uma URL temporária (Presigned URL) para download direto do arquivo Parquet.
    Ideal para ferramentas como Power BI ou para baixar grandes volumes de dados.
    
    Args:
        table_name (str): Nome da tabela/arquivo (sem extensão .parquet)
    """
    try:
        url = storage_service.generate_presigned_url(table_name)
        if not url:
            raise HTTPException(status_code=500, detail="Erro ao gerar URL de download")
            
        # Redireciona o cliente diretamente para a URL do S3
        return RedirectResponse(url=url)
    except Exception as e:
        logger.error(f"Erro ao gerar download: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=settings.PORT, reload=True)
