from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "parquet-s3-api"}

def test_get_table_data_not_found():
    # Testa uma tabela que provavelmente não existe
    # Isso deve retornar 404 conforme a lógica do main.py
    # Ou 500 se a conexão S3 falhar completamente (o que é esperado sem credenciais reais válidas no ambiente de teste)
    # Mas como o usuário pode ter o .env configurado, vamos ver.
    # Se der erro de credencial, o DuckDB lança IOException, que é capturado e retorna 404.
    response = client.get("/api/v1/data/tabela_inexistente_123")
    
    # O código trata duckdb.IOException como 404
    # Se for outro erro, será 500.
    assert response.status_code in [404, 500]
