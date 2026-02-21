# Parquet S3 API (Lakehouse)

Uma API REST de alta performance construída com **FastAPI** e **DuckDB** para consultar dados armazenados em arquivos Parquet no Supabase Storage (S3).

Este projeto exemplifica uma arquitetura **Lakehouse leve**, onde os dados são processados e salvos como arquivos Parquet no S3 (via ETL externo, como Kestra), e esta API serve como camada de consulta (Query Layer) desacoplada e eficiente.

## 🚀 Tecnologias

- **[FastAPI](https://fastapi.tiangolo.com/)**: Framework web moderno e rápido para construção de APIs.
- **[DuckDB](https://duckdb.org/)**: Banco de dados OLAP em processo, utilizado aqui para ler arquivos Parquet diretamente do S3 via streaming (sem download prévio).
- **[Pydantic](https://docs.pydantic.dev/)**: Validação de dados e gerenciamento de configurações.
- **Supabase Storage (S3 Compatible)**: Armazenamento de objetos onde residem os arquivos Parquet.

## 🏗 Arquitetura

1.  **ETL (Kestra)**: Extrai dados do Postgres, converte para Parquet e faz upload para o Bucket S3 do Supabase.
2.  **Storage (S3)**: Armazena os dados históricos/processados em formato colunar (Parquet).
3.  **API (FastAPI + DuckDB)**:
    *   Recebe requisições HTTP (ex: `GET /api/v1/data/dados_positivador`).
    *   DuckDB conecta-se ao S3 e lê apenas as colunas/linhas necessárias (Predicate Pushdown).
    *   Retorna os dados em JSON para o cliente.

## 🛠 Configuração

### Pré-requisitos
- Python 3.10+
- Um bucket no Supabase (ou qualquer S3 compatible) com arquivos `.parquet`.

### 1. Clonar e Instalar Dependências

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/parquet-s3-api.git
cd parquet-s3-api

# Crie um ambiente virtual
python -m venv venv

# Ative o ambiente virtual
# Windows:
.\venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

### 2. Variáveis de Ambiente

Copie o arquivo `.env.example` para `.env` e preencha com suas credenciais:

```bash
cp .env.example .env
```

Edite o arquivo `.env`:

```ini
API_TITLE=Parquet S3 API
PORT=8000

# Credenciais do Supabase Storage (S3)
AWS_ACCESS_KEY_ID=sua_access_key_id
AWS_SECRET_ACCESS_KEY=sua_secret_key_access
AWS_DEFAULT_REGION=us-east-1
SUPABASE_S3_ENDPOINT=https://seu-projeto.supabase.co/storage/v1/s3
SUPABASE_BUCKET=nome-do-seu-bucket
```

## ▶️ Executando

Para rodar o servidor de desenvolvimento:

```bash
python -m app.main
```

A API estará disponível em `http://localhost:8000`.

## 📚 Documentação (Swagger UI)

Acesse `http://localhost:8000/docs` para ver a documentação interativa automática gerada pelo FastAPI.

## 🔍 Endpoints Principais

### `GET /api/v1/data/{table_name}`

Consulta um arquivo Parquet no bucket. O nome do arquivo deve corresponder ao `{table_name}.parquet`.

**Parâmetros:**
- `table_name`: Nome do arquivo (sem a extensão .parquet). Ex: `dados_positivador`.
- `limit`: (Opcional) Número máximo de registros a retornar. Default: 100.

**Exemplo de Resposta:**

```json
{
  "table": "dados_positivador",
  "count": 100,
  "data": [
    {
      "id": 1,
      "nome": "Cliente A",
      "data_cadastro": "2024-01-01",
      "valor": 150.50
    },
    ...
  ]
}
```

## 🧠 Por que DuckDB + Parquet?

*   **Velocidade**: Parquet é um formato colunar binário altamente comprimido. DuckDB lê esse formato nativamente de forma vetorizada.
*   **Eficiência de Rede**: O DuckDB não baixa o arquivo inteiro do S3. Ele lê apenas os metadados (cabeçalho) e depois faz requisições HTTP Range apenas para os blocos de dados necessários para a query.
*   **Custo**: Armazenar dados no S3 é muito mais barato que manter um Data Warehouse ligado 24/7.

---

Desenvolvido com 💙
