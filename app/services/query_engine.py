from app.core.config import get_settings
import duckdb
import threading
import json

settings = get_settings()

class QueryEngine:
    def __init__(self):
        # Conecta em memória (DuckDB é muito leve)
        self.con = duckdb.connect(database=':memory:')
        self.lock = threading.Lock()
        self._setup_s3()

    def _setup_s3(self):
        """
        Configura as credenciais e extensões do DuckDB para acessar S3
        """
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")
        self.con.execute("INSTALL aws;")
        self.con.execute("LOAD aws;")
        
        # Tratamento do endpoint (DuckDB não gosta de 'https://' no s3_endpoint em algumas versões)
        # O S3 Endpoint do Supabase geralmente é algo como: region.project.supabase.co
        # DuckDB prefere o estilo path ou vhost. Para S3 compatible, path style costuma ser mais seguro.
        
        endpoint = settings.SUPABASE_S3_ENDPOINT.replace("https://", "").replace("http://", "")
        if endpoint.endswith("/"):
            endpoint = endpoint[:-1]
            
        # IMPORTANTE: DuckDB tem configurações específicas para S3 Compatible (MinIO, Supabase, R2)
        # s3_url_style='path' é crucial para Supabase Storage
        query = f"""
        SET s3_region='{settings.AWS_DEFAULT_REGION}';
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{settings.AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{settings.AWS_SECRET_ACCESS_KEY}';
        SET s3_url_style='path';
        SET s3_use_ssl=true;
        """
        self.con.execute(query)

    def get_parquet_data(self, filename: str, limit: int = 100):
        """
        Lê um arquivo parquet do bucket S3 e retorna como lista de dicionários.
        """
        # Constrói o caminho s3://bucket/arquivo
        # Nota: s3:// é o protocolo padrão do DuckDB para ler de S3
        s3_path = f"s3://{settings.SUPABASE_BUCKET}/{filename}"
        
        # Query SQL direta no arquivo Parquet (Zero-Copy)
        query = f"""
        SELECT * 
        FROM read_parquet('{s3_path}')
        LIMIT {limit}
        """
        
        # Retorna resultado como lista de dicts (JSON-friendly)
        # .df() converte pra pandas
        # Usamos to_json() -> json.loads() para garantir que Timestamps e NaNs sejam serializados corretamente
        with self.lock:
            df = self.con.execute(query).df()
            return json.loads(df.to_json(orient="records", date_format="iso"))

    def execute_custom_query(self, sql: str):
        """
        Permite executar SQL arbitrário (com cuidado!)
        """
        with self.lock:
            df = self.con.execute(sql).df()
            return json.loads(df.to_json(orient="records", date_format="iso"))

    def list_tables(self):
        """
        Lista todos os arquivos .parquet no bucket S3.
        """
        s3_path = f"s3://{settings.SUPABASE_BUCKET}/*.parquet"
        # O glob retorna a coluna 'file' com o caminho completo
        query = f"SELECT file FROM glob('{s3_path}')"
        
        with self.lock:
            try:
                # O glob do DuckDB retorna o caminho completo
                # Ex: s3://bucket/arquivo.parquet
                df = self.con.execute(query).df()
                if df.empty:
                    return []
                
                # Extrai o nome do arquivo da coluna 'file'
                files = df['file'].tolist()
                tables = []
                for f in files:
                    # Normaliza barras
                    f = f.replace('\\', '/')
                    name = f.split('/')[-1]
                    if name.endswith('.parquet'):
                        tables.append(name[:-8]) # Remove .parquet
                return tables
            except Exception as e:
                # Se der erro (ex: bucket vazio ou erro de permissão), retorna lista vazia ou propaga log
                print(f"Erro ao listar tabelas: {e}")
                return []

# Instância global (Singleton simples)
query_engine = QueryEngine()
