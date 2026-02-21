from app.core.config import get_settings
import duckdb
import threading
import json
import os
import tempfile
import logging

settings = get_settings()
logger = logging.getLogger(__name__)

class QueryEngine:
    def __init__(self):
        # Lazy initialization: não conecta nem instala extensões no boot
        self.con = None
        self.lock = threading.Lock()
        
    def _get_connection(self):
        """
        Garante que a conexão existe e está configurada.
        Se não estiver, cria e configura.
        """
        if self.con is not None:
            return self.con
            
        try:
            logger.info("Iniciando conexão DuckDB...")
            # Configura diretório temporário para extensões (Vercel/Lambda são read-only exceto /tmp)
            temp_dir = tempfile.gettempdir()
            logger.info(f"Usando diretório temporário: {temp_dir}")
            
            # Conecta em memória com configurações específicas
            self.con = duckdb.connect(database=':memory:', config={
                'home_directory': temp_dir,
                'extension_directory': os.path.join(temp_dir, 'duckdb_extensions')
            })
            
            logger.info("Conexão criada. Instalando extensões...")
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
            self.con.execute("INSTALL aws;")
            self.con.execute("LOAD aws;")
            logger.info("Extensões instaladas e carregadas.")
            
            # Tratamento do endpoint
            endpoint = settings.SUPABASE_S3_ENDPOINT.replace("https://", "").replace("http://", "")
            if endpoint.endswith("/"):
                endpoint = endpoint[:-1]
                
            logger.info(f"Configurando S3 com endpoint: {endpoint}")
            
            query = f"""
            SET s3_region='{settings.AWS_DEFAULT_REGION}';
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{settings.AWS_ACCESS_KEY_ID}';
            SET s3_secret_access_key='{settings.AWS_SECRET_ACCESS_KEY}';
            SET s3_url_style='path';
            SET s3_use_ssl=true;
            """
            self.con.execute(query)
            logger.info("Configuração S3 concluída.")
            
            return self.con
            
        except Exception as e:
            logger.error(f"ERRO CRÍTICO ao inicializar DuckDB: {e}")
            # Se falhar, reseta a conexão para tentar de novo na próxima chamada
            self.con = None
            raise e

    def get_parquet_data(self, filename: str, limit: int = 100):
        """
        Lê um arquivo parquet do bucket S3 e retorna como lista de dicionários.
        """
        try:
            con = self._get_connection()
            s3_path = f"s3://{settings.SUPABASE_BUCKET}/{filename}"
            
            query = f"""
            SELECT * 
            FROM read_parquet('{s3_path}')
            LIMIT {limit}
            """
            
            with self.lock:
                df = con.execute(query).df()
                return json.loads(df.to_json(orient="records", date_format="iso"))
        except Exception as e:
            logger.error(f"Erro em get_parquet_data: {e}")
            raise e

    def execute_custom_query(self, sql: str):
        """
        Permite executar SQL arbitrário (com cuidado!)
        """
        try:
            con = self._get_connection()
            with self.lock:
                df = con.execute(sql).df()
                return json.loads(df.to_json(orient="records", date_format="iso"))
        except Exception as e:
            logger.error(f"Erro em execute_custom_query: {e}")
            raise e

    def list_tables(self):
        """
        Lista todos os arquivos .parquet no bucket S3.
        """
        try:
            con = self._get_connection()
            s3_path = f"s3://{settings.SUPABASE_BUCKET}/*.parquet"
            query = f"SELECT file FROM glob('{s3_path}')"
            
            with self.lock:
                # O glob do DuckDB retorna o caminho completo
                df = con.execute(query).df()
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
            logger.error(f"Erro ao listar tabelas: {e}")
            # Propaga o erro para que o FastAPI retorne 500 e vejamos no log
            raise e

# Instância global (Singleton simples)
query_engine = QueryEngine()
