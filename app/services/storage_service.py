import boto3
from botocore.exceptions import ClientError
from app.core.config import get_settings
import logging

settings = get_settings()
logger = logging.getLogger(__name__)

class StorageService:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            endpoint_url=settings.SUPABASE_S3_ENDPOINT,
            region_name=settings.AWS_DEFAULT_REGION,
            config=boto3.session.Config(signature_version='s3v4')
        )
        self.bucket_name = settings.SUPABASE_BUCKET

    def generate_presigned_url(self, object_name: str, expiration: int = 3600):
        """
        Gera uma URL pré-assinada para download direto do arquivo Parquet.
        
        Args:
            object_name (str): Nome do arquivo no bucket (ex: 'dados.parquet')
            expiration (int): Tempo de expiração em segundos (default: 1 hora)
            
        Returns:
            str: URL pré-assinada ou None se houver erro
        """
        try:
            # Garante que o nome do arquivo termina com .parquet
            if not object_name.endswith('.parquet'):
                object_name = f"{object_name}.parquet"
                
            response = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': object_name
                },
                ExpiresIn=expiration
            )
            return response
        except ClientError as e:
            logger.error(f"Erro ao gerar URL assinada: {e}")
            return None

storage_service = StorageService()