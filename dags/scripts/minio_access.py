from minio import Minio
from os import getenv

class Client:
    def __init__(self) -> None:
        self.version = 'v1'

    def __enter__(self):
        self.client = Minio(
            'minio:9000',
            access_key=getenv('MINIO_ROOT_USER'),
            secret_key=getenv('MINIO_ROOT_PASSWORD'),
            secure=False
        )
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        pass
        