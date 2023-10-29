from minio import Minio

class Client:
    def __init__(self) -> None:
        self.version = 'v1'

    def __enter__(self):
        self.client = Minio(
            'localhost:9000',
            access_key='raizen',
            secret_key='Raizen#Pass#2023',
            secure=False
        )
        return self.client

    def __exit__(self, exc_type, exc_value, traceback):
        pass
        