from scripts.minio_access import Client

def create_bucket():
    bucket_names = ["landing-zone","bronze", "silver"]

    with Client() as client:
        for bucket_name in bucket_names:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso!")
            else:
                print(f"Bucket '{bucket_name}' já existe.")