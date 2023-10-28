from minio_access import Client

bucket_name = 'bronze'
local_file = 'data/vendas-combustiveis-m3.xls'
object_name = 'vendas-combustiveis-m3.xls'

with Client() as client:
    try:
        if len(list(client.list_objects(bucket_name, recursive=True))) == 0:
            client.fput_object(bucket_name, object_name, local_file)
            print(f"Arquivo '{object_name}' enviado para o bucket '{bucket_name}' com sucesso!")
        else:
            print(f"Arquivo '{object_name}' já existe no bucket '{bucket_name}' e não foi enviado novamente.")
    except Exception as e:
        print(f"Ocorreu um erro ao enviar o arquivo: {e}")