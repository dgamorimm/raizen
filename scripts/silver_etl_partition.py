from minio_access import Client
from utils import write_partitions_csv, read_object_csv
from transformation import *
from datetime import datetime
from pytz import timezone

def silver_etl_partition(sbn, obj, dbn, part):
    """
        sbn = Nome do bucket de origem (sbn = source bucket name)
        dbn = Nome do bucket de destino (dbn = dest bucket name)
        part = Nome da particao (part = particao)
        obj = Nome do objeto de origem/destino(obj = object name)
    """
    with Client() as client:
        
        response = client.get_object(sbn, obj)
        
        df = read_object_csv(response)
        df = remove_column(df, 'created_at')
        df = add_lit_column(df, column_name='created_at', value=datetime.now(timezone('America/Sao_Paulo')))
        
        write_partitions_csv(client,
                             bucket_name=dbn,
                             partition_name=part,
                             df=df)