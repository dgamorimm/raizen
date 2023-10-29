from minio_access import Client
from utils import write_partitions_csv, read_object_csv
from transformation import *
from datetime import datetime
from pytz import timezone
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Tranformacao da Landing Zone Layer para a Bronze Layer'
    )
    parser.add_argument('--sbn', required=True, help='Nome do bucket de origem (sbn = source bucket name)')
    parser.add_argument('--dbn', required=True, help='Nome do bucket de destino (dbn = dest bucket name)')
    parser.add_argument('--part', required=True, help='Nome da particao (part = particao)')
    parser.add_argument('--obj', required=True, help='Nome do objeto de origem/destino(obj = object name)')
    
    
    args = parser.parse_args()

    with Client() as client:
        
        response = client.get_object(args.sbn, args.obj)
        
        df = read_object_csv(response)
        df = remove_column(df, 'created_at')
        df = add_lit_column(df, column_name='created_at', value=datetime.now(timezone('America/Sao_Paulo')))
        
        write_partitions_csv(client,
                             bucket_name=args.dbn,
                             partition_name=args.part,
                             df=df)