from minio_access import Client
from utils import *
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Tranformacao da Landing Zone Layer para a Bronze Layer'
    )
    parser.add_argument('--sbn', required=True, help='Nome do bucket de origem (sbn = source bucket name)')
    parser.add_argument('--son', required=True, help='Nome do objeto que que esta no bucket de origem (son = source object name)')
    parser.add_argument('--dbn', required=True, help='Nome do bucket de destino (dbn = dest bucket name)')
    parser.add_argument('--don', required=True, help='Nome do objeto que sera armazenado no bucket de destino (don = dest object name)')
    parser.add_argument('--line-start', required=True, help='Numero inteiro que indica o indice do inicio da linha')
    parser.add_argument('--line-end', required=True, help='Numero inteiro que indica o indice do final da linha')
    parser.add_argument('--col-start', required=True, help='Numero inteiro que indica o indice do inicio da coluna')
    parser.add_argument('--col-end', required=True, help='Numero inteiro que indica o indice do final da coluna')
    
    args = parser.parse_args()

    with Client() as client:
        
        response = client.get_object(args.sbn, args.son)
        df = read_object_xls(response,
                            line_start=args.line_start,
                            line_end=args.line_end,
                            col_start=args.col_start,
                            col_end=args.col_end)
        
        write_object_xlsx(client,
                          bucket_name=args.dbn,
                          excel_file_name=args.don,
                          df=df)