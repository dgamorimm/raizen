from minio_access import Client
from utils import *
from transformation import *
from datetime import datetime
from pytz import timezone
from rich import print
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Tranformacao da Landing Zone Layer para a Bronze Layer'
    )
    parser.add_argument('--sbn', required=True, help='Nome do bucket de origem (sbn = source bucket name)')
    parser.add_argument('--dbn', required=True, help='Nome do bucket de destino (dbn = dest bucket name)')
    parser.add_argument('--obj', required=True, help='Nome do objeto de origem/destino(obj = object name)')
    
    
    args = parser.parse_args()

    with Client() as client:
        
        response = client.get_object(args.sbn, args.obj)
        
        df = read_object_csv(response)
        df = rename_columns(df, new_columns = 'oil-derivative-fuels')
        df = add_month_number_column(df)
        df = title_columns(df)
        filter_1 = ((df['year'] != 2020) | ((df['year'] == 2020) & ~df['month'].isin(['Oct', 'Nov', 'Dec'])))
        filter_2 = ((df['year'] >= 2000) & ((df['year'] <= 2020)))
        df = filter_column(df, _filter=filter_1)
        df = filter_column(df, _filter=filter_2)
        df = add_lit_column(df, column_name='unit', value='m3')
        df = add_lit_column(df, column_name='created_at', value=datetime.now(timezone('America/Sao_Paulo')))
        df = concat_columns(df, 'year_month', 'year', 'month_number')
        df = replace_values(df, 'volume', ',', '.')
        df = cast_type(df)
        df = df[['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']]
        
        write_object_csv(client,
                          bucket_name=args.dbn,
                          csv_file_name=args.obj,
                          df=df)