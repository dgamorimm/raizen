from minio_access import Client
from utils import *
from transformation import *
from datetime import datetime
from pytz import timezone

def bronze_etl_oil_dev_fuels(sbn,dbn, obj):
    """
        sbn = Nome do bucket de origem (sbn = source bucket name)
        dbn = Nome do bucket de destino (dbn = dest bucket name)
        obj = Nome do objeto de origem/destino(obj = object name)
    """
    with Client() as client:
        
        response = client.get_object(sbn, obj)
        
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
                          bucket_name=dbn,
                          csv_file_name=obj,
                          df=df)