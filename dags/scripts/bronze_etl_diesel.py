from scripts.minio_access import Client
from scripts.utils import *
from scripts.transformation import *
from datetime import datetime
from pytz import timezone

def bronze_etl_diesel(sbn,dbn, obj):
    """
        sbn = Nome do bucket de origem (sbn = source bucket name)
        dbn = Nome do bucket de destino (dbn = dest bucket name)
        obj = Nome do objeto de origem/destino(obj = object name)
    """
    with Client() as client:
        
        response = client.get_object(sbn, obj)
        
        df = read_object_csv(response)
        df = rename_columns(df, new_columns = 'diesel')
        df = transpose_columns(df,
                               ['product','year','region','uf','unit','total'], 
                               ['JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ'],
                               'month',
                               'volume')
        df = add_month_number_column(df)
        df = title_columns(df)
        df = add_lit_column(df, column_name='created_at', value=datetime.now(timezone('America/Sao_Paulo')))
        df = concat_columns(df, 'year_month', 'year', 'month_number')
        df = replace_values(df, 'volume', ',', '.')
        df = replace_values(df, 'volume', ' -   ', '0')
        df = replace_values(df, 'product', ' (M3)', '')
        df = cast_type(df)
        df = df[['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']]
        
        write_object_csv(client,
                          bucket_name=dbn,
                          csv_file_name=obj,
                          df=df)