from minio_access import Client
from utils import *

source_bucket_name = 'landing-zone'
source_object_name = 'vendas-combustiveis-m3.xls'

dest_bucket_name = 'bronze'
dest_object_name = 'diesel.xlsx'

with Client() as client:
    
    response = client.get_object(source_bucket_name, source_object_name)
    df = read_object_xls(response,
                         line_start=131,
                         line_end=145,
                         col_init=1,
                         col_end=11)
    
    write_object_xlsx(client,
                      bucket_name=dest_bucket_name,
                      excel_file_name=dest_object_name,
                      df=df)