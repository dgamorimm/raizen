from minio_access import Client
from utils import *

def load_landing_zone(sbn, son, src):
    
    """ 
        sbn = Nome do bucket de origem (sbn = source bucket name)
        son = Nome do objeto que sera armazenado no bucket de origem (son = source object name)
        src = Caminho da fonte do arquivo (src = source)
        
        Oil: 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-2000-2023.csv'
        Diesel: 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-oleo-diesel-tipo-m3-2020.csv'
    """
    
    with Client() as client:
        
        df = read_object_csv(src)
        
        write_object_csv(client,
                          bucket_name=sbn,
                          csv_file_name=son,
                          df=df)