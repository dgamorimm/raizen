from minio_access import Client
from utils import *
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Capturando dados da ANP e armazenando na Landing Zone Layer'
    )
    parser.add_argument('--sbn', required=True, help='Nome do bucket de origem (sbn = source bucket name)')
    parser.add_argument('--son', required=True, help='Nome do objeto que sera armazenado no bucket de origem (son = source object name)')
    parser.add_argument('--src', required=True, help='Caminho da fonte do arquivo (src = source)')
    
    args = parser.parse_args()
    
    with Client() as client:
        
        df = read_object_csv(args.src)
        
        write_object_csv(client,
                          bucket_name=args.sbn,
                          csv_file_name=args.son,
                          df=df)
        # Oil: 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-combustiveis-m3-2000-2023.csv'
        # Diesel: 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vendas-oleo-diesel-tipo-m3-2020.csv'
