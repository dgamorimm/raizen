import pandas as pd
import io

def read_object_csv(response)-> pd.DataFrame:
    if type(response) == str:
        return pd.read_csv(response, sep=';')
    else:
        csv_content = response.read()
        df = pd.read_csv(io.BytesIO(csv_content), sep=';')
        return df

def read_object_xlsx(response:object)-> pd.DataFrame:
    xls_content = response.read()
    df = pd.read_excel(io.BytesIO(xls_content))
    return df

def write_object_csv(client:object,
                      bucket_name:str,
                      csv_file_name:str,
                      df : pd.DataFrame):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, sep=';')
    csv_buffer.seek(0)
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    client.put_object(bucket_name, 
                      csv_file_name.replace('.csv', '') + '.csv', 
                      io.BytesIO(csv_bytes), 
                      len(csv_bytes))
    
    print(f'Arquivo CSV {csv_file_name} enviado com sucesso')

def write_object_xlsx(client:object,
                      bucket_name:str,
                      excel_file_name:str,
                      df : pd.DataFrame):
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    client.put_object(bucket_name, 
                      excel_file_name + '.xlsx',
                      excel_buffer,
                      len(excel_buffer.getvalue()),
                      content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print(f'Arquivo excel {excel_file_name} enviado com sucesso')
    
def write_partitions_csv(client:object,
                        bucket_name:str,
                        partition_name:str,
                        df : pd.DataFrame):
    
    df['year_month'] = pd.to_datetime(df['year_month'])
    df['year'] = df['year_month'].dt.year
    df['month'] = df['year_month'].dt.strftime('%m')  # Extrai o mês como uma string de 2 dígitos
    df['day'] = df['year_month'].dt.strftime('%d')
    
    
    df.reset_index(inplace=True)
   
    # escrevendo as partições
    i = 1
    for _, group_df in  df.groupby(['year', 'month', 'day']):
        year, month, day = group_df['year'].values[0], group_df['month'].values[0], group_df['day'].values[0]
        partition_path = f'{partition_name}/{year}/{month}/{day}'
        
        data = io.BytesIO(group_df.to_csv(index=False).encode())
        
        client.put_object(bucket_name,
                        f'{partition_path}/part-000{i}.csv',
                        data,
                        len(data.getvalue()),
                        content_type='text/csv')
        i += 1
    
    # adicionando o indice
    df.set_index('year', inplace=True)
    df.set_index('month', inplace=True)
    df.set_index('day', inplace=True)
    
    print("Particoes foram escritas com sucesso!")