import pandas as pd
import io

def read_object_csv(response:str | object)-> pd.DataFrame:
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