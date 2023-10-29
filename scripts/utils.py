import pandas as pd
import io

def read_object_xls(response:object,
                    line_start:int, 
                    line_end:int, 
                    col_init:int, 
                    col_end:int)-> pd.DataFrame:
    xls_content = response.read()
    df = pd.read_excel(io.BytesIO(xls_content))
    df = df.iloc[line_start:line_end, col_init:col_end]
    new_header = df.iloc[0]
    df = df[1:]
    df.columns = new_header
    df = df.fillna('')
    return df

def read_object_xlsx(response:object)-> pd.DataFrame:
    xls_content = response.read()
    df = pd.read_excel(io.BytesIO(xls_content))
    return df

def write_object_xlsx(client:object,
                      bucket_name:str,
                      excel_file_name:str,
                      df : pd.DataFrame):
    excel_buffer = io.BytesIO()
    df.to_excel(excel_buffer, index=False)
    excel_buffer.seek(0)
    client.put_object(bucket_name, 
                      excel_file_name,
                      excel_buffer,
                      len(excel_buffer.getvalue()),
                      content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    print(f'Arquivo excel {excel_file_name} enviado com sucesso')