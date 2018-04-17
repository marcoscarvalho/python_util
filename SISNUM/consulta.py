# coding: utf-8

import pandas as pd
import numpy as np
import cx_Oracle

user = 'number_inventory_prod'
password = 'gvt25gvt'
db_alias = '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=svcpnum_inst1))(ADDRESS=(PROTOCOL=TCP)(HOST=10.41.14.52)(PORT=1521)))'

def execute_select(df):
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()

        contador = 0 
        for index, row in df.iterrows():

            if contador > 1:
                return df
            
            linha = row[index]
            cur.execute(linha)
            
            for result in cur:
                print(result)

            contador += 1

    except Exception as e:
        print('Erro ao executar execute_select', e)

df1 = pd.read_csv('C:\\GIT\\python_util\\SISNUM\\SELECT.csv')
df = execute_select(df1)

#df.to_csv('resultado.csv', sep=';', encoding='utf-8')