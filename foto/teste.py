import pandas as pd
import numpy as np
import cx_Oracle

ipunif_user = 'PROFILING_OWNER'
ipunif_pass = 'ProfilingVivo123'
ipunif_db_alias = '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=IPUNIF))(ADDRESS=(PROTOCOL=TCP)(HOST=vipscancrs033)(PORT=1521)))'


def execute_select(nrc):
    header = ['designator_value', 'external_address_id', 'account_id', 'designator_id']
    lst = []

    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        return pd.read_sql('''
				select * 
				  from profiling_owner.servicos_ativos_20171108 
				 where nrc = :p01_nrc''', 
				 con=con2, params={'p01_nrc': nrc})

    except Exception as e:
        print('Erro ao executar select', nrc, e)

    return None

df = execute_select('8812683051')
print(df)