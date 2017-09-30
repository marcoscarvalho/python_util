# coding: utf-8

import pandas as pd
import numpy as np
import cx_Oracle
from domain import DataTable
import csv
import pandasql

siebel_user = 'sbleim'
siebel_pass = 'sbleim'
siebel_db_alias = 'CSIE8'

siebel_select_main = '''
select m.terminal, m.documento, sap.x_cnl cnl, sap.x_cnl_code cnl_code, sap.city cidade, sap.state es
  from siebel81_owner.mgr_leg m,
       siebel.s_asset         sa,
       siebel.s_asset_om      som,
       siebel.s_addr_per      sap
 where m.terminal = sa.serial_num
   and cenario = 'LIRT'
   and sa.status_cd = 'Ativo'
   and sa.row_id = som.par_row_id
   and som.from_addr_id = sap.row_id
   and processado <> 'OK'
'''

def execute_select_main(user, password, db_alias, select):
    tabela = ['terminal', 'documento', 'cnl', 'cnl_code', 'cidade', 'es']
    lst = []
	
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for terminal, documento, cnl, cnl_code, cidade, es in cur:
            lst.append([terminal, documento, cnl, cnl_code, cidade, es])

    except Exception as e:
        print('Erro ao executar select', e)
	
    return pd.DataFrame(lst, columns=tabela)

df = execute_select_main(siebel_user, siebel_pass, siebel_db_alias, siebel_select_main)
df.to_csv('LIRTComProblemas.csv', sep=';', encoding='utf-8')