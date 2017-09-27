# coding: utf-8

import pandas as pd
import numpy as np
import cx_Oracle
from domain import DataTable
import csv
import pandasql

col_user = 'MIG_CONF_ONLINE'
col_pass = 'gvt2000'
col_db_alias = 'SVCPCONF'

is_user = 'service_inventory_owner'
is_pass = 'service_inventory_owner'
is_db_alias = 'SVCPCONF'

col_select_equipamentos = '''
select modelo, id_recurso_logico_spec, id_modelo_equipamento from conf_online_owner.modelo_equipamento me
'''

col_select_main = '''
select ip.designador,
       ip.crm_origem,
       hg.serial_home_gateway,
       hg.macaddress_home_gateway,
       hg.id_modelo_equipamento   hg_modelo_equipamento,
       rl.id_recurso_logico_spec,
       ef.id_modelo_equipamento   ef_modelo_equipamento,
       ef.serial,
       ef.mac_address,
	   hg.id id_hg
	   , ef.id id_ef
  from conf_online_owner.instancia_produto  ip,
       conf_online_owner.instancia_servico  iss,
       conf_online_owner.home_gateway       hg,
       conf_online_owner.recurso_logico     rl,
       conf_online_owner.equipamento_fisico ef
 where ip.id = iss.id_instancia_produto
   and iss.id = hg.id_instancia_servico
   and iss.id = rl.id_instancia_servico
   and rl.id_equipamento_fisico = ef.id
   and iss.id_status = 2
   and hg.id_status = 2
   and rl.id_status_servico = 2
   and ip.crm_origem = 'MIGRACAO'
   and iss.create_date > sysdate - 30
'''

is_select_rpon = '''
select d.designator_value designador, ip.param_value rpon
  from service_inventory_owner.gvt_inv_designator d,
       service_inventory_owner.gvt_inv_designator dl,
       service_inventory_owner.gvt_inv_item       i,
       service_inventory_owner.gvt_inv_item_param ip
 where d.status_id in (1, 2)
   and d.designator_type_id = 3
   and d.parent_designator = dl.parent_designator
   and dl.status_id in (1, 2)
   and i.status_id in (1, 2)
   and dl.designator_type_id = 2
   and i.item_spec_id = 3
   and dl.designator_id = i.designator_id
   and i.item_id = ip.item_id
   and ip.param_id = 21
   and d.designator_value = :desig
'''

def execute_select_equip(user, password, db_alias, select):
    cols_equipamento = ['modelo', 'id_recurso_logico_spec', 'id_modelo_equipamento']
    lst = []
	
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for modelo, id_recurso_logico_spec, id_modelo_equipamento in cur:
            lst.append([modelo, id_recurso_logico_spec, id_modelo_equipamento])

    except Exception as e:
        print('Erro ao executar select', e)
	
    return pd.DataFrame(lst, columns=cols_equipamento)
		
def execute_select(user, password, db_alias, select):
    cols_cliente = ['designador', 'crm_origem', 'serial_home_gateway', 'macaddress_home_gateway', 'hg_modelo_equipamento', 'id_recurso_logico_spec', 'ef_modelo_equipamento', 'serial', 'mac_address', 'rpon', 'id_hg', 'id_ef', 'processado_ef', 'processado_hg', 'mac_acs', 'serial_acs', 'equip_acs']
    lst = []
	
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for designador, crm_origem, serial_home_gateway, macaddress_home_gateway, hg_modelo_equipamento, id_recurso_logico_spec, ef_modelo_equipamento, serial, mac_address, id_hg, id_ef in cur:
            lst.append([designador, crm_origem, serial_home_gateway, macaddress_home_gateway, hg_modelo_equipamento, id_recurso_logico_spec, ef_modelo_equipamento, serial, mac_address, None, id_hg, id_ef, None, None, None, None, None])

    except Exception as e:
        print('Erro ao executar select', e)
	
    return pd.DataFrame(lst, columns=cols_cliente)

def execute_update(user, password, db_alias, update):
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(update)
        con.commit()
        return True

    except Exception as e:
        print('Erro ao executar execute_update', e)
        return False
		
def execute_select2(user, password, db_alias, select, df):
    try:
        for index, row in df.iterrows():
            con = cx_Oracle.connect(user, password, db_alias)
            cur = con.cursor()
            designador = row['designador']
            cur.execute(select, desig=designador)

            for designador, rpon in cur:
                df.at[index, 'rpon'] = rpon

    except Exception as e:
        print('Erro ao executar execute_select2', e)

dfacs = pd.read_csv('massa/BaseACSFibra-20170926.csv', encoding='ISO-8859-1', sep=';', dtype=str)
dfacs['NRC'] = dfacs['NRC'].fillna('nulo')

#dfeqp = execute_select_equip(col_user, col_pass, col_db_alias, col_select_equipamentos)
dfcol = execute_select(col_user, col_pass, col_db_alias, col_select_main)
execute_select2(is_user, is_pass, is_db_alias, is_select_rpon, dfcol)

#print(dfeqp)

for index, row in dfcol.iterrows():
    serial = row['serial']
    mac_address = row['mac_address']
    serial_home_gateway = row['serial_home_gateway']
    macaddress_home_gateway = row['macaddress_home_gateway']
    id_ef = row['id_ef']
    id_hg = row['id_hg']
    designador = row['designador']
    
    df_ok = dfacs[dfacs['NRC'].str.contains(row['rpon'])]
    if df_ok.empty:
        if 'MIGR' in serial or 'MIGR' in serial_home_gateway or 'MIGR' in mac_address  or 'MIGR' in macaddress_home_gateway: 
            print('Sem solucao para o cliente ', designador)
        else:
            continue

    s = df_ok.iloc[0]['SERIALNUMBER'].upper()
    m = df_ok.iloc[0]['MACADDR'].upper()
    d = df_ok.iloc[0]['DEVICE_TYPE_NAME'].upper()
	
    dfcol.at[index, 'mac_acs'] = m
    dfcol.at[index, 'serial_acs'] = s
    dfcol.at[index, 'equip_acs'] = d

    #Equipamento Fisico
    if serial != s or mac_address != m:
        query_serial = 'update conf_online_owner.equipamento_fisico ef set ef.serial = \'' + str(s).upper() + '\', ef.mac_address = \'' + str(m).upper() + '\' where id = ' + str(id_ef)
        dfcol.at[index, 'processado_ef'] = execute_update(col_user, col_pass, col_db_alias, query_serial)
        print('Executado com sucesso em EF', designador, serial, s, mac_address, m, query_serial)

    #HG
    if serial_home_gateway != s or macaddress_home_gateway != m:
        query_hg = 'update conf_online_owner.home_gateway hg set hg.serial_home_gateway = \'' + str(s).upper() + '\', hg.macaddress_home_gateway = \'' + str(m).upper() + '\' where id = ' + str(id_hg)
        dfcol.at[index, 'processado_hg'] = execute_update(col_user, col_pass, col_db_alias, query_hg)
        print('Executado com sucesso em hg', designador, serial_home_gateway, s, macaddress_home_gateway, m, query_hg)

dfcol.to_csv('executado.csv', sep=';', encoding='utf-8')