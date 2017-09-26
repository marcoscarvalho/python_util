# coding: utf-8

import cx_Oracle
from domain import DataTable

col_user = 'MIG_CONF_ONLINE'
col_pass = 'gvt2000'
col_db_alias = 'SVCPCONF'

is_user = 'service_inventory_owner'
is_pass = 'service_inventory_owner'
is_db_alias = 'SVCPCONF'

tabela_equipamentos = DataTable("Equipamentos")
tabela_equipamentos.add_column('modelo', 'varchar')
tabela_equipamentos.add_column('id_recurso_logico_spec', 'numeric')
tabela_equipamentos.add_column('id_modelo_equipamento', 'numeric')
		
table = DataTable("COL")
col_designador = table.add_column('designador', 'varchar')
table.add_column('crm_origem', 'varchar')
table.add_column('iss_status', 'numeric')
table.add_column('hg_status', 'numeric')
table.add_column('serial_home_gateway', 'varchar')
table.add_column('macaddress_home_gateway', 'varchar')
table.add_column('hg_modelo_equipamento', 'numeric')
table.add_column('rl_status', 'numeric')
table.add_column('id_recurso_logico_spec', 'numeric')
table.add_column('ef_modelo_equipamento', 'numeric')
table.add_column('serial', 'varchar')
table.add_column('mac_address', 'varchar')
table.add_column('rpon', 'varchar')

table_is = DataTable("IS")
table_is.add_column('designador', 'varchar')
table_is.add_column('rpon', 'varchar')

table.add_references("designador", table_is, col_designador)

col_select_equipamentos = '''
select modelo, id_recurso_logico_spec, id_modelo_equipamento from conf_online_owner.modelo_equipamento me
'''

col_select_main = '''
select ip.designador,
       ip.crm_origem,
       iss.id_status              iss_status,
       hg.id_status               hg_status,
       hg.serial_home_gateway,
       hg.macaddress_home_gateway,
       hg.id_modelo_equipamento   hg_modelo_equipamento,
       rl.id_status_servico       rl_status,
       rl.id_recurso_logico_spec,
       ef.id_modelo_equipamento   ef_modelo_equipamento,
       ef.serial,
       ef.mac_address
  from conf_online_owner.instancia_produto  ip,
       conf_online_owner.instancia_servico  iss,
       conf_online_owner.home_gateway       hg,
       conf_online_owner.recurso_logico     rl,
       conf_online_owner.equipamento_fisico ef
 where ip.id = iss.id_instancia_produto
   and iss.id = hg.id_instancia_servico
   and iss.id = rl.id_instancia_servico
   and rl.id_equipamento_fisico = ef.id
   and ip.crm_origem = 'MIGRACAO'
   and ip.designador = 'TBS-056101889655-013'
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
   and d.designator_value = 'TBS-056101889655-013'
'''

def execute_select_equip(user, password, db_alias, select, table):
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for modelo, id_recurso_logico_spec, id_modelo_equipamento in cur:
            table = modelo, id_recurso_logico_spec, id_modelo_equipamento

    except Exception as e:
        print('Erro ao executar select', e)
		
def execute_select(user, password, db_alias, select, table):
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for designador, crm_origem, iss_status, hg_status, serial_home_gateway, macaddress_home_gateway, hg_modelo_equipamento, rl_status, id_recurso_logico_spec, ef_modelo_equipamento, serial, mac_address in cur:
            table = designador, crm_origem, iss_status, hg_status, serial_home_gateway, macaddress_home_gateway, hg_modelo_equipamento, rl_status, id_recurso_logico_spec, ef_modelo_equipamento, serial, mac_address

    except Exception as e:
        print('Erro ao executar select', e)

def execute_select2(user, password, db_alias, select, table):
    try:
        con = cx_Oracle.connect(user, password, db_alias)
        cur = con.cursor()
        cur.execute(select)

        for designador, rpon in cur:
            table = designador, rpon

    except Exception as e:
        print('Erro ao executar select', e)

execute_select_equip(col_user, col_pass, col_db_alias, col_select_equipamentos, tabela_equipamentos)
execute_select(col_user, col_pass, col_db_alias, col_select_main, table)
execute_select2(is_user, is_pass, is_db_alias, is_select_rpon, table_is)

print(tabela_equipamentos)
print(table)
print(table_is)