# coding: utf-8

import pandas as pd
import numpy as np
import cx_Oracle
import datetime
import threading
import time

siebel_user = 'sbleim'
siebel_pass = 'sbleim'
siebel_db_alias = 'PSIE8'

is_user = 'service_inventory_owner'
is_pass = 'service_inventory_owner'
is_db_alias = 'SVCPCONF'

log_out = open('log_out_siebel.txt', 'a')

ultimo = ''

MAX_CONEXOES = 20
lista_threads = []

def logar(msg, *args):
    valor = str(datetime.datetime.now()) + ': ' +  str(msg)

    for arg in args:
        valor = valor + ' ' + str(arg)

    valor = valor + '\n'
    
    print(valor)
    log_out.write(valor)

def execute_select_principal():
    header = ['serial_num', 'end_rpon']
    lst = []

    try:
        con = cx_Oracle.connect(siebel_user, siebel_pass, siebel_db_alias)
        cur = con.cursor()
        cur.execute('''
                    select sa.serial_num, sap.INTEGRATION_ID end_rpon
                        from siebel.s_asset sa, siebel.s_asset_om som, siebel.s_addr_per sap
                        where       sa.row_id = som.PAR_ROW_ID
                                and som.FROM_ADDR_ID = sap.row_id
                                and sa.created_by = '8-KAG14D0' -- Usuário SBLVivo
                                and sa.STATUS_CD = 'Ativo'
                                and sa.prod_id in ('1-5WPB', '1-7HWB')
                                and sa.serial_num in ('1833232140',
'1533279401',
'1533885259',
'1120311901',
'1120370455',
'1932318515',
'1932330992',
'1932121674',
'1932433433',
'1332363178',
'1334662687',
'1141237663',
'1143304123',
'1129504269',
'1129595285',
'1130635927',
'1132286015',
'1132289962',
'1130832738',
'1131677928',
'1131683203',
'1145243367',
'1145245729',
'1130319343',
'1130319950')
                                ''')

        for serial_num, end_rpon in cur:
            lst.append([str(serial_num), str(end_rpon)])

    except Exception as e:
        logar('Erro ao executar execute_select_principal', e)

    return pd.DataFrame(lst, columns=header)

def execute_select(terminal, endereco):
    header = ['designator_value', 'external_address_id', 'account_id', 'designator_id']
    lst = []

    try:
        con = cx_Oracle.connect(is_user, is_pass, is_db_alias)
        cur = con.cursor()
        cur.execute('''
                    select  d.designator_value,
                            ia.external_address_id,
                            ia.account_id,
                            dpai.designator_id
                from service_inventory_owner.gvt_inv_designator        d,
                    service_inventory_owner.gvt_inv_item              i,
                    service_inventory_owner.gvt_inv_designator        dpai,
                    service_inventory_owner.gvt_inv_item              ipai,
                    service_inventory_owner.gvt_inv_inventory_address ia
                where d.designator_id = i.designator_id
                and dpai.designator_id = ipai.designator_id
                and d.parent_designator = dpai.designator_id
                and dpai.inventory_address_id = ia.inventory_address_id
                and d.designator_value = :designador
                and ia.external_address_id <> :end
                and d.status_id = 2
                and i.status_id = 2
                and dpai.status_id = 2
                and ipai.status_id = 2''', designador=terminal, end=endereco)

        for designator_value, external_address_id, account_id, designator_id in cur:
            lst.append([designator_value, external_address_id, account_id, designator_id])

    except Exception as e:
        logar('Erro ao executar select', terminal, endereco, e)

    return pd.DataFrame(lst, columns=header)

def execute_update1(endereco, p_account):

    try:
        con = cx_Oracle.connect(is_user, is_pass, is_db_alias)
        cur = con.cursor()
        cur.execute("""
                        insert into service_inventory_owner.gvt_inv_inventory_address
                            select service_inventory_owner.gvt_inv_seq_inventory_address.nextval,
                                    :endereco , 
                                    :account 
                                , sysdate,
                                    sysdate
                                from dual
                            where not exists (select 1
                                        from service_inventory_owner.gvt_inv_inventory_address
                                    where external_address_id =  :endereco )
                    """, {
                        "endereco" : str(endereco),
                        "account" : str(p_account)
                        })
        con.commit()

    except Exception as e:
        logar('Erro ao executar execute_update1', endereco, p_account, e)

def execute_update2(p_endereco, p_designator_id):

    try:
        con = cx_Oracle.connect(is_user, is_pass, is_db_alias)
        cur = con.cursor()
        cur.execute(""" 
                    update service_inventory_owner.gvt_inv_designator d
                        set d.inventory_address_id =
                            (select min(inventory_address_id)
                                from service_inventory_owner.gvt_inv_inventory_address
                                where external_address_id = :endereco 
                                ) where d.designator_id =  :designator
                    """, {
                        "endereco" : str(p_endereco),
                        "designator" : str(p_designator_id)
                        })
        con.commit()

    except Exception as e:
        logar('Erro ao executar execute_update2', p_endereco, p_designator_id, e)

def executar_acao(terminal, endereco):
    df_is = execute_select(terminal, endereco)
    logar('terminal', terminal, 'endereco', endereco, df_is)

    for index, row in df_is.iterrows():
        p_terminal = row['designator_value']
        p_endereco = row['external_address_id']
        p_account = row['account_id']
        p_designator_id = row['designator_id']

        execute_update1(endereco, p_account)
        execute_update2(endereco, p_designator_id)

        logar('terminal', terminal, 'end correto', endereco, 'is_account_id', p_account, 'is_designator_id', p_designator_id, '\n')

def executar(df):
    contador = 0
    for index, row in df.iterrows():
        if index % 100 == 0:
            print(index)

        if contador >= 1000000:
            break

        terminal = row['serial_num']
        endereco = row['end_rpon']

        if terminal is None or endereco is None:
            logar('Erro no terminal', terminal, endereco)
            continue

        #print(threading.active_count())
        while threading.active_count() > MAX_CONEXOES:
            #print("Esperando 2s .....")
            time.sleep(1)
        
        thread = threading.Thread(target=executar_acao, args=(terminal, endereco,))
        lista_threads.append(thread)
        thread.start()

        contador = contador + 1
        ultimo = terminal
    
    logar('Ultimo', ultimo)

logar('----------------------------------------- nova execução -----------------------------------------')
executar(execute_select_principal())
logar('----------------------------------------- termino execução -----------------------------------------')