# coding: utf-8

from subprocess import check_output
import numpy as np
import pandas as pd
import paramiko

str_host = 'vipsoasom2'
str_user = 'prd1vlog'
str_pass = '32qYmkTfW'

'''
select a.*, sa.serial_num designador
  from (select '00597484864' documento, '31228201500001' oldnrc
          from dual
        union
        select '04987350874' documento, '31158577800001' oldnrc
          from dual
        union
        select '05232137867' documento, '31190842500001' oldnrc
          from dual
        union
        select '05677957720' documento, '31163181900003' oldnrc from dual) a,
       siebel.s_asset sa,
       siebel.s_org_ext soe
 where sa.serv_acct_id = soe.row_id
   and sa.status_cd = 'Ativo'
   and sa.prod_id = '1-F7ISQ'
   and soe.x_doc_number = a.documento
'''

str_parte1 = '''curl -vk -m 30 -H "Content-Type: application/json" -d'{"instance": "25", "oldUniqueid": "'''
str_parte2 = '''", "newUniqueid": "'''
str_parte3 = '''"}'  https://babom.gvp.telefonica.com:7443/ossbss/v1/ChangeUniqueId'''

contador = 0
df = pd.read_csv("C:\GIT\python_util\ChangeUniqueId.20171031.csv", encoding='ISO-8859-1', sep=';', dtype=str)

ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

for index, row in df.iterrows():
    if index % 100 == 0:
        print(index)
    
    if contador >= 600:
        break
        
    DOCUMENTO = row['DOCUMENTO']
    OLDNRC = row['OLDNRC']
    DESIGNADOR = row['DESIGNADOR']
    
    comando = str_parte1 + str(OLDNRC) + str_parte2 + DESIGNADOR + str_parte3
    print(comando)

    
    ssh_client.connect(str_host, username=str_user, password=str_pass)
    stdin, stdout, stderr = ssh_client.exec_command(comando)
    print('stdin', stdin)
    print('stdout', stdout)
    print('stderr', stderr)

    for line in stdout:
        print(line)

    print('-----------------------------------------------------------------------')
	
ssh_client.close
#comando = "dir C:"
#retorno = check_output(comando, shell=True).decode('windows-1252')
#print(retorno)
