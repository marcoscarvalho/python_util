# coding: utf-8

import pandas as pd
import datetime
import ftplib
import cx_Oracle
import subprocess
import zipfile
import os

ip = "192.168.236.47"
username = "telesp"
password = "tele@ftp22"
diretorio = "/ativacao/ftp_telesp/d_menos_1_fttx"
filename = "servicos_ativos_detalhado_";

ipunif_user = 'PROFILING_OWNER'
ipunif_pass = 'ProfilingVivo123'
ipunif_db_alias = '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=IPUNIF))(ADDRESS=(PROTOCOL=TCP)(HOST=vipscancrs033)(PORT=1521)))'

data = datetime.date.today().strftime('%Y%m%d')
nome_do_arquivo = filename + data + '.zip'
nome_do_arquivo_csv = filename + data + '.csv'
funcionou = False

print('Inicio')

def truncate_table():
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        cur = con2.cursor()
        cur.execute('''truncate table sad''')

    except Exception as e:
        print('Erro ', e)

def retornar_create(tabela):
    return '''create table PROFILING_OWNER.''' + tabela + ''' (
                NRO_TELEFONE13 varchar2(50),
                NRO_TELEFONE15 varchar2(50),
                DDD varchar2(50),
                CNL varchar2(50),
                NRC varchar2(50),
                REDE varchar2(50),
                VELOCIDADE_BL varchar2(50),
                LOCALIDADE varchar2(50),
                SIGLA_AT varchar2(50),
                SITE varchar2(50),
                NOME_OLT varchar2(50),
                SLOT_OLT varchar2(50),
                SubSlot_OLT varchar2(50),
                PORTA_OLT varchar2(50),
                NOME_NISIP varchar2(50),
                SLOT_NISIP varchar2(50),
                SUBSLOT_NISIP varchar2(50),
                PORTA_NISIP varchar2(50),
                DATA_CRIACAO_CLIENTE varchar2(50),
                DATA_MODIFICACAO_CLIENTE varchar2(50),
                ENDIP_AGREGADOR varchar2(50),
                ENDIP_SWC varchar2(50),
                ENDIP_SWD varchar2(50),
                TIPO_EQUIP_OLT varchar2(50),
                TIPO_EQUIP_AGREGADOR varchar2(50),
                ENDIP_OLT varchar2(50),
                NOME_SWD varchar2(50),
                NOME_SWC varchar2(50),
                IP_FIXO_PLUS varchar2(50),
                CATEGORIA_EQUIPTO_ACESSO varchar2(50),
                TIPO_REDE varchar2(50),
                CONNECT_INFO_SERVICO varchar2(50),
                ESTADO_PATH_CLIENTE varchar2(50),
                ESTADO_PORTA_ACESSO_FISICA varchar2(50),
                ESTADO_PORTA_ACESSO_LOGICA varchar2(50),
                SERVICE_ID_CLIENTE varchar2(50),
                SLID varchar2(50),
                NOME_REDE_OLT varchar2(50),
                NOME_REDE_AGREGADOR varchar2(50),
                NOME_REDE_SWD varchar2(50),
                NOME_REDE_SWC varchar2(50),
                CLIENTE_BLOQUEADO varchar2(50),
                MOTIVO_BLOQUEIO varchar2(50),
                SLOT_SWC varchar2(50),
                PORT_SWC varchar2(50),
                SLOT_SWD varchar2(50),
                PORT_SWD varchar2(50),
                TIPO_EQUIP_SWC varchar2(50),
                TIPO_EQUIP_SWD varchar2(50),
                CABO varchar2(50),
                FIBRA varchar2(50),
                VREDE varchar2(50),
                VUSER varchar2(50),
                VLAN_VPN_IP varchar2(50),
                VLAN_VPN_IP_INTERNET varchar2(50),
                NAS_PORT varchar2(50),
                NAS_IP varchar2(50),
                ONU varchar2(50),
                SLOT_ONU varchar2(50),
                PORTA_ONU varchar2(50),
                IP_FIXO varchar2(50),
                Fabricante_OLT varchar2(50),
                Fabricante_SWD varchar2(50),
                Fabricante_SWC varchar2(50),
                Fabricante_SWC_Agregador varchar2(50),
                STB_IP varchar2(50),
                STB_IP2 varchar2(50),
                STB_IP3 varchar2(50),
                STB_IP4 varchar2(50),
                STB_IP5 varchar2(50),
                STB_IP6 varchar2(50),
                MASK varchar2(50),
                STB_DEFAULT_GATEWAY varchar2(50),
                QTD_PONTOS_TV varchar2(50),
                TV_SERVICES varchar2(50),
                TV_SERVICES2 varchar2(50),
                TV_SERVICES3 varchar2(50),
                TV_SERVICES4 varchar2(50),
                TV_SERVICES5 varchar2(50),
                TV_SERVICES6 varchar2(50),
                VLAN_M varchar2(50),
                VLAN_U varchar2(50),
                VLAN_A  varchar2(50)
                )'''

def executar_base(param):
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        cur = con2.cursor()
        cur.execute(param)

    except Exception as e:
        print('Erro ', e)

def create_loader(arquivo_original, tabela):
    arquivo = '''
         OPTIONS (DIRECT=TRUE, ERRORS=50, rows=500000, bindsize=100000)
            UNRECOVERABLE

            load data
            infile ' ''' + arquivo_original + ''''
            truncate into table PROFILING_OWNER.''' + tabela + '''
            
            FIELDS TERMINATED BY ';'
            TRAILING NULLCOLS
            ( 
            
            NRO_TELEFONE13,
            NRO_TELEFONE15,
            DDD,
            CNL,
            NRC,
            REDE,
            VELOCIDADE_BL,
            LOCALIDADE,
            SIGLA_AT,
            SITE,
            NOME_OLT,
            SLOT_OLT,
            SubSlot_OLT,
            PORTA_OLT,
            NOME_NISIP,
            SLOT_NISIP,
            SUBSLOT_NISIP,
            PORTA_NISIP,
            DATA_CRIACAO_CLIENTE,
            DATA_MODIFICACAO_CLIENTE,
            ENDIP_AGREGADOR,
            ENDIP_SWC,
            ENDIP_SWD,
            TIPO_EQUIP_OLT,
            TIPO_EQUIP_AGREGADOR,
            ENDIP_OLT,
            NOME_SWD,
            NOME_SWC,
            IP_FIXO_PLUS,
            CATEGORIA_EQUIPTO_ACESSO,
            TIPO_REDE,
            CONNECT_INFO_SERVICO,
            ESTADO_PATH_CLIENTE,
            ESTADO_PORTA_ACESSO_FISICA,
            ESTADO_PORTA_ACESSO_LOGICA,
            SERVICE_ID_CLIENTE,
            SLID,
            NOME_REDE_OLT,
            NOME_REDE_AGREGADOR,
            NOME_REDE_SWD,
            NOME_REDE_SWC,
            CLIENTE_BLOQUEADO,
            MOTIVO_BLOQUEIO,
            SLOT_SWC,
            PORT_SWC,
            SLOT_SWD,
            PORT_SWD,
            TIPO_EQUIP_SWC,
            TIPO_EQUIP_SWD,
            CABO,
            FIBRA,
            VREDE,
            VUSER,
            VLAN_VPN_IP,
            VLAN_VPN_IP_INTERNET,
            NAS_PORT,
            NAS_IP,
            ONU,
            SLOT_ONU,
            PORTA_ONU,
            IP_FIXO,
            Fabricante_OLT,
            Fabricante_SWD,
            Fabricante_SWC,
            Fabricante_SWC_Agregador,
            STB_IP,
            STB_IP2,
            STB_IP3,
            STB_IP4,
            STB_IP5,
            STB_IP6,
            MASK,
            STB_DEFAULT_GATEWAY,
            QTD_PONTOS_TV,
            TV_SERVICES,
            TV_SERVICES2,
            TV_SERVICES3,
            TV_SERVICES4,
            TV_SERVICES5,
            TV_SERVICES6,
            VLAN_M,
            VLAN_U,
            VLAN_A
            )
    '''

    file = open(tabela + '.ctl','w') 
    file.write(arquivo)
    #print(arquivo)
    file.close() 

ftp = ftplib.FTP(ip)
ftp.login(username, password)
ftp.cwd(diretorio)
files = ftp.nlst()  

print('nome_do_arquivo', nome_do_arquivo)

for arquivo in files:
    if arquivo == nome_do_arquivo:
        fhandle = open(arquivo, 'wb')
        ftp.retrbinary('RETR ' + arquivo, fhandle.write)
        fhandle.close()    
        funcionou = True
  
print('funcionou', funcionou)  


if funcionou is False:
    today = datetime.date.today()
    data = today.replace(day=today.day - 1).strftime('%Y%m%d')
    nome_do_arquivo = filename + data + '.zip'
    nome_do_arquivo_csv = filename + data + '.csv'
    print("Pegar o arquivo do dia anterior", nome_do_arquivo)
    
    for arquivo in files:
        if arquivo == nome_do_arquivo:
            fhandle = open(arquivo, 'wb')
            ftp.retrbinary('RETR ' + arquivo, fhandle.write)
            fhandle.close()    
            funcionou = True
    

if funcionou:
    zip_ref = zipfile.ZipFile(nome_do_arquivo, 'r')
    zip_ref.extractall()
    zip_ref.close()

    cwd = os.getcwd()
    cwd_csv = str(cwd) + str('\\export\\home\\inventario\\sigres_telesp\\relatorios\\d_menos_1_fttx\\csv\\') + str(nome_do_arquivo_csv)

    # Truncate table
    #truncate_table()
    tabela_com_data = 'sad_' + data
    executar_base(retornar_create(tabela_com_data))
    executar_base('CREATE INDEX ' + tabela_com_data + '_nrc ON PROFILING_OWNER.' + tabela_com_data + ' (nrc)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_fibra ON PROFILING_OWNER.' + tabela_com_data + ' (NRO_TELEFONE15)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_NPT ON PROFILING_OWNER.' + tabela_com_data + ' (NAS_PORT)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_NIP ON PROFILING_OWNER.' + tabela_com_data + ' (NAS_IP)')
    print('Criou a tabela ', tabela_com_data, ' + e truncou a SAD')

    create_loader(nome_do_arquivo_csv, tabela_com_data)
    subprocess.call('C:\Oracle\instantclient_12_2\sqlldr.exe \
                    PROFILING_OWNER/ProfilingVivo123@vipscancrs033:1521/IPUNIF \
                    control=' + tabela_com_data + '.ctl \
                    data=' + cwd_csv + ' log=' + tabela_com_data + '.log', shell=True)

    create_loader(nome_do_arquivo_csv, 'sad')
    subprocess.call('C:\Oracle\instantclient_12_2\sqlldr.exe \
                    PROFILING_OWNER/ProfilingVivo123@vipscancrs033:1521/IPUNIF \
                    control=sad.ctl \
                    data=' + cwd_csv + ' log=sad.log', shell=True)
                    
    create_loader(nome_do_arquivo_csv, 'SIGRES_BANDA_FIBRA')
    subprocess.call('C:\Oracle\instantclient_12_2\sqlldr.exe \
                    PROFILING_OWNER/ProfilingVivo123@vipscancrs033:1521/IPUNIF \
                    control=SIGRES_BANDA_FIBRA.ctl \
                    data=' + cwd_csv + ' log=SIGRES_BANDA_FIBRA.log', shell=True)

    executar_base("delete from PROFILING_OWNER." + tabela_com_data + " where nrc = 'NRC' ")
    executar_base("delete from PROFILING_OWNER.sad where nrc = 'NRC' ")
    executar_base("delete from PROFILING_OWNER.SIGRES_BANDA_FIBRA where nrc = 'NRC' ")