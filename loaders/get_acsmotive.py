# coding: utf-8
    
import pandas as pd
import datetime
import ftplib
import cx_Oracle
import subprocess
import zipfile
import os
import requests
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
import gzip
import shutil

endereco = 'http://10.18.77.146/sera/base/index.php'
username = "basehdm"
password = "B4s3@c507"
filename = "BaseACSCompleta";

ipunif_user = 'PROFILING_OWNER'
ipunif_pass = 'ProfilingVivo123'
ipunif_db_alias = '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=IPUNIF))(ADDRESS=(PROTOCOL=TCP)(HOST=vipscancrs033)(PORT=1521)))'

data = datetime.date.today().strftime('%Y%m%d')
nome_do_arquivo = filename + '-' + data + '.csv.gz'
#nome_do_arquivo = filename + '-20171212.csv.gz'
nome_do_arquivo_csv = filename + '-' + data + '.csv'
funcionou = False

def truncate_table():
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        cur = con2.cursor()
        cur.execute('''truncate table BASEACSCOMPLETA''')

    except Exception as e:
        print('Erro ', e)

def retornar_create(tabela):
    return '''create table PROFILING_OWNER.''' + tabela + ''' (
                                    SUBSCRIBERID VARCHAR2(50),
                                    SERIALNUMBER VARCHAR2(50),
                                    DEVICE_TYPE_NAME VARCHAR2(50),
                                    FIRSTCONTACTTIME VARCHAR2(50),
                                    LASTCONTACTTIME VARCHAR2(50),
                                    LASTACTIVATIONTIME VARCHAR2(50),
                                    SOFTWAREVERSION VARCHAR2(50),
                                    EXTERNALIPADDRESS VARCHAR2(50),
                                    PRODUCT_CLASS VARCHAR2(50),
                                    MANUFACTURER VARCHAR2(50),
                                    OUI VARCHAR2(50),
                                    MODEL_NAME VARCHAR2(50),
                                    ACTIVATED VARCHAR2(50),
                                    DELETED VARCHAR2(50),
                                    NASIP VARCHAR2(50),
                                    NASPORT VARCHAR2(50),
                                    NRC VARCHAR2(50),
                                    REPLACED_BY VARCHAR2(50),
                                    SERVICETYPEID VARCHAR2(50),
                                    UPTIME VARCHAR2(50),
                                    FIRSTUSAGEDATE VARCHAR2(50),
                                    HARDWAREVERSION VARCHAR2(50),
                                    SPECVERSION VARCHAR2(50),
                                    PROVISIONCODE VARCHAR2(50),
                                    ADDITIONALHARDWAREVERSION VARCHAR2(50),
                                    ADDITIONALSOFTWAREVERSION VARCHAR2(50),
                                    VENDORCONFIGF VARCHAR2(50),
                                    MACADDR VARCHAR2(50)
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
            SUBSCRIBERID,
            SERIALNUMBER,
            DEVICE_TYPE_NAME,
            FIRSTCONTACTTIME,
            LASTCONTACTTIME,
            LASTACTIVATIONTIME,
            SOFTWAREVERSION,
            EXTERNALIPADDRESS,
            PRODUCT_CLASS,
            MANUFACTURER,
            OUI,
            MODEL_NAME,
            ACTIVATED,
            DELETED,
            NASIP,
            NASPORT,
            NRC,
            REPLACED_BY,
            SERVICETYPEID,
            UPTIME,
            FIRSTUSAGEDATE,
            HARDWAREVERSION,
            SPECVERSION,
            PROVISIONCODE,
            ADDITIONALHARDWAREVERSION,
            ADDITIONALSOFTWAREVERSION,
            VENDORCONFIGF,
            MACADDR
            )
    '''

    file = open(tabela + '.ctl','w') 
    file.write(arquivo)
    file.close() 


def recuperar_html():
    try:
        return str(requests.get(endereco, auth=(username, password)).content)
    except Exception as e:
        print('Erro ao carregar url', e)

def recuperar_tag_a(html):
    soup = BeautifulSoup(html, "html.parser")
    return soup.find_all('a')

def filtrar_links(tags, filtro):
    values = []
    for t in tags:
        if t.get('href') and filtro in t.get('href'):
            values.append(t)
    
    return values

def recuperar_arquivos(url, tag):
    try:
        link = ''
        #http://10.18.77.146/sera/base/BaseACSCompleta-20171213.csv.gz
        
        if not url.endswith('/'):
            link = url[:(url.rfind('/') + 1)] + str(tag.get('href')).replace('./','')
        else:
            link = url + tag.get('href')
        
        filename = link[(link.rfind('/') + 1):]
        print('Recuperar arquivos {} para {}'.format(link, filename))
        r = requests.get(link, auth=(username, password), stream=True)
        print('Arquivo do tamanho de:', len(r.content))
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        
        print('Gravado no disco e OK')
        return True
        
    except Exception as e:
        print('Erro ao recuperar arquivos', e)
        return False

def recuperar_arquivos2(url, tag):
    try:
        link = ''
        #http://10.18.77.146/sera/base/BaseACSCompleta-20171213.csv.gz
        
        if not url.endswith('/'):
            link = url[:(url.rfind('/') + 1)] + str(tag.get('href')).replace('./','')
        else:
            link = url + tag.get('href')
        
        filename = link[(link.rfind('/') + 1):]
        print('Recuperar arquivos {} para {}'.format(link, filename))
        r = requests.get(link, auth=(username, password), stream=True)
        print('Arquivo do tamanho de:', len(r.content))
        with open(filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

        tamanhoArquivo = os.path.getsize(filename)
        print('Tamanho do arquivo baixado = ', tamanhoArquivo)
        
        print('Gravado no disco e OK')
        return True
        
    except Exception as e:
        print('Erro ao recuperar arquivos', e)
        return False

tags = filtrar_links(recuperar_tag_a(recuperar_html()), nome_do_arquivo)
for t in tags:
    funcionou = recuperar_arquivos(endereco, t)
    
print('Executara?: ', funcionou)
if funcionou:
    with gzip.open(nome_do_arquivo, 'rb') as f_in, open(nome_do_arquivo_csv, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    print('Criou o arquivo >> ', nome_do_arquivo_csv)
    cwd = os.getcwd()
    cwd_csv = str(cwd) + '\\' + str(nome_do_arquivo_csv)
    print('cwd_csv', cwd_csv)

    # Truncate table
    #truncate_table()
    tabela_com_data = 'BASEACSCOMPLETA_' + data
    executar_base(retornar_create(tabela_com_data))
    executar_base('CREATE INDEX ' + tabela_com_data + '_NRC ON PROFILING_OWNER.' + tabela_com_data + ' (NRC)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_ser ON PROFILING_OWNER.' + tabela_com_data + ' (SERIALNUMBER)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_mac ON PROFILING_OWNER.' + tabela_com_data + ' (MACADDR)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_sid ON PROFILING_OWNER.' + tabela_com_data + ' (SUBSCRIBERID)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_NPT ON PROFILING_OWNER.' + tabela_com_data + ' (NASPORT)')
    executar_base('CREATE INDEX ' + tabela_com_data + '_NIP ON PROFILING_OWNER.' + tabela_com_data + ' (NASIP)')
    print('Criou a tabela ', tabela_com_data, ' + e truncou a BASEACSCOMPLETA')

    create_loader(nome_do_arquivo_csv, tabela_com_data)
    subprocess.call('C:\Oracle\instantclient_12_2\sqlldr.exe \
                    PROFILING_OWNER/ProfilingVivo123@vipscancrs033:1521/IPUNIF \
                    control=' + tabela_com_data + '.ctl \
                    data=' + cwd_csv + ' log=' + tabela_com_data + '.log', shell=True)

    create_loader(nome_do_arquivo_csv, 'BASEACSCOMPLETA')
    subprocess.call('C:\Oracle\instantclient_12_2\sqlldr.exe \
                    PROFILING_OWNER/ProfilingVivo123@vipscancrs033:1521/IPUNIF \
                    control=BASEACSCOMPLETA.ctl \
                    data=' + cwd_csv + ' log=BASEACSCOMPLETA.log', shell=True)

    executar_base("delete from PROFILING_OWNER." + tabela_com_data + " c where c.nrc = 'NRC' ")
    executar_base("delete from PROFILING_OWNER.BaseACSCompleta c where c.nrc = 'NRC' ")