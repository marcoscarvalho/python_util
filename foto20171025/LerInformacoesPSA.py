# coding: utf-8

import numpy
import pandas as pd
import urllib
import http
import datetime
import threading
import time

is_user = 'service_inventory_owner'
is_pass = 'service_inventory_owner'
is_db_alias = 'SVCPCONF'
  
def read_excel(filename, sheet):
    xl = pd.ExcelFile(filename)
    return xl.parse(sheet)
	
	
def recuperar_informacao_web_radius(nrc):
    try:
        data = urllib.parse.urlencode({'valor1': nrc, 'tipo': 'n', 'Submit' : 'Consultar'})
        h = http.client.HTTPConnection('10.18.77.146')
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        h.request('POST', '/cgi-bin/consulta_socket', data, headers)
        return h.getresponse()
        
    except Exception as e:
        logar('Erro ao carregar url', e)

def recuperar_informacao_web_radius_por_terminal(terminal):
    try:
        data = urllib.parse.urlencode({'valor1': terminal, 'tipo': 't', 'Submit' : 'Consultar'})
        h = http.client.HTTPConnection('10.18.77.146')
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        h.request('POST', '/cgi-bin/consulta_socket', data, headers)
        return h.getresponse()
        
    except Exception as e:
        logar('Erro ao carregar url', e)

# Esse método abaixo encontra uma string.
# O find encontra char. Diferente do find_str
def find_str(s, char):
    index = 0

    if char in s:
        c = char[0]
        for ch in s:
            if ch == c:
                if s[index:index+len(char)] == char:
                    return index

            index += 1

    return -1


# Método criado para retirar qualquer coisa entre <palavra>
def retirar_colchetes(texto, separador):
    try:
        while "<" in texto:
            inicio = texto.find('<')
            fim = texto.find('>')
            parte1 = texto[:inicio]
            parte2 = texto[(fim+1):]
            texto = parte1 + separador + parte2
        return texto
    except Exception as e:
        logar('Erro ao retirar_colchetes', e)	

# Método com o objetivo de achar um título em um grande texto e
# pegar qualquer valor logo após essa palavra. 
# Lembrando que é necessário ter após esse valor um \n
def retornar_string(texto, titulo, separador='\n'):
    try:
        t1 = find_str(texto, titulo)
        t2 = texto[t1:]
        t3 = find_str(t2, ': ')
        t4 = t2[(t3+2):]
        t5 = find_str(t4, separador)
        return t4[:t5]
    except Exception as e:
        logar('Erro ao fazer o parse no ACS', e)	

def mapear_cliente(response, log):
    try:
        log = logar2(log, 'Status do processamento WebRadius: ', response.status, response.reason)
        q= response.read().decode("utf-8") 

        '''essa_informacao = retornar_string(q, 'nao foi encontrado favor consultar pelo NRC do cliente')
        if essa_informacao is not None:
            log = logar2(log, 'Não foi encontrado informações para o mac passado')
            return log'''

        nrc = retornar_string(q, 'NRC')
        serial = retornar_string(q, 'ACS Numero Serial')
        fabricante = retornar_string(q, 'ACS Fabr. Modem')
        modelo = retornar_string(q, 'ACS Modelo Modem')
        mac = retornar_string(q, 'ACS MAC-ADDR')
        log = logar2(log, 'Equipamento do cliente >> {} | mac: {} | serial: {} | nrc: {} | fabricante: {}'.format(modelo, mac, serial, nrc, fabricante));

        p_in_psa = find_str(q, 'estamos ocultando o IP do cliente')
        p_in_aprovisionamento = find_str(q, '<table border="1" width="200%"><tr><td colspan="18" align="center"><strong>Hist')
        p_out_aprovisionamento = find_str(q, '</table><br>')
        psa = q[(p_in_psa+36):p_in_aprovisionamento]
        aprovisionamento = q[p_in_aprovisionamento:(p_out_aprovisionamento+8)]
        
        log = logar2(log, 'Informações de PSA')
        log = logar2(log, psa)
        log = logar2(log, '')
        log = logar2(log, 'Inicio aprovisionamento')
        log = logar2(log, retirar_colchetes(aprovisionamento, ' | '))

        return log
    
    except Exception as e:
        logar('Erro ao fazer o parse do retorno do WebRadius', e)

def executar_acao_por_nrc(nrc):
    mapear_cliente(recuperar_informacao_web_radius(nrc))
    logar('')

def executar_acao_por_terminal(numero):
    mapear_cliente(recuperar_informacao_web_radius_por_terminal(numero))
    logar('')

def get_value_in_df(df, campo, valor):
    try:
        return df.loc[df[campo].isin(valor)]
        #return df.loc[df[campo] == valor]
    except Exception as e:
        print('error in get_value_in_df', e)

nome_sigres = ["NRO_TELEFONE13", "NRO_TELEFONE15", "DDD", "CNL", "NRC", "REDE", "VELOCIDADE_BL", "LOCALIDADE", "SIGLA_AT", "SITE", "NOME_OLT", "SLOT_OLT",
"SubSlot_OLT", "PORTA_OLT", "NOME_NISIP", "SLOT_NISIP", "SUBSLOT_NISIP", "PORTA_NISIP", "DATA_CRIACAO_CLIENTE", "DATA_MODIFICACAO_CLIENTE",
"ENDIP_AGREGADOR", "ENDIP_SWC", "ENDIP_SWD", "TIPO_EQUIP_OLT", "TIPO_EQUIP_AGREGADOR", "ENDIP_OLT", "NOME_SWD", "NOME_SWC",
"IP_FIXO_PLUS", "CATEGORIA_EQUIPTO_ACESSO", "TIPO_REDE", "CONNECT_INFO_SERVICO", "ESTADO_PATH_CLIENTE", "ESTADO_PORTA_ACESSO_FISICA", "ESTADO_PORTA_ACESSO_LOGICA", "SERVICE_ID_CLIENTE",
"SLID", "NOME_REDE_OLT", "NOME_REDE_AGREGADOR", "NOME_REDE_SWD", "NOME_REDE_SWC", "CLIENTE_BLOQUEADO", "MOTIVO_BLOQUEIO", "SLOT_SWC",
"PORT_SWC", "SLOT_SWD", "PORT_SWD", "TIPO_EQUIP_SWC", "TIPO_EQUIP_SWD", "CABO", "FIBRA", "VREDE",
"VUSER", "VLAN_VPN_IP", "VLAN_VPN_IP_INTERNET", "NAS_PORT", "NAS_IP", "ONU", "SLOT_ONU", "PORTA_ONU",
"IP_FIXO", "Fabricante_OLT", "Fabricante_SWD", "Fabricante_SWC", "Fabricante_SWC_Agregador", "STB_IP", "STB_IP2", "STB_IP3",
"STB_IP4", "STB_IP5", "STB_IP6", "MASK", "STB_DEFAULT_GATEWAY", "QTD_PONTOS_TV", "TV_SERVICES", "TV_SERVICES2",
"TV_SERVICES3", "TV_SERVICES4", "TV_SERVICES5", "TV_SERVICES6","VLAN_M", "VLAN_U", "VLAN_A", "ultimo"]

MAX_CONEXOES = 10
lista_threads = []
def executar():

    contador = 0
    df_clientes_migrados = pd.read_csv("C:\GIT\python_util\\foto\clientes_migrados_com_id_fibra.csv", encoding='ISO-8859-1', sep=';', dtype=str)
    df_20171021 = pd.read_csv(
        "C:\GIT\python_util\\foto\servicos_ativos_detalhado_20171021.csv", 
        encoding='ISO-8859-1', 
        sep=';', 
        dtype=str,
        usecols=["NRC", "NRO_TELEFONE15"])
    df_20171023 = pd.read_csv(
        "C:\GIT\python_util\\foto\servicos_ativos_detalhado_20171023.csv", 
        encoding='ISO-8859-1', 
        sep=';', 
        dtype=str,
        usecols=["NRC", "NRO_TELEFONE15"])

    nl_21 = df_20171021.shape[0]
    nl_23 = df_20171023.shape[0]

    for index, row in df_clientes_migrados.iterrows():

        if index % 100 == 0:
            print(index)

        if contador >= 10:
            break

        #print(threading.active_count())
        while threading.active_count() > MAX_CONEXOES:
            #print("Esperando 2s .....")
            time.sleep(2)
        
        #thread = threading.Thread(target=executar_acao_por_terminal, args=(terminal,))
        thread = threading.Thread(target=execute, args=(index, row, nl_21, nl_23, df_20171021, df_20171023, df_clientes_migrados,))
        lista_threads.append(thread)
        thread.start()

        contador += 1

def execute(index, row, nl_21, nl_23, df_20171021, df_20171023, df_clientes_migrados): 
    
    log = str(datetime.datetime.now()) + '\n'

    #BANDA	RPON_BANDA	LINHA	RPON_LINHA	TV	RPON_TV	DOCUMENTO	RPON_END
    BANDA = row['BANDA']
    RPON_BANDA = row['RPON_BANDA']
    LINHA = row['LINHA']
    RPON_LINHA = row['RPON_LINHA']
    TV = row['TV']
    RPON_TV = row['RPON_TV']
    DOCUMENTO = row['DOCUMENTO']
    RPON_END = row['RPON_END']
    ID_FIBRA = row['ID_FIBRA']

    df_position_20171021 = df_20171021.loc[df_20171021['NRO_TELEFONE15'] == ID_FIBRA]
    if df_position_20171021.empty:
        df_position_20171021 = df_20171021.loc[df_20171021['NRC'] == RPON_LINHA]

    df_position_20171023 = df_20171023.loc[df_20171023['NRO_TELEFONE15'] == ID_FIBRA]
    if df_position_20171023.empty:
        df_position_20171023 = df_20171023.loc[df_20171023['NRC'] == RPON_LINHA]

    msg_sigres = 'Informacoes de Sigres nulas'
    NRC1 = None
    NRC2 = None

    if not df_position_20171021.empty and not df_position_20171023.empty:
        msg_sigres = '------------'

        for index3, row3 in df_position_20171021.iterrows():
            NRC1 = row3['NRC']

        '''print('df_position_20171021')
        print(df_position_20171021)'''

        index1 = 0
        row1 = 0

        for index1, row1 in df_position_20171021.iterrows():
            #print('index', index1, 'row', row1)
            variavel_01 = 1

        df_20171021_return = pd.read_csv(
            "C:\GIT\python_util\\foto\servicos_ativos_detalhado_20171021.csv", 
            encoding='ISO-8859-1', 
            sep=';', 
            dtype=str, engine='python', header=None,
            skiprows=(int(index1)+2), skipfooter=(int(nl_21)-int(index1)-1),
            names=nome_sigres )

        '''print(' -fasdfa asdfasdf - df_20171021_return')
        print(df_20171021_return)
        print(' -fasdfa asdfasdf - df_20171021_return')


        print('----------------------------------------')
        print('df_position_20171023')
        print(df_position_20171023)'''

        index2 = 0
        row2 = 0

        for index2, row2 in df_position_20171023.iterrows():
            #print('index', index2, 'row', row2)
            variavel_02 = 1

        df_20171023_return = pd.read_csv(
            "C:\GIT\python_util\\foto\servicos_ativos_detalhado_20171023.csv", 
            encoding='ISO-8859-1', 
            sep=';', 
            dtype=str, engine='python', header=None,
            skiprows=(int(index2)+2), skipfooter=(int(nl_23)-int(index2)-1),
            names=nome_sigres )

        '''print(' -fasdfa asdfasdf - df_20171023_return')
        print(df_20171023_return)
        print(' -fasdfa asdfasdf - df_20171023_return')'''

        comparativoEntreDataFrames = df_20171023_return.equals(df_20171021_return)
        msg_sigres = 'Sigres é igual? >> ' + str(comparativoEntreDataFrames)

    log = logar2(log, 'BANDA', BANDA, 'RPON_BANDA', RPON_BANDA, 'LINHA', LINHA, 'RPON_LINHA', RPON_LINHA, 'TV', TV, 'RPON_TV', RPON_TV, 'DOCUMENTO', DOCUMENTO, 'RPON_END', RPON_END, 'ID_FIBRA', ID_FIBRA, 'Sigres:', msg_sigres, 'NRC1:', NRC1, 'NRC2', NRC2)

    if NRC1 is not None:
        log = mapear_cliente(recuperar_informacao_web_radius(NRC1), log)
        
    else:
        log = mapear_cliente(recuperar_informacao_web_radius_por_terminal(LINHA[2:]), log)


    log = logar2(log, '---------------------------------------- FIM ----------------------------------------')
    logar(log)

log_out = open('log_out_fera.txt', 'a')
def logar(msg, *args):
    valor = str(datetime.datetime.now()) + ': ' +  str(msg)

    for arg in args:
        valor = valor + ' ' + str(arg)

    valor = valor + '\n'
    
    #print(valor)
    log_out.write(valor)
'''
a = read_excel('TblProblemaLIRS.xlsx', 'TblProblemaLIRS')
for index, row in a.iterrows():
    logar('Cliente possui terminal {}, NRC {}, status {}, criação {}, modificacao {}'.format(row['LINHA'], row['RPON'], row['ISTATUS'], row['DATE_CREATED'], row['DATE_MODIFIED']))    
    mapear_cliente(recuperar_informacao_web_radius(row['RPON']))
    logar('')
'''

def logar2(string_append, msg, *args):
    string_append = string_append + ' ' + str(msg)

    for arg in args:
        string_append = string_append + ' ' + str(arg)

    string_append = string_append + '\n'
    return string_append

executar()