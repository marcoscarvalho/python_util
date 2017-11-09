# coding: utf-8

import numpy as np
import pandas as pd
import urllib
import http
import datetime
import threading
import time
import sys, os
import cx_Oracle

ipunif_user = 'PROFILING_OWNER'
ipunif_pass = 'ProfilingVivo123'
ipunif_db_alias = '(DESCRIPTION=(CONNECT_DATA=(SERVICE_NAME=IPUNIF))(ADDRESS=(PROTOCOL=TCP)(HOST=vipscancrs033)(PORT=1521)))'

array_vazio = ['', '', '', '', '', '', '', '', '', '', '', '','', '', 
'', '', '', '','', '','', '', '', '', '', '', '', '','', '', '', 
'', '', '', '', '','', '', '', '', '', '', '', '','', '', '', '', 
'', '', '', '','', '', '', '', '', '', '', '','', '', '', '', '', 
'', '', '','', '', '', '', '', '', '', '','', '', '', '','', '', '', '']

cols = ['BANDA', 'RPON_BANDA', 'LINHA', 'RPON_LINHA', 'TV', 'RPON_TV', 'DOCUMENTO', 'RPON_END', 'ID_FIBRA', 'nrc', 'serial', 'fabricante', 'modelo', 'mac', 'psa', 
        'aprovisionamento', 'sigres_df_igual','NRO_TELEFONE13_df1', 'NRO_TELEFONE15_df1', 'DDD_df1', 'CNL_df1', 'NRC_df1', 'REDE_df1', 'VELOCIDADE_BL_df1', 'LOCALIDADE_df1', 'SIGLA_AT_df1', 'SITE_df1', 'NOME_OLT_df1', 'SLOT_OLT_df1',
        'SubSlot_OLT_df1', 'PORTA_OLT_df1', 'NOME_NISIP_df1', 'SLOT_NISIP_df1', 'SUBSLOT_NISIP_df1', 'PORTA_NISIP_df1', 'DATA_CRIACAO_CLIENTE_df1', 'DATA_MODIFICACAO_CLIENTE_df1',
        'ENDIP_AGREGADOR_df1', 'ENDIP_SWC_df1', 'ENDIP_SWD_df1', 'TIPO_EQUIP_OLT_df1', 'TIPO_EQUIP_AGREGADOR_df1', 'ENDIP_OLT_df1', 'NOME_SWD_df1', 'NOME_SWC_df1',
        'IP_FIXO_PLUS_df1', 'CATEGORIA_EQUIPTO_ACESSO_df1', 'TIPO_REDE_df1', 'CONNECT_INFO_SERVICO_df1', 'ESTADO_PATH_CLIENTE_df1', 'ESTADO_PORTA_ACESSO_FISICA_df1', 'ESTADO_PORTA_ACESSO_LOGICA_df1', 'SERVICE_ID_CLIENTE_df1',
        'SLID_df1', 'NOME_REDE_OLT_df1', 'NOME_REDE_AGREGADOR_df1', 'NOME_REDE_SWD_df1', 'NOME_REDE_SWC_df1', 'CLIENTE_BLOQUEADO_df1', 'MOTIVO_BLOQUEIO_df1', 'SLOT_SWC_df1',
        'PORT_SWC_df1', 'SLOT_SWD_df1', 'PORT_SWD_df1', 'TIPO_EQUIP_SWC_df1', 'TIPO_EQUIP_SWD_df1', 'CABO_df1', 'FIBRA_df1', 'VREDE_df1',
        'VUSER_df1', 'VLAN_VPN_IP_df1', 'VLAN_VPN_IP_INTERNET_df1', 'NAS_PORT_df1', 'NAS_IP_df1', 'ONU_df1', 'SLOT_ONU_df1', 'PORTA_ONU_df1',
        'IP_FIXO_df1', 'Fabricante_OLT_df1', 'Fabricante_SWD_df1', 'Fabricante_SWC_df1', 'Fabricante_SWC_Agregador_df1', 'STB_IP_df1', 'STB_IP2_df1', 'STB_IP3_df1',
        'STB_IP4_df1', 'STB_IP5_df1', 'STB_IP6_df1', 'MASK_df1', 'STB_DEFAULT_GATEWAY_df1', 'QTD_PONTOS_TV_df1', 'TV_SERVICES_df1', 'TV_SERVICES2_df1',
        'TV_SERVICES3_df1', 'TV_SERVICES4_df1', 'TV_SERVICES5_df1', 'TV_SERVICES6_df1','VLAN_M_df1', 'VLAN_U_df1', 'VLAN_A_df1',
        'NRO_TELEFONE13__df2', 'NRO_TELEFONE15__df2', 'DDD__df2', 'CNL__df2', 'NRC__df2', 'REDE__df2', 'VELOCIDADE_BL__df2', 'LOCALIDADE__df2', 'SIGLA_AT__df2', 'SITE__df2', 'NOME_OLT__df2', 'SLOT_OLT__df2',
        'SubSlot_OLT__df2', 'PORTA_OLT__df2', 'NOME_NISIP__df2', 'SLOT_NISIP__df2', 'SUBSLOT_NISIP__df2', 'PORTA_NISIP__df2', 'DATA_CRIACAO_CLIENTE__df2', 'DATA_MODIFICACAO_CLIENTE__df2',
        'ENDIP_AGREGADOR__df2', 'ENDIP_SWC__df2', 'ENDIP_SWD__df2', 'TIPO_EQUIP_OLT__df2', 'TIPO_EQUIP_AGREGADOR__df2', 'ENDIP_OLT__df2', 'NOME_SWD__df2', 'NOME_SWC__df2',
        'IP_FIXO_PLUS__df2', 'CATEGORIA_EQUIPTO_ACESSO__df2', 'TIPO_REDE__df2', 'CONNECT_INFO_SERVICO__df2', 'ESTADO_PATH_CLIENTE__df2', 'ESTADO_PORTA_ACESSO_FISICA__df2', 'ESTADO_PORTA_ACESSO_LOGICA__df2', 'SERVICE_ID_CLIENTE__df2',
        'SLID__df2', 'NOME_REDE_OLT__df2', 'NOME_REDE_AGREGADOR__df2', 'NOME_REDE_SWD__df2', 'NOME_REDE_SWC__df2', 'CLIENTE_BLOQUEADO__df2', 'MOTIVO_BLOQUEIO__df2', 'SLOT_SWC__df2',
        'PORT_SWC__df2', 'SLOT_SWD__df2', 'PORT_SWD__df2', 'TIPO_EQUIP_SWC__df2', 'TIPO_EQUIP_SWD__df2', 'CABO__df2', 'FIBRA__df2', 'VREDE__df2',
        'VUSER__df2', 'VLAN_VPN_IP__df2', 'VLAN_VPN_IP_INTERNET__df2', 'NAS_PORT__df2', 'NAS_IP__df2', 'ONU__df2', 'SLOT_ONU__df2', 'PORTA_ONU__df2',
        'IP_FIXO__df2', 'Fabricante_OLT__df2', 'Fabricante_SWD__df2', 'Fabricante_SWC__df2', 'Fabricante_SWC_Agregador__df2', 'STB_IP__df2', 'STB_IP2__df2', 'STB_IP3__df2',
        'STB_IP4__df2', 'STB_IP5__df2', 'STB_IP6__df2', 'MASK__df2', 'STB_DEFAULT_GATEWAY__df2', 'QTD_PONTOS_TV__df2', 'TV_SERVICES__df2', 'TV_SERVICES2__df2',
        'TV_SERVICES3__df2', 'TV_SERVICES4__df2', 'TV_SERVICES5__df2', 'TV_SERVICES6__df2','VLAN_M__df2', 'VLAN_U__df2', 'VLAN_A__df2']
	
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

def select_sigres_anterior_by_nrc(nrc):
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        return pd.read_sql('''
				select * 
				  from profiling_owner.sad_20171106 
				 where nrc = :p01_nrc''', 
				 con=con2, params={'p01_nrc': nrc})

    except Exception as e:
        print('Erro ao executar select', nrc, e)

    return None

def select_sigres_anterior_by_id_fibra(id_fibra):
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        return pd.read_sql('''
				select * 
				  from profiling_owner.sad_20171106 
				 where nro_telefone15 = :p01_nrc''', 
				 con=con2, params={'p01_nrc': id_fibra})

    except Exception as e:
        print('Erro ao executar select', id_fibra, e)

    return None

def select_sigres_depois_by_nrc(nrc):
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        return pd.read_sql('''
				select * 
				  from profiling_owner.sad_20171108 
				 where nrc = :p01_nrc''', 
				 con=con2, params={'p01_nrc': nrc})

    except Exception as e:
        print('Erro ao executar select', nrc, e)

    return None

def select_sigres_depois_by_id_fibra(id_fibra):
    try:
        con2 = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        return pd.read_sql('''
				select * 
				  from profiling_owner.sad_20171108 
				 where nro_telefone15 = :p01_nrc''', 
				 con=con2, params={'p01_nrc': id_fibra})

    except Exception as e:
        print('Erro ao executar select', id_fibra, e)

    return None

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
        t6 = t4[:t5]

        if t6 is None or t6 == '':
            return ''

        return t6
    except Exception as e:
        logar('Erro ao fazer o parse no ACS', e)	



def mapear_cliente(response):
    try:
        logar('Status do processamento WebRadius: ', response.status, response.reason)
        q= response.read().decode("utf-8") 

        nrc = retornar_string(q, 'NRC')
        serial = retornar_string(q, 'ACS Numero Serial')
        fabricante = retornar_string(q, 'ACS Fabr. Modem')
        modelo = retornar_string(q, 'ACS Modelo Modem')
        mac = retornar_string(q, 'ACS MAC-ADDR')

        logar('Equipamento do cliente >> {} | mac: {} | serial: {} | nrc: {} | fabricante: {}'.format(modelo, mac, serial, nrc, fabricante));

        p_in_psa = find_str(q, 'Status  Data Start ')
        p_in_aprovisionamento = find_str(q, '<table border="1" width="200%"><tr><td colspan="18" align="center"><strong>Hist')
        p_out_aprovisionamento = find_str(q, '</table><br>')

        logar('p_in_psa', p_in_psa)
        logar('p_in_aprovisionamento', p_in_aprovisionamento)
        logar('p_out_aprovisionamento', p_out_aprovisionamento)

        psa = ''
        if p_in_psa > 10:
            psa = q[p_in_psa:p_in_aprovisionamento]
        else: 
            psa = 'Erro ao fazer o parse psa'
            logar(psa)

        aprovisionamento = ''
        if p_in_aprovisionamento > 10:
            aprovisionamento = q[p_in_aprovisionamento:(p_out_aprovisionamento+8)]
            aprovisionamento = retirar_colchetes(aprovisionamento, ' | ')
        else: 
            aprovisionamento = 'Erro ao fazer o parse aprovisionamento'
            logar(aprovisionamento)
        
        #logar('Informações de PSA')
        #logar(psa)
        #logar('')
        #logar('Inicio aprovisionamento')
        #logar(aprovisionamento)

        return nrc, serial, fabricante, modelo, mac, psa, aprovisionamento

    except Exception as e:
        logar('Erro ao fazer o parse do retorno do WebRadius', e)

def executar():

    lst = []
    contador = 0
    df_clientes_migrados = pd.read_csv("C:\GIT\python_util\\foto\clientes_migrados_com_id_fibra.csv", encoding='ISO-8859-1', sep=';', dtype=str)

    for index, row in df_clientes_migrados.iterrows():

        if index % 100 == 0:
            print(index)

        if contador >= 100:
            break
        
        try:
            BANDA = row['BANDA']
            RPON_BANDA = row['RPON_BANDA']
            LINHA = row['LINHA']
            RPON_LINHA = row['RPON_LINHA']
            TV = row['TV']
            RPON_TV = row['RPON_TV']
            DOCUMENTO = row['DOCUMENTO']
            RPON_END = row['RPON_END']
            ID_FIBRA = row['ID_FIBRA']

            

            df_position_20171021 = select_sigres_anterior_by_id_fibra(ID_FIBRA)
            if df_position_20171021 is None:
                df_position_20171021 = select_sigres_anterior_by_nrc(RPON_LINHA)

            df_position_20171023 = select_sigres_depois_by_id_fibra(ID_FIBRA)
            if df_position_20171023 is None:
                df_position_20171023 = select_sigres_depois_by_nrc(RPON_LINHA)

            nrc = None
            serial = None
            fabricante = None
            modelo = None
            mac = None
            psa = None
            aprovisionamento = None
            sigres_df_igual = None
            NRC1 = None

            if not df_position_20171021.empty:
                for index3, row3 in df_position_20171021.iterrows():
                    NRC1 = row3['NRC']
            
                sigres_df_igual = df_position_20171021.equals(df_position_20171023)

            if NRC1 is not None:
                logar('Pelo NRC que veio do Sigres', NRC1)
                nrc, serial, fabricante, modelo, mac, psa, aprovisionamento = mapear_cliente(recuperar_informacao_web_radius(NRC1))
                
            else:
                logar('Pelo NRC que veio do Siebel', RPON_LINHA)
                nrc, serial, fabricante, modelo, mac, psa, aprovisionamento = mapear_cliente(recuperar_informacao_web_radius(RPON_LINHA))

            lst1 = []
            #cols = ['BANDA', 'RPON_BANDA', 'LINHA', 'RPON_LINHA', 'TV', 'RPON_TV', 'DOCUMENTO', 'RPON_END', 'ID_FIBRA', 'nrc', 'serial', 'fabricante', 'modelo', 'mac', 'psa', 'aprovisionamento', 'sigres_df_igual']

            a = None
            if not df_position_20171021.empty:
                a = df_position_20171021.values
            else:
                a = array_vazio
                logar('df_position_20171021 Vazio')

            b = None
            if not df_position_20171023.empty:
                b = df_position_20171023.values
            else:
                b = array_vazio
                logar('df_position_20171023 Vazio')

            lst2 = []
            if df_position_20171021.empty and df_position_20171023.empty:
                lst2.append([BANDA, RPON_BANDA, LINHA, RPON_LINHA, TV, RPON_TV, DOCUMENTO, RPON_END, ID_FIBRA, 
                nrc, serial, fabricante, modelo, mac, psa, aprovisionamento, sigres_df_igual,
                '', '', '', '', '', '', '', '', '', '', '', '','', '', 
                    '', '', '', '','', '','', '', '', '', '', '', '', '','', '', '', 
                    '', '', '', '', '','', '', '', '', '', '', '', '','', '', '', '', 
                    '', '', '', '','', '', '', '', '', '', '', '','', '', '', '', '', 
                    '', '', '','', '', '', '', '', '', '', '','', '', '', '','', '', '', 
                    
                    '', '', '', '', '', '', '', '', '', '', '', '','', '', 
                    '', '', '', '','', '','', '', '', '', '', '', '', '','', '', '', 
                    '', '', '', '', '','', '', '', '', '', '', '', '','', '', '', '', 
                    '', '', '', '','', '', '', '', '', '', '', '','', '', '', '', '', 
                    '', '', '','', '', '', '', '', '', '', '','', '', '', '','', '', ''])
            else:
                lst1.append([BANDA, RPON_BANDA, LINHA, RPON_LINHA, TV, RPON_TV, DOCUMENTO, RPON_END, ID_FIBRA, nrc, serial, fabricante, modelo, mac, psa, aprovisionamento, sigres_df_igual])
                lst2 = np.hstack((lst1, a, b))
                logar('lst2 ok', lst2.shape)

            lst.append(lst2[0])

        except Exception as e:
            logar('Erro na iteração principal', e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logar(exc_type, fname, exc_tb.tb_lineno)

        contador += 1
        logar('----------------------------------------------------------------------------------------------------------')
    
    logar('----------------------------------------------------------------------------------------------------------\
    Sair\
    ----------------------------------------------------------------------------------------------------------')
    logar('tamanho', len(lst), 'qtd parametros', )
    return pd.DataFrame(lst, columns=cols)
    

log_out = open('log_out_fera.txt', 'a')
def logar(msg, *args):
    valor = str(datetime.datetime.now()) + ': ' +  str(msg)

    for arg in args:
        valor = valor + ' ' + str(arg)

    valor = valor + '\n'
    
    print(valor)
    log_out.write(valor)

df = executar()
logar('----------------------------------------------------------------------------------------------------------\
Gravando\
----------------------------------------------------------------------------------------------------------')
df.to_csv('executado20171108.csv', sep=';', encoding='utf-8')
logar('----------------------------------------------------------------------------------------------------------')