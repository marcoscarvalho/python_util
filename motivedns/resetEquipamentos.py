# coding: utf-8

import pandas as pd
import numpy as np
import threading
import time
import requests
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.exceptions import Fault, TransportError, XMLSyntaxError
import sys
import json
import urllib
import http
import cx_Oracle

ipunif_user = 'PROFILING_OWNER'
ipunif_pass = 'ProfilingVivo123'
ipunif_db_alias = 'IPUNIF'

ipunif_select = '''
select a.subscriberid
  from profiling_owner.baseacscompleta_20171003 a
 where a.nrc = :nrcfera
'''

def execute_select(nrc):
    try:
        con = cx_Oracle.connect(ipunif_user, ipunif_pass, ipunif_db_alias)
        cur = con.cursor()
        cur.execute(ipunif_select, nrcfera=nrc)

        for subscriberid in cur:
            return subscriberid

    except Exception as e:
        print('Erro ao executar select', e)

log_out = open('log_out.txt', 'w')
df = pd.read_csv('C:\GIT\python_util\motivedns\clientes.csv', encoding='ISO-8859-1', sep=';', dtype=str)

def logar(msg):
    if isinstance(msg, str):
        print("String")
        print(msg)
        log_out.write(msg)
    elif isinstance(msg, Exception):
        print("Exception")
        print("{}".format(msg))
        log_out.write("{}".format(msg))
    else:
        print("else")
        print("{}".format(msg))
        log_out.write("{}".format(msg))


nBIOption = {'disableCaptureConstraint': 'true', 'executionTimeoutSeconds': '1', 'expirationTimeoutSeconds': '600',
             'failOnConnectionRequestFailure': 'true', 'policyClass': 'test5555', 'priority': '100',
             'updateCachedDataRecord': 'false'}

#client_sync_lab = Client('http://200.168.104.216:7025/SynchDeviceOpsImpl/SynchDeviceOperationsNBIService?wsdl',
#                         wsse=UsernameToken('g0031273', 'Fernando*24'))

client_sync_prod = Client('http://10.113.64.1:7025/SynchDeviceOpsImpl/SynchDeviceOperationsNBIService?wsdl',
                          wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi01_lab = Client('http://200.168.104.216:7025/remotehdm/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

client_nbi01_prod = Client('http://10.113.64.1:7025/remotehdm/NBIService?wsdl',
                           wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi02_lab = Client('http://200.168.104.216:7025/NBIServiceImpl/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

client_nbi02_prod = Client('http://10.113.64.1:7025/NBIServiceImpl/NBIService?wsdl',
                           wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#VIP Gerencia: 10.113.64.1:7025
#Link normal: 10.223.190.36:17025

nbi01 = client_sync_prod
nbi02 = client_nbi01_prod
nbi03 = client_nbi02_prod

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
        print('Erro ao fazer o parse no ACS', e)	

def mapear_cliente(response):
    try:
        #print('Status do processamento WebRadius: ', response.status, response.reason)
        q= response.read().decode("utf-8") 
        serial = retornar_string(q, 'ACS Numero Serial')
        modelo = retornar_string(q, 'ACS Modelo Modem')
        mac = retornar_string(q, 'ACS MAC-ADDR')
        IDFibra = retornar_string(q, 'ID Fibra')
        ClientId = retornar_string(q, 'ClientId')
        #print('Equipamento do cliente >> {} | mac: {} | serial: {} | ClientId: {} | ID Fibra: {}'.format(modelo, mac, serial, ClientId, IDFibra))

        return serial, mac, ClientId, IDFibra
    
    except Exception as e:
        print('Erro ao fazer o parse do retorno do WebRadius', e)

def recuperar_informacao_web_radius(nrc):
    try:
        data = urllib.parse.urlencode({'valor1': nrc, 'tipo': 'n', 'Submit' : 'Consultar'})
        h = http.client.HTTPConnection('10.18.77.146')
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        h.request('POST', '/cgi-bin/consulta_socket', data, headers)
        return h.getresponse()
        
    except Exception as e:
        print('Erro ao carregar url', e)

def consultar_motive(subscriberId):

    try:
        retorno = nbi03.service.findDevicesBySubscriberId(subscriberId.strip())
        #print(retorno)
        deviceModel = retorno[0]['deviceId']['productClass']
        deviceGUID = retorno[0]['deviceGUID']

        return deviceGUID

    except Exception as e:
        #print('Erro ao consultar a motive', e)
        return 

def reboot_motive(deviceGUID):

    try:
        reboot = nbi02.service.createSingleDeviceOperationByDeviceGUID(longVal=deviceGUID,
                                                                        nBIFunction={'functionCode': '1'},
                                                                        nBISingleDeviceOperationOptions=nBIOption)
        print('Solicitado boot via Connection Request para o dispositivo Motive: ' + str(deviceGUID))
        return True

    except Exception as e:
        #print('Falha reboot do deviceGUID', deviceGUID, e);
        return False

def arrumar_id_fibra(valor):
    return valor[14:].replace(" ", "")

def retornar_string_for_tupple(valor):
    try:
        return valor[0]
    except:
        return

contador = 0
for index, row in df.iterrows():
    if contador > 37:
        break

    nrc = row['INTEGRATION_ID']
    terminal = row['SERIAL_NUM']

    IDFibra = retornar_string_for_tupple(execute_select('0' + nrc))

    #serial, mac, ClientId, IDFibra = mapear_cliente(recuperar_informacao_web_radius(nrc))
    #IDFibra = arrumar_id_fibra(IDFibra)

    if not IDFibra:
        print(index, nrc, terminal, 'ID Fibra nulo no ACS')
        print('')
        continue

    deviceGUID = consultar_motive(IDFibra)
    if not deviceGUID:
        print(index, nrc, terminal, IDFibra, 'Não existe deviceGUID')
        print('')
        continue

    processado = reboot_motive(deviceGUID)

    print(index, nrc, terminal, IDFibra, deviceGUID, processado)
    print('')
    contador = contador + 1