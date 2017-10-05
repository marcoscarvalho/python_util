# -*- coding: utf-8 -*-

import threading
import time
import requests
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.exceptions import Fault, TransportError, XMLSyntaxError
import sys
import json

MAX_CONEXOES = 10

# Funcao para imprimir uma linha por vez via lock
print_lock = threading.Lock()
def mostrar_msg(msg):
    print_lock.acquire()
    print msg
    print_lock.release()


log_out = open('log_out.txt', 'w')

nBIOption = {'disableCaptureConstraint': 'true', 'executionTimeoutSeconds': '1', 'expirationTimeoutSeconds': '600',
             'failOnConnectionRequestFailure': 'true', 'policyClass': 'test5555', 'priority': '100',
             'updateCachedDataRecord': 'false'}

#client_sync_lab = Client('http://200.168.104.216:7025/SynchDeviceOpsImpl/SynchDeviceOperationsNBIService?wsdl',
#                         wsse=UsernameToken('g0031273', 'Fernando*24'))

client_sync_prod = Client('http://10.223.190.36:17025/SynchDeviceOpsImpl/SynchDeviceOperationsNBIService?wsdl',
                          wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi01_lab = Client('http://200.168.104.216:7025/remotehdm/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

client_nbi01_prod = Client('http://10.223.190.36:17025/remotehdm/NBIService?wsdl',
                           wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi02_lab = Client('http://200.168.104.216:7025/NBIServiceImpl/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

client_nbi02_prod = Client('http://10.223.190.36:17025/NBIServiceImpl/NBIService?wsdl',
                           wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

VIP Gerencia: 10.113.64.1:7025


nbi01 = client_sync_prod
nbi02 = client_nbi01_prod
nbi03 = client_nbi02_prod

# FUNCAO setDNS

def setDns_motive(subscriberId, nbi01, nbi02, nbi03):

    try:
        findbySubscriber = nbi03.service.findDevicesBySubscriberId(subscriberId.strip())
        print findbySubscriber
        oui_value = findbySubscriber[0]['deviceId']['OUI']
        productClass_value = findbySubscriber[0]['deviceId']['productClass']
        protocol_value = findbySubscriber[0]['deviceId']['protocol']
        serialNumber_value = findbySubscriber[0]['deviceId']['serialNumber']


        arg0 = {'OUI': '001195', 'productClass': 'DSL-2730R', 'protocol': 'DEVICE_PROTOCOL_DSLFTR069v1',
                'serialNumber': 'R3K91DB001147'}
        print arg0
        arg1 = []
        arg2 = 9516
        nBIOption1 = {'disableCaptureConstraint': 'true', 'executionTimeoutSeconds': '1200',
                    'expirationTimeoutSeconds': '600','failOnConnectionRequestFailure': 'true',
                    'opaqueTransactionId': 'test5555', {dslfkdlfkl:kjashkdhs}
                    'policyClass': 'policytest','priority': '100', 'replaceDeviceCachedDataRecord': 'false',
                    'updateCachedDataRecord': 'true'}


        arg3 = nBIOption1
        arg4 = 10000




        nbi01.service.executeFunction(arg0, arg1, arg2, arg3, arg4)


        #setDns = nbi02.service.createSingleDeviceOperationByDeviceGUID(longVal=deviceGUID,
        #                                                                nBIFunction={'functionCode': '1'},
        #                                                               nBISingleDeviceOperationOptions=nBIOption)
        #print 'Solicitado boot via Connection Request para o dispositivo Motive: ' + str(deviceGUID)
        #log_out.write('OK' ';' + str(deviceGUID) + ';' + str(subscriberId) + ';' + deviceModel.strip() + ';' 'Motive')
        #log_out.write("\n")

    except:
        e = sys.exc_info()[1]
        print(e)
        log_out.write('NAO ENCONTRADO;' + str(subscriberId) + ';' 'N/A' ';' 'N/A' ';' 'N/A')
        log_out.write("\n")
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % subscriberId)



# FUNCAO REBOOT MOTIVE

def reboot_motive(subscriberId, nbi01, nbi02, nbi03):

    try:
        findbySubscriber = nbi03.service.findDevicesBySubscriberId(subscriberId.strip())
        deviceModel = findbySubscriber[0]['deviceId']['productClass']
        deviceGUID = findbySubscriber[0]['deviceGUID']

        try:
            reboot = nbi02.service.createSingleDeviceOperationByDeviceGUID(longVal=deviceGUID,
                                                                           nBIFunction={'functionCode': '1'},
                                                                           nBISingleDeviceOperationOptions=nBIOption)
            print 'Solicitado boot via Connection Request para o dispositivo Motive: ' + str(deviceGUID)
            log_out.write('OK' ';' + str(deviceGUID) + ';' + str(subscriberId) + ';' + deviceModel.strip() + ';' 'Motive')
            log_out.write("\n")


        except:
            e = sys.exc_info()[1]
            print(e);
            log_out.write('FALHA REBOOT: ' + str(deviceGUID) + ';' + str(subscriberId) + ';' + deviceModel.strip() +
                          ';' 'Motive')
            log_out.write("\n")
            mostrar_msg("Terminada consulta do susbcriberId '%s'" % arg0)

    except:
        e = sys.exc_info()[1]
        print(e)
        log_out.write('NAO ENCONTRADO;' + str(subscriberId) + ';' 'N/A' ';' 'N/A' ';' 'N/A')
        log_out.write("\n")
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % arg0)

# FUNCAO THREADING

def consultar_device(subscriberId, nbi01, nbi02, nbi03):

    mostrar_msg("Consultando o deviceId '%s'..." % subscriberId)
    try:
        setDns_motive(subscriberId, nbi01, nbi02, nbi03)
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % subscriberId)
    except:
        e = sys.exc_info()[1]
        print(e);
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % subscriberId)


# Thread Principal
lista_threads = []

with open('lista_subscriber.txt', 'rb') as arquivo:
    for linha in arquivo:
        arg0 = linha.strip()
        print str(threading.active_count())
        while threading.active_count() > MAX_CONEXOES:
            mostrar_msg("Esperando 2s .....")
            time.sleep(2)
        thread = threading.Thread(target=consultar_device, args=(arg0, nbi01, nbi02, nbi03,))
        lista_threads.append(thread)
        thread.start()

# Esperando pelas threads abertas terminarem
mostrar_msg("Eperando pelas threads abertas terminare")
for thread in lista_threads:
    thread.join()
