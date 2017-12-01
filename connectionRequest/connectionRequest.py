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

log_out = open('log_out.txt', 'a')

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

#client_sync_prod = Client('http://10.113.64.1:7025/SynchDeviceOpsImpl/SynchDeviceOperationsNBIService?wsdl',
#                          wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi01_lab = Client('http://200.168.104.216:7025/remotehdm/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

#client_nbi01_prod = Client('http://10.113.64.1:7025/remotehdm/NBIService?wsdl',
#                           wsse=UsernameToken('nbi_acesso', '@Telefonic@*17'))

#client_nbi02_lab = Client('http://200.168.104.216:7025/NBIServiceImpl/NBIService?wsdl',
#                          wsse=UsernameToken('g0031273', 'Fernando*24'))

client_nbi02_prod = Client('http://10.113.64.1:7025/NBIServiceImpl/NBIService?wsdl',
                           wsse=UsernameToken('g0031273', 'Blumen@u*17'))

#VIP Gerencia: 10.113.64.1:7025
#Link normal: 10.223.190.36:17025

#nbi01 = client_sync_prod
#nbi02 = client_nbi01_prod
nbi03 = client_nbi02_prod

def consultar_motive(guid):

    try:
        print("Teste")
        retorno = nbi03.service.findDeviceByGUID(guid)
        print(retorno)
        print("Caramba")
        #deviceModel = retorno[0]['deviceId']['productClass']
        #deviceGUID = retorno[0]['deviceGUID']

        #return deviceGUID

    except Exception as e:
        print('Erro ao consultar a motive', e)
        return

consultar_motive('17830478')