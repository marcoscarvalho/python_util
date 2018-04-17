# -*- coding: utf-8 -*-

import threading
import time
import datetime
import requests
from requests.auth import HTTPBasicAuth
import csv
from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.exceptions import Fault, TransportError, XMLSyntaxError
import sys
import json

'''client_nbi02_prod = Client('http://200.168.104.216:7025/NBIServiceImpl/NBIService?wsdl',
                           wsse=UsernameToken('nbi_oss', '@Telefonic@*15'))'''

client_nbi02_prod = Client('http://10.113.64.1:7025/NBIServiceImpl/NBIService?wsdl',
                           wsse=UsernameToken('nbi_oss', '@Telefonic@*15'))

nbi03 = client_nbi02_prod

def consultar_motive(subscriberId):

    try:
        retorno = nbi03.service.createSingleFirmwareUpdateOperation('')

    except Exception as e:
        print('Erro ao consultar a motive', e)
        return 

consultar_motive('')