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

MAX_CONEXOES = 18

# Funcao para imprimir uma linha por vez via lock
print_lock = threading.Lock()
def mostrar_msg(msg):
    print_lock.acquire()
    print msg
    print_lock.release()

log_out = open('log_out.txt', 'w')

def changetoMotive(deviceId, designador, mac, serial, model):
    try:
        arris_prod_changetoMotive = 'http://10.200.6.150/nbbs/api/capability/execute?capability=' \
                                        '"changeURLtoMotivePm"&deviceId=' + deviceId
        ans = requests.post(arris_prod_changetoMotive, auth=HTTPBasicAuth('acesso_fixo', '@Telefonic@*15'))
        print ans.status_code
        if ans.status_code == 200:
            print 'Solicitado troca ACS via Connection Request changetoMotive: ' + str(deviceId)
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            log_out.write('MIGRADO' ';' + st + ';' + str(deviceId) + ';' + str(designador) + ';' + str(mac) + ';' + str(serial) +
                          ';' + str(model) + ';' 'ONLINE')
            log_out.write("\n")
        else:
            return 'offline'

    except:
        e = sys.exc_info()[1]
        print(e);
        print 'Houve excecao na busca do dispositivo: ' + str(deviceId)
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        #log_out.write('NAO MIGRADO' ';' + str(deviceId) + ';' + st + ';' 'ERRO NA CONSULTA')
        log_out.write('NAO MIGRADO' ';' + st + ';' + str(deviceId) + ';' + str(designador) + ';' + str(mac) + ';' + str(serial) +
                      ';' + str(model) + ';' 'ERRO NA CONSULTA')
        log_out.write("\n")
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % deviceId)



def changetoMotiveFull(deviceId):


    task = '"' 'id' '"' ':' '"' + deviceId + '"'
    arris_prod_list = 'http://10.200.6.150/nbbs/api/core/device/listDevices?offset=0&limit=5&criteria=''{' + task + '}'
    request_list = requests.post(arris_prod_list, auth = HTTPBasicAuth('g0031273', 'Joana*17'))
    ans_request_list = json.loads(request_list.content)
    num_devices = int(ans_request_list['length'])
    if num_devices != 0:
        for i in xrange(num_devices):
            if ans_request_list['results'][i]['classification'] == 'IAD':
                #deviceId = ans_request_list['results'][i]['id']
                subscriberId = ans_request_list['results'][i]['userKey3']
                deviceModel = ans_request_list['results'][i]['model']
                mac = ans_request_list['results'][i]['mac']
                serialNumber =  ans_request_list['results'][i]['serialNumber']
                arris_prod_status = 'http://10.200.6.150/nbbs/api/capability/getStatus?deviceId=' + deviceId
                ans = json.loads(requests.post(arris_prod_status, auth = HTTPBasicAuth('acesso_fixo', '@Telefonic@*15')).content)
                if ans['status'] == 'ok':
                    print 'Device Online'
                    arris_prod_changetoMotive = 'http://10.200.6.150:8080/nbbs/api/capability/execute?capability=' \
                                                '"changeURLtoMotivePm"&deviceId=' + deviceId
                    requests.post(arris_prod_changetoMotive, auth = HTTPBasicAuth('acesso_fixo', '@Telefonic@*15'))

                    print 'Solicitado boot via Connection Request changetoMotive: ' + str(deviceId)
                    ts = time.time()
                    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    log_out.write('MIGRADO' ';' + str(deviceId) + ';' + str(subscriberId) + ';' + str(deviceModel) +
                                  ';' + str(mac) + ';' + str(serialNumber) + ';' + st + ';' 'ONLINE')
                    log_out.write("\n")
                else:
                    print 'Dispositivo offline na plataforma Arris: ' + str(deviceId)
                    ts = time.time()
                    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                    log_out.write('NAO MIGRADO' ';' + str(deviceId) + ';' + str(subscriberId) + ';' + str(deviceModel) +
                                  ';' + str(mac) + ';' + str(serialNumber) + ';' + st + ';' 'OFFLINE')
                    log_out.write("\n")
    else:
        return 'offline'


def consultar_device(deviceId, designador, mac, serial, model):

    mostrar_msg("Consultando o deviceId '%s'..." % deviceId)
    try:
        result = changetoMotive(deviceId, designador, mac, serial, model)
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % deviceId)
        if (result == 'offline'):
            print 'Dispositivo OFFLINE: ' + str(deviceId)
            ts = time.time()
            st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            #log_out.write('NAO MIGRADO' ';' + str(deviceId) + ';' + st + ';' 'DISPOSITIVO OFFLINE')
            log_out.write('NAO MIGRADO' ';' + st + ';' + str(deviceId) + ';' + str(designador) + ';' + str(mac) + ';' + str(serial) +
                          ';' + str(model) + ';' 'DISPOSITIVO OFFLINE')
            log_out.write("\n")
            mostrar_msg("Terminada consulta do susbcriberId '%s'" % deviceId)
    except:
        e = sys.exc_info()[1]
        print(e);
        print 'Houve excecao na busca do dispositivo: ' + str(deviceId)
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        #log_out.write('NAO MIGRADO' ';' + str(deviceId) + ';' + st + ';' 'ERRO NA CONSULTA')
        log_out.write('NAO MIGRADO' ';' + st + ';' + str(deviceId) + ';' + str(designador) + ';' + str(mac) + ';' + str(serial) +
                      ';' + str(model) + ';' 'ERRO NA CONSULTA')
        log_out.write("\n")
        mostrar_msg("Terminada consulta do susbcriberId '%s'" % deviceId)

# Thread Principal
lista_threads = []
count = 0

arquivo = csv.reader(open('lista_subscriber.txt'), delimiter = ';')
for linha in arquivo:
    deviceId = linha[0].strip()
    designador = linha[1].strip()
    mac = linha[2].strip()
    serial = linha[3].strip()
    model = linha[4].strip()
    count = count + 1
    print str(threading.active_count())
    print "MIGRANDO O DEVICE: " + str(count)
    while threading.active_count() > MAX_CONEXOES:
        mostrar_msg("Esperando 2s .....")
        time.sleep(5)
    thread = threading.Thread(target=consultar_device, args=(deviceId, designador, mac, serial, model, ))
    lista_threads.append(thread)
    thread.start()


# Esperando pelas threads abertas terminarem
mostrar_msg("Eperando pelas threads abertas terminare")
for thread in lista_threads:
    thread.join()