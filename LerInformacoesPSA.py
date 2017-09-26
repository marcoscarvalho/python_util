# coding: utf-8

import numpy
import pandas as pd
import pandasql
import urllib
import http


def filter_by_regular(filename):

    turnstile_data = pd.read_csv(filename, encoding='ISO-8859-1')
    q = """
    select LINHA from turnstile_data;
    """
    return pandasql.sqldf(q, locals())
    
    
def filter(file, field):

    turnstile_data = file
    q = """
    select """ + field + """ from turnstile_data;
    """
    return pandasql.sqldf(q, locals())
    
    
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
        print('Erro ao carregar url', e)

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
        print('Erro ao retirar_colchetes', e)	

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
        print('Status do processamento WebRadius: ', response.status, response.reason)
        q= response.read().decode("utf-8") 
        serial = retornar_string(q, 'ACS Numero Serial')
        fabrincate = retornar_string(q, 'ACS Fabr. Modem')
        modelo = retornar_string(q, 'ACS Modelo Modem')
        mac = retornar_string(q, 'ACS MAC-ADDR')
        print('Equipamento do cliente >> {} | mac: {} | serial: {}'.format(modelo, mac, serial));

        p_in_psa = find_str(q, 'estamos ocultando o IP do cliente')
        p_in_aprovisionamento = find_str(q, '<table border="1" width="200%"><tr><td colspan="18" align="center"><strong>Hist')
        p_out_aprovisionamento = find_str(q, '</table><br>')
        psa = q[(p_in_psa+36):p_in_aprovisionamento]
        aprovisionamento = q[p_in_aprovisionamento:(p_out_aprovisionamento+8)]
        
        print('Informações de PSA')
        print(psa)
        print('')
        print('Inicio aprovisionamento')
        print(retirar_colchetes(aprovisionamento, ' | '))
    
    except Exception as e:
        print('Erro ao fazer o parse do retorno do WebRadius', e)

a = read_excel('TblProblemaLIRS.xlsx', 'TblProblemaLIRS')

for index, row in a.iterrows():
    print('Cliente possui terminal {}, NRC {}, status {}, criação {}, modificacao {}'.format(row['LINHA'], row['RPON'], row['ISTATUS'], row['DATE_CREATED'], row['DATE_MODIFIED']))    
    mapear_cliente(recuperar_informacao_web_radius(row['RPON']))
    print('')