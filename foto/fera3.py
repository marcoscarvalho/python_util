import pandas as pd
import datetime

nrc = 'A0031431'

html = '''
<table border="1" width="200%"><tr><td colspan="18" align="center"><strong>Hist&oacute;rico de aprovisionamento cliente A0000B3CAD - 15 Ultimos registros</strong></td></tr>
<tr align="center"><td >Data Operacao</td><td >Tipo</td><td >Status</td><td >Nrc Novo</td><td >Nrc Antigo</td><td >Terminal Novo</td><td >Terminal Antigo</td><td >Nas Ip Novo</td><td >Nas Ip Antigo</td><td >Nas Port Novo</td><td >Nas Port  Antigo</td><td >Servico Novo</td><td >Servico Antigo</td><td >IpPc</td><td >IpWan</td><td >Tipo Nas</td><td >Desc. Falha</td></tr>
<tr align="center"><td >09/11/2017 09:46:14,35174</td><td >Baixa</td><td >OK</td><td >null</td><td >0A0000B3CAD</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >Operation Successful</td></tr>
<tr align="center"><td >08/11/2017 09:46:14,35174</td><td >Baixa</td><td >OK</td><td >null</td><td >-0A0000B3CAD</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >Operation Successful</td></tr>
<tr align="center"><td >24/07/2017 09:46:14,35174</td><td >Alta</td><td >OK</td><td >0A0000B3CAD</td><td >-0A0000B3CAD</td><td >113816113104400</td><td >null</td><td >187.100.231.8</td><td >null</td><td >2451080671</td><td >null</td><td >221</td><td >null</td><td >null</td><td >null</td><td >4</td><td >Operation Successful</td></tr>
<tr align="center"><td >13/07/2017 12:38:21,45501</td><td >Alta</td><td >OK</td><td >-0A0000B3CAD</td><td >-0A0000B3CAD</td><td >-113816113104400</td><td >null</td><td >187.100.231.8</td><td >null</td><td >2451080671</td><td >null</td><td >221</td><td >null</td><td >null</td><td >null</td><td >4</td><td >Operation Successful</td></tr>
<tr align="center"><td >13/07/2017 12:18:26,44306</td><td >Baixa</td><td >OK</td><td >-0A0000B3CAD</td><td >0A0000B3CAD</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >Operation Successful</td></tr>
<tr align="center"><td >13/07/2017 12:18:26,44306</td><td >Alta</td><td >OK</td><td >0A0000B3CAD</td><td >0A0000B3CAD</td><td >113816113104400</td><td >null</td><td >187.100.231.8</td><td >null</td><td >2149090783</td><td >null</td><td >221</td><td >null</td><td >null</td><td >null</td><td >4</td><td >Operation Successful</td></tr>
<tr align="center"><td >13/07/2017 12:18:26,44306</td><td >Baixa</td><td >OK</td><td >0A0000B3CAD</td><td >-0A0000B3CAD</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >null</td><td >Operation Successful</td></tr>
<tr align="center"><td >09/01/2017 13:52:59,49979</td><td >Alta</td><td >OK</td><td >-0A0000B3CAD</td><td >-0A0000B3CAD</td><td >-113816113104407</td><td >null</td><td >187.100.231.8</td><td >187.100.231.8</td><td >2149090783</td><td >2149090783</td><td >221</td><td >221</td><td >null</td><td >null</td><td >4</td><td >null</td></tr>
<tr align="center"><td >30/11/2016 19:17:18,69438</td><td >Alta</td><td >OK</td><td >0A0000B3CAD</td><td >-0A0000B3CAD</td><td >113816113104407</td><td >null</td><td >187.100.231.8</td><td >187.100.231.8</td><td >1343784415</td><td >1343784415</td><td >221</td><td >-1</td><td >null</td><td >null</td><td >4</td><td >null</td></tr>
</table>
'''
df = pd.read_html(html, header=1)[0]
df['Data Operacao'] = pd.to_datetime(df['Data Operacao'], format='%d/%m/%Y %H:%M:%S,%f')
#print(df['Data Operacao'])
df = df[(df['Data Operacao'] >= '2017-11-01' )]
df = df[(df['Data Operacao'] <= '2017-11-12' )]
#df.to_csv('executado201711081234123455.csv', sep=';', encoding='utf-8')
#print(df['Data Operacao'])

df['NRC'] = nrc
print(df)