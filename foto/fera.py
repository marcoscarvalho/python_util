import datetime

entrada = '''
		|  |  |  | Historico de aprovisionamento cliente A0000B3CAD - 15 Ultimos registros |  |  | 

 |  | Data Operacao |  | Tipo |  | Status |  | Nrc Novo |  | Nrc Antigo |  | Terminal Novo |  | Terminal Antigo |  | Nas Ip Novo |  | Nas Ip Antigo |  | Nas Port Novo |  | Nas Port  Antigo |  | Servico Novo |  | Servico Antigo |  | IpPc |  | IpWan |  | Tipo Nas |  | Desc. Falha |  | 

 |  | 24/07/2017 09:46:14,35174 |  | Baixa |  | OK |  | null |  | 0A0000B3CAD |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | Operation Successful |  | 

 |  | 24/07/2017 09:46:14,35174 |  | Baixa |  | OK |  | null |  | -0A0000B3CAD |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | Operation Successful |  | 

 |  | 24/07/2017 09:46:14,35174 |  | Alta |  | OK |  | 0A0000B3CAD |  | -0A0000B3CAD |  | 113816113104400 |  | null |  | 187.100.231.8 |  | null |  | 2451080671 |  | null |  | 221 |  | null |  | null |  | null |  | 4 |  | Operation Successful |  | 

 |  | 13/07/2017 12:38:21,45501 |  | Alta |  | OK |  | -0A0000B3CAD |  | -0A0000B3CAD |  | -113816113104400 |  | null |  | 187.100.231.8 |  | null |  | 2451080671 |  | null |  | 221 |  | null |  | null |  | null |  | 4 |  | Operation Successful |  | 

 |  | 13/07/2017 12:18:26,44306 |  | Baixa |  | OK |  | -0A0000B3CAD |  | 0A0000B3CAD |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | Operation Successful |  | 

 |  | 13/07/2017 12:18:26,44306 |  | Alta |  | OK |  | 0A0000B3CAD |  | 0A0000B3CAD |  | 113816113104400 |  | null |  | 187.100.231.8 |  | null |  | 2149090783 |  | null |  | 221 |  | null |  | null |  | null |  | 4 |  | Operation Successful |  | 

 |  | 13/07/2017 12:18:26,44306 |  | Baixa |  | OK |  | 0A0000B3CAD |  | -0A0000B3CAD |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | null |  | Operation Successful |  | 

 |  | 09/01/2017 13:52:59,49979 |  | Alta |  | OK |  | -0A0000B3CAD |  | -0A0000B3CAD |  | -113816113104407 |  | null |  | 187.100.231.8 |  | 187.100.231.8 |  | 2149090783 |  | 2149090783 |  | 221 |  | 221 |  | null |  | null |  | 4 |  | null |  | 

 |  | 30/11/2016 19:17:18,69438 |  | Alta |  | OK |  | 0A0000B3CAD |  | -0A0000B3CAD |  | 113816113104407 |  | null |  | 187.100.231.8 |  | 187.100.231.8 |  | 1343784415 |  | 1343784415 |  | 221 |  | -1 |  | null |  | null |  | 4 |  | null |  | 

 | 
'''.replace(" |  | ","	")

entrada.split("\n")

format = "%d/%m/%Y"

t = datetime.date.today()

dataParametro = "26/07/2017"

dataConvertida = datetime.datetime.strptime(dataParametro, '%d/%m/%Y')

dia=1

datas = []

while dia <= 7:
	dataFim = datetime.timedelta(days=dia)
	datas.append((dataConvertida - dataFim))
	dia+=1

cont=0
i=0

listaHistorico = []

try:
    for line in entrada.splitlines():
    	if line.rstrip():
    		cont+=1
    		if cont > 2:
    			while i<len(datas):
    				i+=1
    				valor = line[1:11]
    				if datas[i].strftime(format) == valor:
    					print (line)
    					listaHistorico.append(line)
    					i=0
    					break
except Exception as e:
    print(e)
	
return listaHistorico