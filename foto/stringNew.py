import datetime
import re

entrada = '''
Status  Data Start            Data Stop/Interim     Segundos   Login                     UserIpAddr      Dowload         Upload          Service  Ipv6_Delegated                 Ipv6_Wan                       Term. Cause       Radius            ClientID
Aberto  04/11/2017 20:02:01   07/11/2017 20:02:01   259200     cliente@cliente           201.43.140.98   17892687473     506860005       bid09                                                                  ............      br-psa-cis-rd02   108001316
Fechado 27/10/2017 20:01:57   04/11/2017 20:01:57   691200     cliente@cliente           177.45.190.XXX  36189059984     1473545597      bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 19/10/2017 20:01:52   27/10/2017 20:01:52   691200     cliente@cliente           191.254.50.XXX  39842499404     1094949187      bid09                                                                  NAS-Request       br-psa-pd-rd10    108001316
Fechado 15/10/2017 18:38:51   19/10/2017 20:01:32   350561     cliente@cliente           189.46.57.XXX   10405466541     430639812       bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 11/10/2017 02:48:21   14/10/2017 04:57:41   266960     cliente@cliente           187.34.98.XXX   7831474795      330152918       bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 03/10/2017 02:48:17   11/10/2017 02:48:16   691199     cliente@cliente           152.249.164.XXX 50886384675     1803092776      bid09                                                                  NAS-Request       br-psa-pd-rd10    108001316
Fechado 02/10/2017 18:50:27   03/10/2017 02:47:47   28640      cliente@cliente           177.45.242.XXX  3298243707      92463049        bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 02/10/2017 18:46:34   02/10/2017 18:48:14   100        cliente@cliente           187.56.234.XXX  26143           62479           bid09                                                                  NAS-Request       br-psa-pd-rd10    108001316
Fechado 26/09/2017 06:47:04   02/10/2017 18:38:27   561083     cliente@cliente           189.110.136.XXX 49076745429     1577480682      bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 26/09/2017 04:20:11   26/09/2017 06:43:31   8600       cliente@cliente           187.57.249.XXX  24881653        1835577         bid09                                                                  NAS-Request       br-psa-cis-rd02   108001316
Fechado 21/09/2017 08:23:08   26/09/2017 02:53:06   412198     cliente@cliente           201.42.30.XXX   8087001321      430870334       bid09                                                                  NAS-Request       br-psa-pd-rd09    108001316
Fechado 20/09/2017 18:32:55   21/09/2017 08:22:30   49775      cliente@cliente           201.92.95.XXX   692149811       63928318        bid09                                                                  NAS-Request       br-psa-pd-rd09    108001316
Fechado 19/09/2017 18:27:24   20/09/2017 18:32:44   86720      cliente@cliente           189.110.84.XXX  1637051304      102297010       bid09                                                                  NAS-Request       br-psa-pd-rd09    108001316
Fechado 18/09/2017 19:01:56   19/09/2017 18:27:16   84320      cliente@cliente           179.111.77.XXX  3132544716      148624550       bid09                                                                  NAS-Request       br-psa-pd-rd09    108001316
Fechado 16/09/2017 23:19:53   18/09/2017 19:01:13   157280     cliente@cliente           189.18.115.XXX  3625080756      227225936       bid09                                                                  NAS-Request       br-psa-pd-rd09    108001316


'''

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

format = "%d/%m/%Y"
dataParametro = "21/10/2017"
dataConvertida = datetime.datetime.strptime(dataParametro, '%d/%m/%Y')

cont=0
i=0
dia=1
datas = []

while dia <= 7:
	dataFim = datetime.timedelta(days=dia)
	datas.append((dataConvertida - dataFim))
	datas.append((dataConvertida + dataFim))
	dia+=1

listaHistorico = []

p01 = None
p02 = None
p03 = None
p04 = None
p05 = None
p06 = None
p07 = None
p08 = None
p09 = None
p10 = None
p11 = None
p12 = None
p13 = None
p14 = None

for line in entrada.splitlines():
	if line.rstrip():

		if find_str(line, "Status") >= 0:
			p01 = find_str(line, "Status")
			p02 = find_str(line, "Data Start")
			p03 = find_str(line, "Data Stop")
			p04 = find_str(line, "Segundos")
			p05 = find_str(line, "Login")
			p06 = find_str(line, "UserIpAddr")
			p07 = find_str(line, "Dowload")
			p08 = find_str(line, "Upload")
			p09 = find_str(line, "Service")
			p10 = find_str(line, "Ipv6_Delegated")
			p11 = find_str(line, "Ipv6_Wan")
			p12 = find_str(line, "Term. Cause")
			p13 = find_str(line, "Radius")
			p14 = find_str(line, "ClientID")

		for d in datas:
			if p02 > 0 and d.strftime(format) == line[p02:p03].split(" ")[0].strip():
				c01 = None
				c02 = None
				c03 = None
				c04 = None
				c05 = None
				c06 = None
				c07 = None
				c08 = None
				c09 = None
				c10 = None
				c11 = None
				c12 = None
				c13 = None
				c14 = None

				c01 = line[:p02].rstrip()
				c02 = line[p02:p03].rstrip()
				c03 = line[p03:p04].rstrip()
				c04 = line[p04:p05].rstrip()
				c05 = line[p05:p06].rstrip()
				c06 = line[p06:p07].rstrip()
				c07 = line[p07:p08].rstrip()
				c08 = line[p08:p09].rstrip()
				c09 = line[p09:p10].rstrip()
				c10 = line[p10:p11].rstrip()
				c11 = line[p11:p12].rstrip()
				c12 = line[p12:p13].rstrip()
				c13 = line[p13:p14].rstrip()
				c14 = line[p14:].rstrip()

				listaHistorico.append([c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14])

print(listaHistorico)