# coding: utf-8

import pandas as pd
import numpy as np

#filename = 'BaseACSCompleta-20171002.csv'
#chunksize = 10 ** 6
#for chunk in pd.read_csv(filename, chunksize=chunksize, encoding='ISO-8859-1', sep=';', dtype=str):
#    print(chunk)


#SERIALNUMBER | DEVICE_TYPE_NAME | MANUFACTURER | MODEL_NAME | NRC | VENDORCONFIGFILENUMBEROFENTRIES,MACADDR
dfacs = pd.read_csv('BaseACSFibra-20171002.csv', encoding='ISO-8859-1', sep=';', dtype=str, usecols=[1,2,9,11,16,26], index_col=16)
#dfacs['NRC'] = dfacs['NRC'].fillna('nulo')
#print('columns', dfacs.columns)
#print('shape', dfacs.shape)
#print('index', dfacs.index)

print(dfacs)
#print('Antes da modificação ---------------------------')
#dfacs = dfacs[pd.notnull(dfacs['NRC'])]
#print('Depois da modificação ---------------------------')
#print('columns', dfacs.columns)
#print('shape', dfacs.shape)
#print('index', dfacs.index)

'''
dfequip = pd.read_csv('executadoTudo.csv', encoding='ISO-8859-1', sep=';', dtype=str)
#print(dfequip)

for index, row in dfequip.iterrows():
    serial = row['serial_home_gateway']
    mac = row['macaddress_home_gateway']
    rpon = row['rpon']

    df_ok = dfacs[dfacs['NRC'].str.contains(rpon)]
    print("rpon {} - serial {}, mac address {} + df_ok {}".format(rpon, serial, mac, df_ok))
'''