# coding: utf-8

import numpy as np
import pandas as pd
import urllib
import http
import datetime
import threading
import time
import sys, os
import cx_Oracle

def read_html_from_str(html):
    df = pd.read_html(html, header=0)[0]
    #df['Data Operacao'] = pd.to_datetime(df['Data Operacao'], format='%d/%m/%Y %H:%M:%S,%f')
    #df = df[(df['Data Operacao'] >= data_anterior )]
    #df = df[(df['Data Operacao'] <= data_posterior )]
    #df.to_csv('executado201711081234123455.csv', sep=';', encoding='utf-8')
    #print(df['Data Operacao'])
    #df['NRC'] = nrc
    return df

df = read_html_from_str("d_megasc.htm")
df2 = df[df.columns[2:8]]
print(df2)
df3 = df2[np.isfinite(df2['1ª Dezena'])]
print('-----------------------------------------------')
print(df3)
df3.to_csv('mega.csv', sep=';', encoding='utf-8')