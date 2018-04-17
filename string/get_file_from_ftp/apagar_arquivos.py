# coding: utf-8

import datetime
import os
import shutil

def apagar(arquivo):
    try:
        os.remove(arquivo)
    except Exception as e:
        print('Erro no arquivo', arquivo, e)

data = datetime.date.today().strftime('%Y%m%d')

filename_acs = "BaseACSCompleta";
apagar('BASEACSCOMPLETA.ctl')
apagar('BASEACSCOMPLETA.log')
apagar(filename_acs + '-' + data + '.csv.gz')
apagar(filename_acs + '-' + data + '.csv')

filename_acs = "BaseACSCompleta" + "_" + data;
apagar('BASEACSCOMPLETA' + '_' + data + '.ctl')
apagar('BASEACSCOMPLETA' + '_' + data + '.log')