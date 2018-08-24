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

apagar('BASEACSCOMPLETA.ctl')
apagar('BASEACSCOMPLETA.log')

apagar('BaseACSCompleta-' + data + '.csv.gz')
apagar('BaseACSCompleta-' + data + '.csv')
apagar('BaseACSCompleta-' + data + '.bad')

apagar('BASEACSCOMPLETA' + '_' + data + '.ctl')
apagar('BASEACSCOMPLETA' + '_' + data + '.log')

apagar('sad.ctl')
apagar('sad.log')

apagar('sad_' + data + '.ctl')
apagar('sad_' + data + '.log')

apagar('SIGRES_BANDA_FIBRA.ctl')
apagar('SIGRES_BANDA_FIBRA.log')

apagar('servicos_ativos_detalhado_' + data + '.zip')
shutil.rmtree('export')