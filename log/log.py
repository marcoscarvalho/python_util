def logar(msg, *args, arquivoLog):
    valor = str(msg)

    for arg in args:
        valor = ' ' + str(arg)
    
    print(valor)

    if arquivoLog is not None
        arquivoLog.write(valor)