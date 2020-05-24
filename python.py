# -*- coding: utf-8 -*-
"""
Created on Sun May 24 15:00:46 2020

@author: Leonardo Pilati Milos
Ra:221170129
"""
import concurrent.futures
import time
import shutil
def cnpj(numeros):
#   variaveis para calcular o digito verificador
    cont=5
    cont2=6
    primeiro=0
    segundo=0
#    calcula o valor de ambos digitos verificadores
    for i in numeros:
        if cont==1:
            cont=9
        if cont2==1:
            cont2=9
        primeiro=primeiro+(cont*int(i))
        cont-=1
        segundo=segundo+(cont2*int(i))
        cont2-=1
#   primeiro digito:checa para ver se o resto da divisao por 11 e um numero abaixo de 10 caso seja ele sera o proprio numero verificador caso contrario o digito sera 0
    primeiro=(11-(primeiro%11))
    if primeiro>=10:
        primeiro=0
    segundo=segundo+(primeiro*2)
#   segundo digito:checa para ver se o resto da divisao por 11 e um numero abaixo de 10 caso seja ele sera o proprio numero verificador caso contrario o digito sera 0
    segundo=(11-(segundo%11))
    if segundo>=10:
        segundo=0
#    retorna o cnpj completo para funcao
    return "{}{}{}\n".format(numeros,primeiro,segundo)
    
def cpf(numeros):
#   variaveis para calcular o digito verificador
    primeiro=0
    segundo=0
    cont=10
    cont2=11
#    calcula o valor de ambos digitos verificadores
    for i in numeros:
        primeiro= primeiro + (cont*int(i))
        segundo=segundo + (cont2*int(i))
        cont-=1
        cont2-=1
#   primeiro digito:checa para ver se o resto da divisao por 11 e um numero abaixo de 10 caso seja ele sera o proprio numero verificador caso contrario o digito sera 0
    primeiro=((primeiro%11)-11)*(-1)
    if primeiro>=10:
        primeiro=0
    segundo=segundo+(primeiro*2)
#   segundo digito:checa para ver se o resto da divisao por 11 e um numero abaixo de 10 caso seja ele sera o proprio numero verificador caso contrario o digito sera 0
    segundo=((segundo%11)-11)*(-1)
    if segundo>=10:
        segundo=0
#    retorna o cpf completo para funcao
    return "{}{}{}\n".format(numeros,primeiro,segundo)
    
def cpf_cnpj(texto,saida):
#    recebe um vetor de cpf/cnpj e checa para ver se e cpf ou cnpj e envia para funcao que calcula cada um
    file1 = open('saida{}.txt'.format(saida),'a+')
    for i in texto:
        if len(i)==9:
#            calcula o digito verificador cpf e coloca no txt
            file1.write(cpf(i))
        else:
#            calcula o digito verificador cnpj e coloca no txt
            file1.write(cnpj(i))
if __name__ == '__main__':
#    variaveis para separar a funcao para o multiprocessamento
    teste=0
    process=[]
    vet=[]
#    salva o tempo que o codigo comecou a rodar
    now=time.time()
#    abre e a base de cpf/cnpj e salva em um vetor
    with open("BASEPROJETO.txt",'r') as base:
         cpfcnpj = [line.strip(' ').strip('\n') for line in base.readlines()]
#    separa em 4 grupos cada um com 300.000 cpf/cnpj
    vet.append(cpfcnpj[0:int(len(cpfcnpj)/4)])
    vet.append(cpfcnpj[int(len(cpfcnpj)/4):int(len(cpfcnpj)/2)])
    vet.append(cpfcnpj[int(len(cpfcnpj)/2):int((len(cpfcnpj)*3)/4)])
    vet.append(cpfcnpj[int((len(cpfcnpj)*3)/4):int(len(cpfcnpj))])
#    roda ate 4 processos por vez (max_workers=4) 
    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        cont=0
        process=[]
#        para cada um dos 300.000 cpf/cnpj roda um processo a parte
        for i in vet:
            cont+=1
            process.append(executor.submit(cpf_cnpj, i,cont))
#    printa o tempo que demorou para rodar o codigo
    print(time.time()-now)
#    concatena todos os txt gerador por cada um dos processos
    with open('final.txt','wb') as wfd:
        for f in ['saida1.txt','saida2.txt','saida3.txt','saida4.txt']:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd)

















