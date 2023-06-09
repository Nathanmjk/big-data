# -*- coding: utf-8 -*-
"""TDE - Big data

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1O75GUJPbDoC6E6rmKMMin3_flqffHJqz

#Alunos: Nathan Machado Josviak e Carlos Eduardo Rodrigues
"""

!pip install pyspark
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession\
            .builder\
            .master('local[*]')\
            .appName('TDE-Spark')\
            .getOrCreate()
sc = spark.sparkContext

# CARREGANDO O ARQUIVO DO TDE
!wget https://jpbarddal.github.io/assets/data/bigdata/transactions_amostra.csv.zip
!unzip transactions_amostra.csv.zip
rdd = sc.textFile('transactions_amostra.csv')

print(rdd)

rdd.take(10)

# Exercicio 1 - The number of transactions involving Brazil

rdd_brazil = rdd.filter(lambda x: x.split(';')[1] == 'Brazil')

rdd_brazil.count()

# Exercicio 2 - The number of transactions per flow type and year


"""
 Aplicando a transformação map e usando a funcao lambda, onde é separado cada
elemento x por ";" e extraido os valores da posicao [4](flow type) e posicao [1](year).
A funcao retorna uma tupla representando chave-valor onde a chave é o flow type e
year, e o valor é 1.
"""
rdd_transictionE2 = rdd.map(lambda x: ((x.split(';')[4], x.split(';')[1]),1))



"""
 É aplicado a transformação ReduceByKey que agrupa os elementos por suas chaves flow type e year,
e adiciona os valores para cada chave e tambem é  aplicado a função lambda que soma os valores x e y
"""
resultE2 = rdd_transictionE2.reduceByKey(lambda x, y: x + y)

# result.take(5)


# for para imprimir o tipo , ano e a quantidade
for ((flow, year), qntd) in resultE2.take(15): # result até 15 elementos apenas
  print("Flow: {}, Year: {}, Quantidade: {}".format(flow, year, qntd))

# Exercicio 3 - The average of commodity values per year;


'''
Eliminando o cabeçalho se nao só vai apresentar erro na hora de executar o codigo,
'''
header = rdd.first()
rdd = rdd.filter(lambda row: row != header)

'''
Criando um pair-rdd e utilizando a funcao map e tambem separando os valores por ";"
para utilizar os valores da posicao 1 e 5
'''
pair_rdd = rdd.map(lambda row: (row.split(";")[1], float(row.split(";")[5])))


'''
Aplicando a funcao mapValues que  retorna uma tupla com dois elementos:
o valor original (x) e o número 1.
'''
sum_count_rdd = pair_rdd.mapValues(lambda x: (x, 1))

sum_count_by_year = sum_count_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

average_by_year = sum_count_by_year.mapValues(lambda x: x[0] / x[1])

average_by_year.take(10)

# Exercicio 4 - The average price of commodities per unit type, year, and category in the export flow
# in Brazil;

filter_rdd = rdd.filter(lambda x: x.split(";")[0] == "Brazil" and x.split(";")[4] == "Export")

pair_rdd = filter_rdd.map(lambda x: (
    (x.split(";")[7], x.split(";")[1], x.split(";")[9]),
    float(x.split(";")[5])
))

# realizando a media dos pair-rdd
AVG = pair_rdd.aggregateByKey(
    (0.0, 0),
    lambda x, value: (x[0] + value, x[1] + 1),
    lambda x, y: (x[0] + y[0], x[1] + y[1])
).mapValues(lambda x: x[0] / x[1])

AVG.take(5)

# Exercicio 5 - The maximum, minimum, and mean transaction price per unit type and year

# Filtrando linhas por fluxo de exportação no Brasil
filtered_rdd = rdd.filter(lambda row: row.split(";")[0] == "Brazil" and row.split(";")[4] == "Export")

# Criando um par de chave-valor com tipo, ano e valor
pair_rdd = filtered_rdd.map(lambda row: (
    (row.split(";")[7], row.split(";")[1]),
    float(row.split(";")[5])
))

# Calculando o máximo, mínimo e médio
result = pair_rdd.aggregateByKey(
    (float('-inf'), float('inf'), 0.0, 0),
    lambda acc, value: (
        max(acc[0], value),
        min(acc[1], value),
        acc[2] + value,
        acc[3] + 1
    ),
    lambda acc1, acc2: (
        max(acc1[0], acc2[0]),
        min(acc1[1], acc2[1]),
        acc1[2] + acc2[2],
        acc1[3] + acc2[3]
    )
).mapValues(lambda acc: (acc[0], acc[1], acc[2] / acc[3]))

# Exibir o resultado
result.take(15)

# Exercicio 6 - The country with the largest average commodity price in the Export flow;

# Filtrando linhas por fluxo de exportação
filter_rdd = rdd.filter(lambda x: x.split(";")[4] == "Export")

# Criando um par de chave-valor com país e o valor
pair_rdd = filter_rdd.map(lambda x: (x.split(";")[0], float(x.split(";")[5])))

# Calculando a média de preço por país
AVG = pair_rdd.aggregateByKey(
    (0.0, 0),
    lambda x, value: (x[0] + value, x[1] + 1),
    lambda x, y: (x[0] + y[0], x[1] + y[1])
).mapValues(lambda x: x[0] / x[1])

# Encontrando o país com o maior preço médio
max_avg = AVG.max(lambda x: x[1])

# Exibindo o resultado
print(f"País com preço médio mais alto: {max_avg[0]}")
print(f"Preço médio mais alto: {max_avg[1]}")