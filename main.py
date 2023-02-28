import pandas as pd
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
import re

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
ES_INDEX = os.getenv("ES_INDEX")

df = pd.read_csv("Estabelecimentos9.csv", delimiter=";", header=None, encoding="latin-1", nrows=10)

df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7]].head(10)

df.columns = [
    "CNPJ BÁSICO",
    "CNPJ ORDEM",
    "CNPJ DV",
    "Nome Fantasia",
    "DDD",
    "TELEFONE",
    "CEP",
    "Email",
]


def formatar_cnpj(cnpj):
    cnpj = str(cnpj).zfill(14)  
    return re.sub(r'(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', r'\1.\2.\3/\4-\5', cnpj)

df['CNPJ COMPLETO'] = (df['CNPJ BÁSICO'].astype(str)
                       + df['CNPJ ORDEM'].astype(str).str.zfill(4)
                       + df['CNPJ DV'].astype(str))

df['CNPJ COMPLETO'] = df['CNPJ COMPLETO'].apply(formatar_cnpj)

def formatar_telefone(ddd, telefone):
    telefone = re.sub(r'\D', '', telefone) 
    return f'({ddd}) {telefone[:4]}-{telefone[4:]}'

df['TELEFONE'] = df.apply(lambda row: formatar_telefone(str(row['DDD']), str(row['TELEFONE'])), axis=1)

dados = df.loc[:, ["CNPJ COMPLETO", "Nome Fantasia", "TELEFONE", "CEP", "Email"]].to_dict(orient='records')

client = MongoClient(MONGO_URL)
db = client['desafio1']
collection = db['estabelecimentos9']

collection.insert_many(dados)

es = Elasticsearch(hosts=[os.getenv("ELASTICSEARCH_HOSTS")])
for dado in dados:
    es.index(index=ES_INDEX, body=dado)
