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
    "Nome",
    "DDD",
    "Telefone",
    "CEP",
    "Email",
]


def formatar_cnpj(cnpj):
    cnpj = str(cnpj).zfill(14)  
    return re.sub(r'(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', r'\1.\2.\3/\4-\5', cnpj)

df['CNPJ'] = (df['CNPJ BÁSICO'].astype(str)
                       + df['CNPJ ORDEM'].astype(str).str.zfill(4)
                       + df['CNPJ DV'].astype(str))

df['CNPJ'] = df['CNPJ'].apply(formatar_cnpj)

def formatar_telefone(ddd, telefone):
    telefone = re.sub(r'\D', '', telefone) 
    return f'({ddd}) {telefone[:4]}-{telefone[4:]}'

df['Telefone'] = df.apply(lambda row: formatar_telefone(str(row['DDD']), str(row['Telefone'])), axis=1)

dados = df.loc[:, ["CNPJ", "Nome", "Telefone", "CEP", "Email"]].to_dict(orient='records')

client = MongoClient(MONGO_URL)
db = client['desafio1']
collection = db['estabelecimentos9']

collection.insert_many(dados)

es = Elasticsearch(hosts=[os.getenv("ELASTICSEARCH_HOSTS")])

index_mapping = {
    "mappings": {
        "properties": {
            "CNPJ": {"type": "text"},
            "Nome": {"type": "text"},
            "Telefone": {"type": "text"},
            "CEP": {"type": "text"},
            "Email": {"type": "text"}
        }
    }
}

if not es.indices.exists(index=ES_INDEX):
    es.indices.create(index=ES_INDEX, body=index_mapping, ignore=400)

for dado in dados:
    dado.pop('_id', None)
    es.index(index=ES_INDEX, document=dado)
