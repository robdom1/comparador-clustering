#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pymongo import MongoClient
from fuzzywuzzy import fuzz
import numpy as np
from sklearn.cluster import AgglomerativeClustering
import os

# Crear una instancia del cliente de MongoDB
client = MongoClient(os.getenv('MONGO_URL'))

# Acceder a la base de datos y la colección específicas
db = client.proyecto_final
collection = db['cleaned_data']


# Obtener los datos y cargarlos en un DataFrame de pandas
data = list(collection.find()) 
df = pd.DataFrame(data)

df = df[['_id','nombre','tienda']]

nombre_values = df["nombre"].values
tienda_values = df["tienda"].values
similarity_matrix = np.zeros((len(nombre_values), len(nombre_values)))


for i in range(len(nombre_values)):
    for j in range(len(nombre_values)):
        if tienda_values[i] == tienda_values[j]:
            similarity_matrix[i, j] = 0
        else:
            similarity_matrix[i, j] = fuzz.token_set_ratio(nombre_values[i], nombre_values[j])
    print(str(i) + ": " + nombre_values[i])


distance_matrix = 100 - similarity_matrix

clustering_model = AgglomerativeClustering(n_clusters=None, distance_threshold=2, affinity='precomputed', linkage='single')
clustering_model.fit(distance_matrix)
df['cluster_label'] = clustering_model.labels_

clustersDf = df.groupby("cluster_label").agg({"_id":lambda x: list(x), 'nombre':"first"})
clustersDf.rename(columns={"_id":"productos"}, inplace=True)

for index, row in clustersDf.iterrows():
        row["productos"] = [db["cleaned_data"].find_one({"_id":_id}) for _id in row["productos"]]
        db["Productos"].update_one({"nombre": row["nombre"]}, {"$set": {"productos": row["productos"]}}, upsert=True)


