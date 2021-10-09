
import os

from flask import jsonify
from scipy.sparse import data
from sklearn.decomposition import PCA
import pandas as pd

def clean(dataset):
    # need to get the data to X and y separately
    y = dataset[:,-1]
    X = dataset[:, :-1]

    pca = PCA(n_components=30)

    pca.fit(X)
    X_clean = pca.transform(X)
    y_2d = np.reshape(y, (-1, 1))

    clean_data = np.hstack((X_clean, y_2d))

    data_list = []
    for row in clean_data:
        dict_1 = {f"pix_{i}": row[i] for i in range(row.shape[0] - 1)}
        dict_1["y"] = row[-1]

        data_list.append(dict_1)
    return data_list
