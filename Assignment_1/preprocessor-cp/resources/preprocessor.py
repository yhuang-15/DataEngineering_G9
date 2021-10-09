
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

    clean_data = np.append(X_clean, y, axis=1)

    return clean_data