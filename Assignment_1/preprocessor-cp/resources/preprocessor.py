
import os

from flask import jsonify
from scipy.sparse import data
from sklearn.decomposition import PCA
import pandas as pd


def clean(dataset):
    # need to get the data to X and y separately
    y = dataset['label']
    X = dataset.drop(label="label")

    pca = PCA()

    pca.fit(X)
    X_clean = pca.transform(X)

    clean_data = X_clean.join(y)

    return clean_data