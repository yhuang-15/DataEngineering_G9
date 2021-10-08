import os

import pandas as pd
import numpy as np
import requests
from flask import Flask, Response

from flask import jsonify
from resources import preprocessor

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/preprocessing-cp/pca', methods=['POST'])
def train_models():
    db_api = os.environ['TRAININGDB_API']
    # Make a GET request to training db service to retrieve the training data/features.
    r = requests.get(db_api)
    j = r.json()
    df = pd.DataFrame.from_dict(j)
    resp = preprocessor.clean(df)
    resp_np = resp.to_numpy()
    response = requests.request("PUT", db_api, json=resp_np, headers=headers)

    #you might need to return response.content below
    return response




app.run(host='0.0.0.0', port=5000)
