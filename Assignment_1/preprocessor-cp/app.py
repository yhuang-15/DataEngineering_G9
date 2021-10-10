import os

import pandas as pd
import numpy as np
import requests
from flask import Flask, Response
import json

from flask import jsonify
from resources import preprocessor

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/preprocessing-cp/pca', methods=['PUT'])
def preprocess():
    db_api = os.environ['TRAININGDB_API']
    db_save_train_api = os.environ['SAVE_TRAIN_API']
    db_save_test_api = os.environ['SAVE_TEST_API']
    # Make a GET request to training db service to retrieve the training data/features.
    r = requests.get(db_api)
    j = r.json()
    df = pd.DataFrame.from_dict(j)

    # preprocess the data
    resp = preprocessor.clean(df.values)
    spilt_idx = int(len(resp) * 0.8)

    headers = {"Content-Type": "application/json"}
    response_train = requests.request("PUT", db_save_train_api, json=resp[:spilt_idx], headers=headers)
    response_test = requests.request("PUT", db_save_test_api, json=resp[spilt_idx:], headers=headers)

    if response_train.ok and response_test.ok:
        return response_train.content
    else:
        return json.dumps({'message': 'Tables update fail.'}, sort_keys=False, indent=4), 400


app.run(host='0.0.0.0', port=5002)
