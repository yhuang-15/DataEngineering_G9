import os

import requests
from flask import Flask, Response
import json

from resources import download_sklearn_data

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/data-ingestion', methods=['PUT'])
def save_data():
    db_api = os.environ['TRAININGDB_API']

    content = download_sklearn_data.load()

    # Make a PUT request to training db service to store data into the training data/features.
    headers = {"Content-Type": "application/json"}
    response = requests.request("PUT", db_api, json=content, headers=headers)

    return response.content


app.run(host='0.0.0.0', port=5000)
