from flask import Flask, json, request
import os
import requests

from resources.predictor import DigitsPredictor
import pandas as pd

app = Flask(__name__)
app.config["DEBUG"] = True
dp = DigitsPredictor()

@app.route('/digits_predictor', methods=['PUT'])
def refresh_model():
    return dp.download_model()


@app.route('/prediction-cp/results', methods=['POST'])
def predict():
    db_api_test = os.environ['LOAD_TEST_API']

    # load data from the database
    r = requests.get(db_api_test)
    j = r.json()
    df = pd.DataFrame.from_dict(j)
    resp = dp.predict(df)
    return resp


app.run(host='0.0.0.0', port=5005)
