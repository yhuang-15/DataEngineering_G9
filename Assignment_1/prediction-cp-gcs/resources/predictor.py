import os

from flask import jsonify
from google.cloud import storage
import pickle
import numpy as np


class DigitsPredictor:
    def __init__(self):
        self.model1 = None
        self.model2 = None

    # download the model
    def download_models(self):
        project_id = os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
        model_repo = os.environ.get('MODEL_REPO', 'Specified environment variable is not set.')
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(model_repo)

        blob1 = bucket.blob('random_forest_model.pkl')
        blob2 = bucket.blob('logistic_regression_model.pkl')

        blob1.download_to_filename('local_random_forest_model.pkl')
        blob2.download_to_filename('local_logistic_regression_model.pkl')

        with open('local_random_forest_model.pkl', 'rb') as fp:
            self.model1 = pickle.load(fp)

        with open('local_logistic_regression_model.pkl', 'rb') as fp:
            self.model2 = pickle.load(fp)

        print('Models loaded successfully')

    # make prediction
    def predict(self, dataset):
        if self.model1 is None or self.model2 is None:
            self.download_models()

        val_set2 = dataset.copy()

        result1 = self.model1.predict_proba(dataset)
        result2 = self.model2.predict_proba(dataset)

        result = np.mean([result1, result2], axis=0)  # columns
    
        y_classes = result.argmax(axis=1)  # rows
        val_set2['class'] = y_classes.tolist()
        # print('prediction succesful')
        dic = val_set2.to_dict(orient='records')
        return jsonify(dic), 200
