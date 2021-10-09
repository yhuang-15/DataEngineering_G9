# MLP for Pima Indians Dataset saved to single file
# see https://machinelearningmastery.com/save-load-keras-deep-learning-models/
import logging
import os

from flask import jsonify
from google.cloud import storage

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

def train(dataset):
    # split into input (X) and output (Y) variables
    X = dataset[:, 0:-1]
    y = dataset[:, -1]

    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.8)
    # define model
    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)

    scores = clf.score(X_test, y_test)
    print(scores)

    text_out = {
        "accuracy:": scores
    }

    project_id = os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
    model_repo = os.environ.get('MODEL_REPO', 'Specified environment variable is not set.')
    if model_repo:
        # Save the model localy
        with open("local_random_forest_model.pkl", "wb") as file:
            pickle.dump(model, file)
        # Save to GCS as random_forestn_model.pkl
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(model_repo)
        blob = bucket.blob('random_forest_model.pkl')
        # Upload the locally saved model
        blob.upload_from_filename('local_random_forest_model.pkl')
        # Clean up
        os.remove('local_random_forest_model.pkl')
        logging.info("Saved the model to GCP bucket : " + model_repo)
        return jsonify(text_out), 200
    else:
        with open("local_random_forest_model.pkl", "wb") as file:
            pickle.dump(model, file)
        return jsonify({'message': 'The model was saved locally.'}), 200
