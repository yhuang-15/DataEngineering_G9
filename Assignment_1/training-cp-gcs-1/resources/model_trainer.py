import logging
import os

from flask import jsonify
from google.cloud import storage
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
import pickle


def train(dataset):
    # split into input (X) and output (Y) variables
    X = dataset[:, :-1]
    Y = dataset[:, -1]

    X_train, X_test, y_train, y_test = train_test_split(X, Y, train_size=0.8)
    # define model
    model = LogisticRegression(max_iter=10000, tol=0.1)

    # Fit the model
    model.fit(X_train, X_train)
    # evaluate the model
    scores = model.score(X_test, y_test)

    text_out = {
        "accuracy:": scores,
    }

    # Saving model in a given location provided as an env. variable
    project_id = os.environ.get('PROJECT_ID', 'Specified environment variable is not set.')
    model_repo = os.environ.get('MODEL_REPO', 'Specified environment variable is not set.')
    if model_repo:
        # Save the model localy
        with open("local_logistic_regression_model.pkl", "wb") as file:
            pickle.dump(model, file)
        # Save to GCS as logistic_regression_model.pkl
        client = storage.Client(project=project_id)
        bucket = client.get_bucket(model_repo)
        blob = bucket.blob('logistic_regression_model.pkl')
        # Upload the locally saved model
        blob.upload_from_filename('local_logistic_regression_model.pkl')
        # Clean up
        os.remove('local_logistic_regression_model.pkl')
        logging.info("Saved the model to GCP bucket : " + model_repo)
        return jsonify(text_out), 200
    else:
        with open("local_logistic_regression_model.pkl", "wb") as file:
            pickle.dump(model, file)
        return jsonify({'message': 'The model was saved locally.'}), 200
