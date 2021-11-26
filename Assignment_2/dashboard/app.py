import pandas as pd
from flask import Flask, json, request, jsonify
# from matplotlib.
from google.cloud import bigquery

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/test', methods=['GET'])
def print_results():
    test_dic = load_table_from_BQ('top_delay')
    return jsonify(test_dic, 200)



def load_table_from_BQ(table_name):
    client = bigquery.Client(project="jads-de-2021")  

    # Perform a query.
    QUERY = (
        f'SELECT * FROM `jads-de-2021.assignment_2.{table_name}` LIMIT 100')   # use the correct project id, etc.
    query_job = client.query(QUERY)  # API request
    df_result = query_job.to_dataframe().to_dict()
    #df_result = query_job.result()  # Waits for query to finish

    return df_result


# http://localhost:5000/test
app.run(host='0.0.0.0', port=5000)