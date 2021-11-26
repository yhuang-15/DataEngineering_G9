from flask import Flask, jsonify, Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import random
import io
import pandas as pd
#from google.cloud import bigquery

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/test_plot')
def plotting():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_figure():
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)

    df = pd.DataFrame()
    xs = range(100)
    ys = [random.randint(1, 50) for x in xs]
    axis.plot(xs, ys)
    return fig
    

# results for batch pipeline
#@app.route('/batch/<table_name>', methods=['GET'])
#def print_batch_results(table_name):
#    test_dic = load_table_from_BQ(table_name).to_dict()
#    return jsonify(test_dic, 200)
#
## results for stream pipeline
#@app.route('/stream/<table_name>', methods=['GET'])
#def print_batch_results(table_name):
#    test_dic = load_table_from_BQ(table_name).to_dict()
#    return jsonify(test_dic, 200)


#def load_table_from_BQ(table_name):
#    client = bigquery.Client(project="jads-de-2021")  
#
#    # Perform a query.
#    QUERY = (
#        f'SELECT * FROM `jads-de-2021.assignment_2.{table_name}` LIMIT 100')   # use the correct project id, etc.
#    query_job = client.query(QUERY)  # API request
#
#    #convert the results to pandas dataframe and then convert to dictionary
#    df_result = query_job.to_dataframe()
#
#    return df_result
#

app.run(host='0.0.0.0', port=5000)