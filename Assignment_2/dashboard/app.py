from flask import Flask, jsonify, Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import random
import io
import pandas as pd
from google.cloud import bigquery

app = Flask(__name__)
app.config["DEBUG"] = True

# results for batch pipeline
@app.route('/batch/<table_name>', methods=['GET'])
def print_batch_results(table_name):
    test_dic = load_table_from_BQ(table_name).to_dict()
    return jsonify(test_dic, 200)

# results for stream pipeline
@app.route('/stream/<table_name>', methods=['GET'])
def print_stream_results(table_name):
    test_dic = load_table_from_BQ(table_name).to_dict()
    return jsonify(test_dic, 200)

def load_table_from_BQ(table_name):
    client = bigquery.Client(project="jads-de-2021")  

    # Perform a query.
    QUERY = (
        f'SELECT * FROM `jads-de-2021.assignment_2.{table_name}` LIMIT 100')   # use the correct project id, etc.
    query_job = client.query(QUERY)  # API request

    #convert the results to pandas dataframe and then convert to dictionary
    df_result = query_job.to_dataframe()

    return df_result


@app.route('/plot_streaming')
def plotting():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_figure():
    fig = Figure(figsize = (20, 12))
    axis_1 = fig.add_subplot(1, 2, 1)
    axis_2 = fig.add_subplot(1, 2, 2)

    #df = pd.read_csv('D:/2021-2023_MDSE/1.1/Data Engineering/Assignments/data/Credit_card_transactions/stream_table.csv')
    df = load_table_from_BQ('streaming_table')
    
    axis_1 = plot_gender(df, 'M', axis_1)
    axis_2 = plot_gender(df, 'F', axis_2)

    return fig

def plot_gender(df, gender, axis):
    df_M = df[df['gender'] == gender]
    df_M['start_time'] = pd.to_datetime(df_M['start_time']).astype('datetime64[s]')
    # sort by start time stamp in case of delayed records
    df_M.sort_values(by='start_time')
    xs = range(df_M['start_time'].unique().shape[0])
    ys = df_M['amt']

    axis.plot(xs, ys)
    axis.set_ylim([ys.min()-10, ys.max()+10])
    axis.set_xticks([i for i in range(df_M['start_time'].unique().shape[0])])
    axis.set_xticklabels([str(time) for time in df_M['start_time']], rotation=45, ha='right')
    axis.title.set_text(f'Gender: {gender}')

    return axis


app.run(host='0.0.0.0', port=5000)