from sklearn import datasets
import numpy as np

def load():
    # load data from sklearn
    X, y = datasets.load_digits(return_X_y=True)
    row_n = X.shape[0]
    column_n = X.shape[1] + 1

    all_data = np.random.rand(row_n, column_n)

    all_data[:, :-1] = X
    all_data[:, -1] = y

    # convert the data into correct request.put json form
    data_list = []
    for row in all_data:
        dict_1 = {f"pix_{i}": row[i] for i in range(row.shape[0] - 1)}
        dict_1["y"] = row[-1]

        data_list.append(dict_1)

    return data_list
