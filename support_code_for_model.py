import os
import time
import requests
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from joblib import load, dump


DATASET_PATH = os.path.join("datasets")


def load_dataset(day: int, month: str, hour: str, minute: str, paths_to_check: list = ["3rd", "4th"], search_for_special_string: str = "", index_col_name: str = "Unnamed: 0"):
    for folder in paths_to_check:
        path = os.path.join(DATASET_PATH, folder)
        for elem in os.listdir(path):
            if "{:02d}-{:02d} {:02d}_{:02d}".format(day, month, hour, minute) in elem and search_for_special_string in elem:
                path = os.path.join(path, elem)
                dataset = pd.read_csv(path, parse_dates=[0])
                dataset = dataset.set_index(index_col_name, drop=True).resample("S").mean()
                return dataset


class CombinedAttributesAdder(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        X.fillna(value=0, inplace=True)
        cols = ['cpuacct.usage',
                'throttledPercent', 'mem_activity_in_mb', 'mem_activity_out_mb',
                'mem_usage_bytes', 'cache_bytes', 'taskCount',
                'queueSize', 'activeCount', 'batch', 'UPDATE', 'INSERT', 'SELECT', 'latency']
        X_copy = X.copy()
        X_copy = X_copy[cols]
        return X_copy


if __name__ == "__main__":
    '''ml_model = load("first_model.joblib")
    df0203_1118 = load_dataset(3, 3, 8, 17).reset_index(drop=True)
    df0203_1118["forecast"] = df0203_1118["latency"].shift(-30)
    df0203_1118.dropna(inplace=True)
    X_train = df0203_1118.drop("forecast", axis=1)
    y_train = df0203_1118["forecast"]
    res = ml_model.predict(X_train) - y_train
    print(res.mean())
    print(ml_model.predict(X_train.iloc[1:2, :]))'''

    url = "http://127.0.0.1:5000/sb/"

    df0203_1118 = load_dataset(4, 4, 4, 4).reset_index(drop=True)

    while df0203_1118.shape[0] > 1:
        sen = df0203_1118.iloc[0:1, :]
        payload = {"data": sen.to_json()}
        headers = {"Content-Type": "application/json"}
        df0203_1118 = df0203_1118.drop(df0203_1118.index[0])

        response = requests.request("PUT", url, json=payload, headers=headers)
        print(response)
        time.sleep(0.007)