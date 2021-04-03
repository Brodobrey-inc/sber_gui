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
        X.loc[: ,'mem_usage_bytes'] = X.loc[: ,'mem_usage_bytes'] / X.loc[: ,'mem_limit_bytes']
        X.loc[: ,'cpuacct.usage'] = X.loc[: ,'cpuacct.usage'] / X.loc[: ,'cpu.cfs_quota_us']
        X.loc[: ,'req_general_currentThreadsBusy'] = X.loc[: ,'req_general_currentThreadsBusy'] / X.loc[: ,'req_general_currentThreadCount']
        return X



class DropBadAttributes(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.attributes_to_drop = {}
        self.list_to_drop = ["sbp.sbt.dataspacecore.lib.packet.processor.PacketExecutorImpl_count:1;commands:c.Contract", "sbp.sbt.dataspacecore.lib.packet.processor.PacketExecutorImpl_count:1;commands:c.Product", "sbp.sbt.dataspacecore.lib.packet.processor.PacketExecutorImpl_count:1;commands:g.Contract", "sbp.sbt.dataspacecore.lib.commandhandlers.CreateCommandHandler_count:1;commands:c.Contract", "sbp.sbt.dataspacecore.lib.commandhandlers.CreateCommandHandler_count:1;commands:c.Product", "sbp.sbt.dataspacecore.lib.commandhandlers.CreateCommandHandler_count:1;commands:g.Contract", "sbp.com.sbt.dataspace.locks.TransactionLockProviderImpl_count:1;commands:c.Contract", "sbp.com.sbt.dataspace.locks.TransactionLockProviderImpl_count:1;commands:c.Product", "sbp.com.sbt.dataspace.locks.TransactionLockProviderImpl_count:1;commands:g.Contract", "sbp.sbt.dataspacecore.lib.commandhandlers.GetCommandHandler_count:1;commands:c.Contract", "sbp.sbt.dataspacecore.lib.commandhandlers.GetCommandHandler_count:1;commands:c.Product", "sbp.sbt.dataspacecore.lib.commandhandlers.GetCommandHandler_count:1;commands:g.Contract", "HikariProxyPreparedStatement.executeQuery", "HikariProxyPreparedStatement.close", "HikariProxyPreparedStatement.executeUpdate", "ProxyDataSource.getConnection", "HikariProxyConnection.close", "HikariProxyConnection.commit", "MetadataGCThreshold_count", "MetadataGCThreshold_mean", "MetadataGCThreshold_sum", "MetadataGCThreshold_upper", "sbp.com.sbt.dataspace.replication.journal.SyncJournalSenderImpl_emtpy", "sbp.sbt.dataspacecore.lib.commandhandlers.UpdateCommandHandler_emtpy", "sbp.sbt.dataspacecore.lib.commandhandlers.TryLockCommandHandler_emtpy", "sbp.sbt.dataspacecore.lib.commandhandlers.UnlockCommandHandler_emtpy", "WITH"]

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        new_X = X.copy()
        for i in range(1, 51):
            col = "thhisto_general_general-pool-thread-" + str(i)
            if col in new_X.columns:
                new_X.drop(col, axis = 1, inplace = True)
        for i in self.list_to_drop:
            if i in new_X.columns:
                new_X.drop(i, axis = 1, inplace = True)
        new_X.drop(['req_general_bytesSent',
                    'req_general_bytesSent',
                    'mem_limit_bytes',
                    'rssanon_kb',
                    'codeheapprofnmet_Max',
                    'tomcatpool_general_taskCount',
                    'tomcatpool_general_corePoolSize',
                    'tomcatpool_general_completedTaskCount',
                    'tomcatpool_general_activeCount',
                    'tomcatpool_general_taskCount',
                    'tomcatpool_general_queueSize',
                    'tomcatpool_general_rjTskCnt',
                    'completedTaskCount',
                    'G1EvacuationPause_count',
                    'GCLockerInitiatedGC_count',
                    'GCLockerInitiatedGC_mean',
                    'value',
                    'target',
                    'success'
                   ], axis=1, inplace=True)
        return new_X


class DropSameAttributes(BaseEstimator, TransformerMixin):
    def __init__(self, lowest_coef_to_drop=0.995):
        self.attributes_to_drop = {}
        self.coef = lowest_coef_to_drop
        self.testattr = {}

    def show_attributes(self):
        return self.testattr

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        corr_matrix = X.corr()
        for col in corr_matrix.columns:
            for row in corr_matrix.index:
                if col != row and corr_matrix.loc[col, row] > self.coef and len(col) < len(row):
                    self.attributes_to_drop[col] = row
                    self.testattr[col] = (row, corr_matrix.loc[col, row])
        return X.drop(self.attributes_to_drop.values(), axis=1)


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

    df0203_1118 = load_dataset(3, 3, 8, 17).reset_index(drop=True)

    for i in range(100):
        sen = df0203_1118.iloc[0:1, :]
        payload = {"data": sen.to_json(orient="index")}
        headers = {"Content-Type": "application/json"}
        df0203_1118 = df0203_1118.drop(df0203_1118.index[0])

        response = requests.request("PUT", url, json=payload, headers=headers)
        print(response)
        time.sleep(0.2)