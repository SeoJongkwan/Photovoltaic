import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import json
import argparse
import configparser
import os
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import h2o
from h2o.automl import H2OAutoML

import warnings
warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)
plt.style.use('dark_background')

parser = argparse.ArgumentParser()
args = parser.parse_args("")

conf = configparser.ConfigParser()
conf.read('info.init')

select ='plant4'
args.sid = conf.get('sid', select)
args.rtu_id_inv = json.loads(conf.get('plant', select))
args.feature = json.loads(conf.get('plant', 'feature'))
print("<Plant Info>")
print("sid:", args.sid)
print("rtu_id_inv:{}".format(args.rtu_id_inv))

data_path = "data/"
model_path = "model/"

os.makedirs(data_path + args.sid, exist_ok=True)
for path in args.rtu_id_inv:
    os.makedirs(data_path + args.sid + '/' + path, exist_ok=True)
    os.makedirs(model_path + args.sid + '/' + path, exist_ok=True)

h2o.init()

def check_nan_value(df):
    print('Check NAN Value on Each Column:\n{}'.format(df.isnull().sum()))
    s = df.isnull().sum()
    for value in s.values:
        if value != 0:
            df1 = df[df.isnull().any(1)]
            print("NAN Value Location: {}\n{}".format(len(df1), df['ud'][df1.index]))
            df1 = df.drop(df1.index).reset_index(drop=True)
            return df1
    return df

def feature_engineering(data):
    plant_feature = data[args.feature[2:]]
    plant = check_nan_value(plant_feature)
    return plant

def create_model(df, rtu_name):
    train = df[0:int(len(df) * .7)]
    test = df[int(len(df) * .7)+1:]

    scaler = MinMaxScaler()
    train_scaled = scaler.fit_transform(train)
    test_scaled = scaler.fit_transform(test)

    train_df = pd.DataFrame(train_scaled, columns=args.feature[2:])
    test_df = pd.DataFrame(test_scaled, columns=args.feature[2:])

    htrain = h2o.H2OFrame(train_df)
    htest = h2o.H2OFrame(test_df)

    x = htrain.columns
    y = 'dc_p'

    aml = H2OAutoML(max_runtime_secs = 600) #nfolds=9
    # aml.train(x=x, y=y, training_frame=htrain, validation_frame=hval, leaderboard_frame = htest)
    aml.train(x=x, y=y, training_frame=htrain)

    lb = aml.leaderboard
    print("LeaderBoard:", lb.head())
    # print("Parameter Keys:", aml.leader.params.keys())

    pred = aml.leader.predict(htest)
    pred_df = pred.as_data_frame()
    htest_df = htest.as_data_frame()

    def inverse_value(value):
        _value = np.zeros(shape=(len(value), len(args.feature[2:])))
        _value[:,0] = value[:,0]
        inv_value = scaler.inverse_transform(_value)[:,0]
        return inv_value

    inv_pred = inverse_value(pred_df['predict'].values.reshape(-1, 1))
    inv_test = inverse_value(htest_df['dc_p'].values.reshape(-1, 1))

    def percentage_error(actual, predicted):
        res = np.empty(actual.shape)
        for j in range(actual.shape[0]):
            if actual[j] != 0:
                res[j] = (actual[j] - predicted[j]) / actual[j]
            else:
                res[j] = predicted[j] / np.mean(actual)
        return res


    def evaluate_model(actual, pred):
        with open(model_path + 'model_accuracy.json', 'r') as j:
            accuracy = json.load(j)
        mape = "%.3f"%np.mean(np.abs(percentage_error(np.asarray(actual), np.asarray(pred))))
        mae = "%.3f"% mean_absolute_error(actual, pred)
        rmse = "%.3f"%np.sqrt(mean_squared_error(actual, pred))
        rsquared = '%.3f'%r2_score(actual, pred)

        for i in range(len(accuracy[args.sid])):
            if list(accuracy[args.sid][i].keys())[0] == rtu_name:
                accuracy[args.sid][i][args.rtu_id_inv[i]][0]["MAPE"] = mape
                accuracy[args.sid][i][args.rtu_id_inv[i]][0]["MAE"] = mae
                accuracy[args.sid][i][args.rtu_id_inv[i]][0]["RMSE"] = rmse
                accuracy[args.sid][i][args.rtu_id_inv[i]][0]["RSQUARED"] = rsquared
        with open(model_path + 'model_accuracy.json', 'w') as w:
            json.dump(accuracy, w, indent=4)

        plt.figure(figsize=(20, 6))
        plt.plot(actual, 'limegreen', label='actual')
        plt.plot(pred, 'yellow', label='predict')
        plt.title('Predict Result: {}'.format(rtu_name)); plt.xlabel('time'); plt.ylabel('dc_p');plt.legend()
        plt.savefig(model_path + args.sid + '/' + rtu_name + '/{}_chart'.format(rtu_name, dpi=300))
        plt.tight_layout()
        plt.show()

    evaluate_model(inv_test, inv_pred)
    aml.leader.model_performance(htest)

    return aml


for r in args.rtu_id_inv:
    print(r)
    load_data = pd.read_csv(data_path + args.sid + '/' + r + '/{}_this.csv'.format(r))
    load_data['ud'] = pd.to_datetime(load_data['ud'], format='%Y-%m-%d %H:%M:%S')
    load_data_hour = load_data[(load_data['ud'].dt.hour > 8) & (load_data['ud'].dt.hour < 19)].reset_index(drop=True)
    preprocessing = feature_engineering(load_data_hour.set_index('ud'))
    aml = create_model(preprocessing, r)
    model_ids = list(aml.leaderboard['model_id'].as_data_frame().iloc[:, 0])
    # print("Model IDs:", model_ids)

    best_model = h2o.download_model(aml.leader, model_path + args.sid + '/' + r)
    print("Best Model: \n", model_ids[0])

    h2o.remove_all()


