import pandas as pd
import numpy as np
import requests
import json
import configparser
import psycopg2
import time
from datetime import datetime
import argparse
from apscheduler.schedulers.background import BackgroundScheduler
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

import warnings

warnings.simplefilter("ignore")
pd.set_option('mode.chained_assignment', None)

parser = argparse.ArgumentParser()
args = parser.parse_args("")

conf = configparser.ConfigParser()
conf.read('info.init')

dbname = conf.get('DB', 'dbname')
host = conf.get('DB', 'host')
user = conf.get('DB', 'user')
password = conf.get('DB', 'password')
port = conf.get('DB', 'port')
table = json.loads(conf.get('DB','table')) #train table
print("<DB Info>")
print("dbname:", dbname + "\nhost:", host + "\nport:", port)


# =======================================CONFIGURATION=======================================*
# args.host = conf.get('server','host')
#args.host = conf.get('server','local')
args.host = conf.get('server','container_host')
args.port = conf.get('server','port')
args.url = 'http://' + args.host + ':' + args.port + '/predict'
# args.url = 'http://125.131.88.57' + ':' + args.port + '/predict'

args.minutes = conf.getint('settings','minutes')
args.interval = conf.getint('settings','interval')
args.count = conf.getint('settings','count')
args.error = conf.getfloat('settings','error') #이상감지 오차 기준
args.timezone = json.loads(conf.get('settings', 'timezone')) #이상감지 시간대
args.features = json.loads(conf.get('plant','feature'))

select = 'plant4'
args.sid = conf.get('sid', select)
args.rtu_id_inv = json.loads(conf.get('plant', select))
args.rtu_id_env = conf.get('env', select)

print("<Plant Info>")
print("sid:", args.sid)
print("rtu_id_inv:{}".format(args.rtu_id_inv))
args.date = '2021-03-11'  # DB 실제 수집 데이터 날짜
# ============================================================================================*

con = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
cursor = con.cursor()

def collect(rtu):
    inv_cond = "rtu_id = '{}' ORDER BY ud DESC LIMIT {}".format(rtu, args.count)
    inv_db = "SELECT * FROM {} WHERE {}".format(table[0], inv_cond)
    cursor.execute(inv_db)
    inv = pd.DataFrame(cursor.fetchall())
    inv.columns = [desc[0] for desc in cursor.description]

    env_cond = "rtu_id = '{}' ORDER BY ud DESC LIMIT {}".format(args.rtu_id_env, args.count)
    env_db = "SELECT * FROM {} WHERE {}".format(table[1], env_cond)
    cursor.execute(env_db)
    env = pd.DataFrame(cursor.fetchall())
    env.columns = [desc[0] for desc in cursor.description]

    inv = inv[['ud', 'rtu_id', 'sid', 'dc_p', 'dc_i', 'dc_v']]
    env = env[['ud', 's_ir', 'h_ir', 'm_t', 'a_t']]
    inv['ud'] = pd.to_datetime(inv['ud'], format='%Y-%m-%d %H:%M:%S')
    env['ud'] = pd.to_datetime(env['ud'], format='%Y-%m-%d %H:%M:%S')
    inv['ud'] = inv['ud'].apply(lambda t: t.replace(second=0, microsecond=0))
    env['ud'] = env['ud'].apply(lambda t: t.replace(second=0, microsecond=0))
    plant = pd.merge(inv.set_index('ud'), env.set_index('ud'), left_index=True, right_index=True).reset_index()
    plant = plant.drop_duplicates(['ud', 'rtu_id'], keep='first').reset_index(drop=True)
    print('plant: {}'.format(args.sid))
    print('rtu_id: {}'.format(rtu))
    print('request date: {}'.format(datetime.now()))
    print('DB recently date: {}'.format(inv['ud'][0]))
    print(plant)
    return plant


def serving(rtu):
    plant = collect(rtu)

    if plant.empty:
        print("plant is empty!!")
    else:
        plant_feature = plant[args.features]
        plant_json = plant_feature.to_json()
        print("request data \n", plant_json)
        req = requests.post(args.url, plant_json, verify=False)

        pred_json = req.json()
        print(req.status_code, "Receive Prediction Result")
        print(pred_json)

        pred_df = pd.DataFrame(pred_json, index=['predict']).T
        pred_df['predict'] = pred_df['predict'].astype(float)
        pred_df['predict'] = pred_df['predict'].round(2)
        for i in range(len(pred_df)):
            if pred_df['predict'][i] < 0:
                pred_df['predict'][i] = 0.0

        plant_predict = pd.merge(plant.reset_index(drop=True), pred_df.reset_index(drop=True), left_index=True, right_index=True)
        print(plant_predict)
        plant_predict['dc_diff'] = round(plant_predict['predict'] - plant_predict['dc_p'],2)

        plant_predict['flag'] = 0
        for i in range(len(plant_predict)):
            if plant_predict['ud'].dt.hour[i] in args.timezone:
                if ((plant_predict['dc_diff'][i] > 0) & (plant_predict['dc_diff'][i] > plant_predict['predict'][i] * args.error)):
                    plant_predict['flag'][i] = 1 # anomaly
                elif plant_predict['dc_diff'][i] < 0:
                    plant_predict['flag'][i] = 2 # dc_p > dc_pred
                else:
                    plant_predict['flag'][i] = 0 # normal
            else:
                plant_predict['flag'][i] = 3 # not include args.timezone

        plant_predict['flag'] = plant_predict['flag'].astype(float)

        plant_predict['status'] = 0
        plant_predict['status'] = plant_predict['status'].astype(float)
        # plant_predict['diff_sum'] = plant_predict['dc_diff'].copy()

        cnt = 0
        for i in range(len(plant_predict)):
            if plant_predict.flag[i] == 1:
                cnt = cnt + 1
        if cnt == args.count: # args.minutes=5 * args.count=30
            plant_predict['status'][0] = 1

        insert_serving = "INSERT INTO ads(ud, rtu_id, sid, dc_p, dc_i, dc_v, s_ir, h_ir, m_t, a_t, dc_pred, dc_diff, flag, status)"\
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
                        "ON CONFLICT ON CONSTRAINT ads_key DO UPDATE SET " \
                        "ud=EXCLUDED.ud, rtu_id = EXCLUDED.rtu_id, sid = EXCLUDED.sid, dc_p = EXCLUDED.dc_p, dc_i = EXCLUDED.dc_i, " \
                        "dc_v = EXCLUDED.dc_v, s_ir = EXCLUDED.s_ir, h_ir = EXCLUDED.h_ir, m_t = EXCLUDED.m_t, a_t = EXCLUDED.a_t," \
                        "dc_pred = EXCLUDED.dc_pred, dc_diff = EXCLUDED.dc_diff, flag = EXCLUDED.flag, status = EXCLUDED.status"

        predict_data = []
        for i in range(len(plant_predict)):
            predict_data.append(plant_predict.loc[i].tolist())

        cursor.executemany(insert_serving, predict_data)
        con.commit()
        print("Insert DB Success")


scheduler = BackgroundScheduler()
# @schedule.scheduled_job('interval', minutes=args.interval, id='test')


def repeat():
    for i in args.rtu_id_inv:
        serving(i)

runner_job = scheduler.add_job(repeat, 'interval', minutes=args.interval)

def start_runner():
    print("Start Runner: {}".format(datetime.now()))
    print("Anomaly Detection Timezone: ", args.timezone)
    runner_job.resume()

def stop_runner():
    print("Stop Runner: {}".format(datetime.now()))
    print("Anomaly Detection Timezone: ", args.timezone)
    runner_job.pause()

stop_runner()

scheduler.add_job(start_runner, 'cron', hour='9-19')
scheduler.add_job(stop_runner, 'cron', hour='0-8, 20-23')

scheduler.start()

while True:
    time.sleep(1)
