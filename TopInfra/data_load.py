import pandas as pd
import psycopg2
import argparse
import configparser
import json
import os

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

select = 'plant4'
args.sid = conf.get('sid', select)
args.rtu_id_inv = json.loads(conf.get('plant', select))
print("<Plant Info>")
print("sid:", args.sid)
print("rtu_id_inv:{}".format(args.rtu_id_inv))

data_path = "data/"
model_path = "model/{}/{}".format(args.sid, args.rtu_id_inv)

os.makedirs(data_path + args.sid, exist_ok=True)
for path in args.rtu_id_inv:
    os.makedirs(data_path + args.sid + '/' + path, exist_ok=True)

con = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
cursor = con.cursor()

def collect(rtu):
    inv_cond = "sid = '{}' ORDER BY ud DESC".format(rtu)
    inv_db = "SELECT * FROM {} WHERE {}".format(table[0], inv_cond)
    cursor.execute(inv_db)
    inv = pd.DataFrame(cursor.fetchall())
    inv.columns = [desc[0] for desc in cursor.description]

    env_cond = "sid = '{}' ORDER BY ud DESC".format(rtu)
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
    print('date: {}'.format(inv['ud'][0]))
    print(plant)
    return plant

plant = collect(args.sid)

for r in args.rtu_id_inv:
    globals()['{}'.format(r)] = plant[plant['rtu_id'] == r].reset_index(drop=True)
    # globals()['{}'.format(r)].to_csv(data_path + args.sid + '/' + r +'/{}_this.csv'.format(r))
    globals()['{}'.format(r)].to_csv(data_path + args.sid + '/' + r +'/{}.csv'.format(r))
    print("File Saved: {}".format(r))

print("Data Load is Completed")


