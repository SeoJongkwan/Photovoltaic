import pandas as pd
import psycopg2
from io import StringIO

conn = psycopg2.connect(host='34.64.92.171', dbname='solar_plant', user='solar_plant', password='ionsolarplantdev', port=5432)
cursor = conn.cursor()

pv = pd.read_csv('0004_0026/0004_0026_P.csv')
we = pd.read_csv('0004_0026/0004_0026_W.csv')

rtu = pv.rename(columns={'dateInfo':'ud','inverterId':'rtu_id','pvPower':'dc_p','pvCurrent':'dc_i','pvVoltage':'dc_v'})
rtu = rtu[['ud', 'rtu_id', 'dc_p', 'dc_i', 'dc_v']]
rtu['ud'] = pd.to_datetime(rtu['ud'], format='%Y-%m-%d %H:%M:%S')
rtu['sid'] = '0004_0026'

weather = we.rename(columns={'dateInfo':'ud','irrModule':'s_ir','irrHorizontal':'h_ir', 'tempModule':'m_t'})
weather = weather[['ud', 'm_t', 's_ir', 'h_ir']]
weather['ud'] = pd.to_datetime(weather['ud'], format='%Y-%m-%d %H:%M:%S')
weather['s_ir'] = weather['s_ir'].astype(float)
weather['h_ir'] = weather['h_ir'].astype(float)

plant = pd.merge(rtu.set_index('ud'), weather.set_index('ud'), left_index=True, right_index=True).reset_index()
plant['a_t'] = 0.00
plant['rtu_id'] = plant['rtu_id'].astype(int)
plant = plant[['ud','rtu_id','sid','dc_p','dc_i','dc_v','s_ir','h_ir','m_t','a_t']]
plant.to_csv("plant.csv")

out = StringIO()
plant.to_csv(out, sep='\t', index=False, header=False)
out1 = out.getvalue()
cursor.copy_from(StringIO(out1), "train")
conn.commit()


# insert_db = "INSERT INTO train(ud, rtu_id, dc_p, dc_i, dc_v, sid, s_ir, h_ir, m_t,a_t) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#
# data = []
# for i in range(len(plant)):
#     data.append(plant.loc[i].tolist())
#
# cursor.executemany(insert_db, data)
# conn.commit()