import pandas as pd
import numpy as np
import requests
import json
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
from urllib.parse import urlencode, unquote

host = "34.64.92.171"
dbname = "solar_plant"
user = "solar_plant"
password = "ionsolarplantdev"
port = "5432"

con = psycopg2.connect(host=host, dbname=dbname, user=user, password=password, port=port)
cursor = con.cursor()


url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
cert_key = "oEXb0KBtqI8V3TJAj1lmb9ZgDq8pwKDDnk2dAlaRpRltMNYuoTCT%2B1hlmImqXNWjK2qquaN9S7v2irGCoRccxw%3D%3D"

params = "?" + urlencode({
    "serviceKey": cert_key,
    "numOfRows": "1000",
    "dataType": "JSON",
    "base_date": datetime.now().strftime("%Y%m%d"),
    "base_time": "0200",                        #0200, 0500, 0800, 1100, 1400, 1700, 2000, 2300 (1일 8회)
    "nx": "55",
    "ny": "68"
})

res = requests.get(url+unquote(params))
res_json = json.loads(res.text)
data = pd.DataFrame(res_json['response']['body']['items']['item'])

data['date'] = data[["fcstDate","fcstTime"]].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
data['base_date'] = data[["baseDate","baseTime"]].apply(lambda row: ''.join(row.values.astype(str)), axis=1)
data['date'] = pd.to_datetime(data['date'], format='%Y%m%d%H%M')
data['base_date'] = pd.to_datetime(data['base_date'], format='%Y%m%d%H%M')

weather = data[['base_date', 'date', 'category', 'fcstValue', 'nx', 'ny',]]

# ALTER TABLE asos ADD CONSTRAINT asos_key UNIQUE (date, category);
def insert_db(data):
    insert_table = "INSERT INTO asos(base_date, date, category, fcst_value, nx, ny) " \
                "VALUES (%s,%s,%s,%s,%s,%s)" \
                "ON CONFLICT ON CONSTRAINT asos_key DO UPDATE SET " \
                "base_date=EXCLUDED.base_date, date=EXCLUDED.date, category=EXCLUDED.category, " \
                "fcst_value=EXCLUDED.fcst_value, nx=EXCLUDED.nx, ny=EXCLUDED.ny"

    wdata = []
    for i in range(len(data)):
        wdata.append(data.loc[i].tolist())

    cursor.executemany(insert_table, wdata)
    con.commit()
insert_db(weather)
print("Time DB Insert: {} ~ {}".format(weather['date'].iloc[0], weather['date'].iloc[-1]))




#지상 시간자료 조회서비스
# day_url = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
# day_cert_key = 'oEXb0KBtqI8V3TJAj1lmb9ZgDq8pwKDDnk2dAlaRpRltMNYuoTCT%2B1hlmImqXNWjK2qquaN9S7v2irGCoRccxw%3D%3D'
#
# params = "?" + urlencode({
#     'serviceKey': day_cert_key,
#     'pageNo': '1',
#     'numOfRows': '10',
#     'dataType': 'JSON',
#     'dataCd': 'ASOS',
#     'dateCd': 'HR',
#     'startDt': datetime.now().strftime("%Y%m%d"),
#     'startHh': '01',
#     'endDt': (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
#     'endHh': '00',
#     'stnIds': '156'
# })

# res = requests.get(day_url+unquote(params))
# res_json = json.loads(res.text)
# data = pd.DataFrame(res_json['response']['body']['items']['item'])
