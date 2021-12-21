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

sid = '0004_0004/'
rtu_id = '0004_0004_I_0001/'
df = pd.read_csv('data/' + sid + rtu_id + '0004_0004_I_0001_total.csv')

col = 'dc_p'
df[col].unique()
df[col].value_counts()
df[col].max()