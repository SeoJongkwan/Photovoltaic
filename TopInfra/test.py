import pandas as pd
import time
import datetime
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import argparse
import configparser
import json

conf = configparser.ConfigParser()
conf.read('info.init')

parser = argparse.ArgumentParser()
args = parser.parse_args("")

interval = 5
rtu_id_inv = [1, 2, 3, 21]
timezone = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]


def serving(data):
    print(time.strftime("%H:%M:%S") + "  rtu_id: ", data)


def repeat():
    for i in rtu_id_inv:
        serving(i)


scheduler = BackgroundScheduler()

# @schedule.scheduled_job('interval', seconds=interval, id='test')
runner_job = scheduler.add_job(repeat, 'interval', seconds=interval)


def start_runner():
    print("Start Runner: {}".format(datetime.now()))
    print("Anomaly Detection Timezone: ", args.timezone)
    runner_job.resume()


def stop_runner():
    print("Stop Runner: {}".format(datetime.now()))
    print("Anomaly Detection Timezone: ", args.timezone)
    runner_job.pause()


stop_runner()

if datetime.now().hour in args.timezone:
    hour = datetime.now().hour
    min = datetime.now().minute + 1
    if min == 60:
        min = 00
    print("Scheduler Start Time(HH:MM) - {}:{}".format(hour, min))
    scheduler.add_job(start_runner, 'cron', hour=hour, minute=min)

# scheduler.add_job(start_runner, 'cron', hour=args.timezone[0])
scheduler.add_job(stop_runner, 'cron', hour=args.timezone[-1])

scheduler.start()

while True:
    time.sleep(1)
