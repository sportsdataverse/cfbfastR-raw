import os, json
import re
import http
import json
import time
import urllib.request
import pyreadr
import pyarrow as pa
import pandas as pd
import sportsdataverse as sdv
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
path_to_schedules = "cfb/schedules"
final_file_name = "cfb_schedule_master.csv"

def download_schedule(season, path_to_schedules=None):
    df = sdv.cfb.espn_cfb_calendar(season)
    df = df[df['season_type'].isin(['2','3'])]

    ev = pd.DataFrame()
    for index, row in df.iterrows():
        date_schedule = sdv.cfb.espn_cfb_schedule(dates=season,
                                                  season_type=row['season_type'],
                                                  week=row['week'])
        ev = pd.concat([ev,date_schedule],axis=0, ignore_index=True)
    ev = ev.drop('competitors', axis=1)
    ev = ev.drop_duplicates(subset=['game_id'], ignore_index=True)
    if path_to_schedules is not None:
        path_to_csv = "{}/{}/".format(path_to_schedules, 'csv')
        path_to_parquet = "{}/{}/".format(path_to_schedules, 'parquet')
        path_to_rds = "{}/{}/".format(path_to_schedules, 'rds')
        path_to_json = "{}/{}/".format(path_to_schedules, 'json')
        Path(path_to_csv).mkdir(parents=True, exist_ok=True)
        Path(path_to_parquet).mkdir(parents=True, exist_ok=True)
        Path(path_to_rds).mkdir(parents=True, exist_ok=True)
        Path(path_to_json).mkdir(parents=True, exist_ok=True)
        ev.to_csv(f"{path_to_schedules}/csv/cfb_schedule_{season}.csv", index = False)
        ev.to_parquet(f"{path_to_schedules}/parquet/cfb_schedule_{season}.parquet", index = False)
        pyreadr.write_rds(f"{path_to_schedules}/rds/cfb_schedule_{season}.rds", ev)
        fp = "{}/cfb_schedule_{}.json".format(path_to_json, season)
        with open(fp,'w') as f:
            json.dump(ev.to_json(orient='records'), f, indent=0, sort_keys=False)
    return ev
def main():

    years_arr = range(2002,2023)
    schedule_table = pd.DataFrame()
    for year in years_arr:
        year_schedule = download_schedule(year, path_to_schedules)
        schedule_table = pd.concat([schedule_table, year_schedule], axis=0)
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_schedules+'/csv') if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_schedules}/csv/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data.to_csv(final_file_name, index=False)
    glued_data.to_parquet(final_file_name.replace('.csv', '.parquet'), index=False)

if __name__ == "__main__":
    main()
