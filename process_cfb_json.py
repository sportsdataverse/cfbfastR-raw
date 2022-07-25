import os, json
import re
import http
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sportsdataverse as sdv
import xgboost as xgb
import time
import urllib.request
import warnings
from tqdm import tqdm
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
warnings.filterwarnings("ignore")
path_to_raw = "pbp_json_raw"
path_to_final = "pbp_json_final"

def main():
    max_year = 2021
    min_year = 2019
    schedule = pd.read_parquet('cfb_schedule_master.parquet', engine='auto', columns=None)
    schedule = schedule.sort_values(by=['season','season_type'], ascending = True)
    schedule["game_id"] = schedule["game_id"].astype(str)

    schedule = schedule[schedule['status_type_completed']==True]
    schedule_with_pbp = schedule[schedule['season']>=2004]

    for year in range(min_year, max_year+1):
        print("Processing year {}...".format(year))
        schedule_pbp = schedule_with_pbp[schedule_with_pbp['season']==year]['game_id'].tolist()
        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]
        json_files = set(json_files).intersection(set(schedule_pbp))
        json_files = list(json_files)
        json_files = sorted(json_files)
        print("Number of Games: {}".format(len(json_files)))
        # we need both the json and an index number so use enumerate()
        for index, js in enumerate(json_files):
            try:
                processed_data = sdv.cfb.CFBPlayProcess(gameId = js, path_to_json = path_to_raw_json)
                # processed_data = sdv.cfb.CFBPlayProcess( gameId = 242620142, path_to_json = 'pbp_json_raw/')
                pbp = processed_data.cfb_pbp_disk()
                result = processed_data.run_processing_pipeline()
                if result.get("playByPlaySource", "none") != "none":
                    result['box_score'] = processed_data.create_box_score()
                fp = "{}{}.json".format(path_to_final_json, js)
                with open(fp,'w') as f:
                    json.dump(result, f, indent=2, sort_keys=False)
            except (IndexError) as e:
                print("IndexError: game_id = {}\n {}".format(js, e))
            except (KeyError) as e:
                print("KeyError: game_id = {}\n {}".format(js, e))
                continue
            except (ValueError) as e:
                print("DecodeError: game_id = {}\n {}".format(js, e))
                continue
            except (AttributeError) as e:
                print("AttributeError: game_id = {}\n {}".format(js, e))
                continue
        print("Finished processing year {}...".format(year))

if __name__ == "__main__":
    main()