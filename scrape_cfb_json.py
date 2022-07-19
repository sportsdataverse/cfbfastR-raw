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
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from pathlib import Path
from play_handler import PlayProcess, process_cfb_raw_for_gop
path_to_raw = "pbp_json_raw"
path_to_final = "pbp_json_final"
def main():
    years_arr = range(2002,2021)
    schedule = pd.read_parquet('cfb_schedule_master.parquet', engine='auto', columns=None)
    schedule["game_id"] = schedule["game_id"].astype(int)
    print(schedule.columns)
    schedule = schedule[schedule['status_type_completed']==True]
    print(schedule.columns)
    schedule = schedule.sort_values(by=['season','season_type'], ascending = False)
    for year in years_arr:
        print(year)
        games = schedule[(schedule['season']==year)].reset_index()['game_id']
        print(f"Number of Games: {len(games)}, first gameId: {games}")

        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        # json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]
        i = 0
        for game in games[i:]:
            if i == len(games):
                print("done with year")
                continue
            # if (i % 100 == 0 ):
            print("Working on game {}/{}, gameId: {}".format(i+1, len(games[i:]) + i, game))
            if len(str(game))<9:
                i+=1
                continue
            try:
                g = sdv.cfb.CFBPlayProcess(gameId = game, raw=True).espn_cfb_pbp()


            except (TypeError) as e:
                print("TypeError: yo", e)
                g = sdv.cfb.CFBPlayProcess(gameId = game, raw=True).espn_cfb_pbp()

            except (KeyError) as e:
                print("KeyError: yo", e)
                i+=1
                if i < len(games):
                    print(f"Skipping game {i+1} of {len(games)}, gameId: {games[i]}")
                else:
                    print(f"Skipping game {i} of {len(games)}")
                continue
            except (ValueError) as e:
                print("DecodeError: yo", e)
                i+=1
                if i < len(games):
                    print(f"Skipping game {i+1} of {len(games)}, gameId: {games[i]}")
                else:
                    print(f"Skipping game {i} of {len(games)}")
                continue
            fp = "{}{}.json".format(path_to_raw_json, game)
            with open(fp,'w') as f:
                json.dump(g, f, indent=0, sort_keys=False)
            #     time.sleep(1)
            # if processed_data.playByPlaySource != "none":
            #     processed_data.run_processing_pipeline()
            #     tmp_json = processed_data.plays_json.to_json(orient="records")
            #     jsonified_df = json.loads(tmp_json)
            #     g['plays'] = jsonified_df
            #     print(processed_data.plays_json.index.size)
            #     if processed_data.plays_json.index.size > 50:
            #         box = processed_data.create_box_score()
            #     else:
            #         box = np.array([]).tolist()

            #     result = process_cfb_raw_for_gop(game, g, jsonified_df, box)
            #     g["box_score"] = box
            #     fp = "{}/{}.json".format(path_to_final, game)
            #     with open(fp,'w') as f:
            #         json.dump(g, f, indent=0, sort_keys=False)
            i+=1

if __name__ == "__main__":
    main()
