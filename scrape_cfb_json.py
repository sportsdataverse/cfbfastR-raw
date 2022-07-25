import os, json
import re
import http
import pyreadr
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
path_to_errors = "cfb/errors"
run_processing = False
def main():
    years_arr = range(2018,2022)
    schedule = pd.read_parquet('cfb_schedule_master.parquet', engine='auto', columns=None)
    schedule = schedule.sort_values(by=['season','season_type'], ascending = True)
    schedule["game_id"] = schedule["game_id"].astype(str)

    schedule = schedule[schedule['status_type_completed']==True]
    schedule_with_pbp = schedule[schedule['season']>=2004]

    for year in years_arr:
        print("Scraping year {}...".format(year))
        games = schedule[(schedule['season']==year)].reset_index()['game_id']
        print(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()
        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        # json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        for game in games:
            try:
                g = sdv.cfb.CFBPlayProcess(gameId = game, raw=True).espn_cfb_pbp()


            except (TypeError) as e:
                print("TypeError: game_id = {}\n {}".format(game, e))
                bad_schedule_keys = pd.concat([bad_schedule_keys, pd.DataFrame({"game_id": game})],ignore_index=True)
                continue
            except (IndexError) as e:
                print("IndexError: game_id = {}\n {}".format(game, e))
                continue
            except (KeyError) as e:
                print("KeyError: game_id = {}\n {}".format(game, e))
                continue
            except (ValueError) as e:
                print("DecodeError: game_id = {}\n {}".format(game, e))
                continue
            except (AttributeError) as e:
                print("AttributeError: game_id = {}\n {}".format(game, e))
                continue
            fp = "{}{}.json".format(path_to_raw_json, game)
            with open(fp,'w') as f:
                json.dump(g, f, indent=0, sort_keys=False)
                time.sleep(1)
            if run_processing == True:
                try:
                    processed_data = sdv.cfb.CFBPlayProcess(gameId = game, path_to_json = path_to_raw_json)
                    # processed_data = sdv.cfb.CFBPlayProcess( gameId = 242620142, path_to_json = 'pbp_json_raw/')
                    pbp = processed_data.cfb_pbp_disk()
                    result = processed_data.run_processing_pipeline()
                    if result.get("playByPlaySource", "none") != "none":
                        result['box_score'] = processed_data.create_box_score()
                    fp = "{}{}.json".format(path_to_final_json, game)
                    with open(fp,'w') as f:
                        json.dump(result, f, indent=2, sort_keys=False)
                except (IndexError) as e:
                    print("IndexError: game_id = {}\n {}".format(game, e))
                except (KeyError) as e:
                    print("KeyError: game_id = {}\n {}".format(game, e))
                    continue
                except (ValueError) as e:
                    print("DecodeError: game_id = {}\n {}".format(game, e))
                    continue
                except (AttributeError) as e:
                    print("AttributeError: game_id = {}\n {}".format(game, e))
                    continue



        # path_to_csv = "{}/{}/".format(path_to_errors, 'csv')
        # path_to_parquet = "{}/{}/".format(path_to_errors, 'parquet')
        # path_to_rds = "{}/{}/".format(path_to_errors, 'rds')
        # path_to_json = "{}/{}/".format(path_to_errors, 'json')
        # Path(path_to_csv).mkdir(parents=True, exist_ok=True)
        # Path(path_to_parquet).mkdir(parents=True, exist_ok=True)
        # Path(path_to_rds).mkdir(parents=True, exist_ok=True)
        # Path(path_to_json).mkdir(parents=True, exist_ok=True)

        # bad_schedule_keys.to_csv(f"{path_to_errors}/csv/cfb_schedule_{year}.csv", index = False)
        # bad_schedule_keys.to_parquet(f"{path_to_errors}/parquet/cfb_schedule_{year}.parquet", index = False)
        # pyreadr.write_rds(f"{path_to_errors}/rds/cfb_schedule_{year}.rds", bad_schedule_keys)
        # fp = "{}/cfb_schedule_{}.json".format(path_to_json, year)
        # with open(fp,'w') as f:
        #     json.dump(bad_schedule_keys.to_json(orient='records'), f, indent=0, sort_keys=False)
        print("Finished Scraping year {}...".format(year))
if __name__ == "__main__":
    main()
