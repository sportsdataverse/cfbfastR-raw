
import argparse
import concurrent.futures
import gc
import json
import http
import logging
import numpy as np
import os
import pyreadr
import pyarrow as pa
import pandas as pd
import re
import sportsdataverse as sdv
import time
import traceback
import urllib.request
import warnings
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap, repeat
from pathlib import Path
from tqdm import tqdm
warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, filename = "cfbfastR_cfb_raw_logfile.txt")
logger = logging.getLogger(__name__)

path_to_raw = "cfb/json/raw"
path_to_clean = "cfb/json/clean"
path_to_final = "cfb/json/final"
path_to_errors = "cfb/errors"
path_to_schedules = "cfb/schedules"
final_file_name = "cfb/cfb_schedule_master.parquet"
github_url_prefix = "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-raw/main/"
run_processing = True
rescrape_all = False
MAX_THREADS = 30

def download_game_pbps(games, process, path_to_raw, path_to_final):
    threads = min(MAX_THREADS, len(games))

    with concurrent.futures.ThreadPoolExecutor(max_workers = threads) as executor:
        result = list(executor.map(download_game, games, repeat(process), repeat(path_to_raw), repeat(path_to_final)))
        return result

def download_game(game, process, path_to_raw, path_to_final):

    # this finds our json files
    path_to_raw_json = f"{path_to_raw}/"
    path_to_final_json = f"{path_to_final}/"
    Path(path_to_raw_json).mkdir(parents = True, exist_ok = True)
    Path(path_to_final_json).mkdir(parents = True, exist_ok = True)
    try:
        g = CFBPlayProcess(gameId = game, raw = True).espn_cfb_pbp()

        with open(f"{path_to_raw_json}{game}.json", "w") as f:
            json.dump(g, f, indent = 0, sort_keys = False)
    except (TypeError) as e:
        logger.exception(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (IndexError) as e:
        logger.exception(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (KeyError) as e:
        logger.exception(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
        pass
    except (ValueError) as e:
        logger.exception(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    except (AttributeError) as e:
        logger.exception(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")
        pass
    if process == True:
        try:
            processed_data = CFBPlayProcess(gameId = game_id)
            pbp = processed_data.espn_cfb_pbp()
            processed_data.run_processing_pipeline()
            tmp_json = processed_data.plays_json.to_json(orient="records")
            jsonified_df = json.loads(tmp_json)

            box = {}
            if pbp.get("header").get("competitions")[0].get("playByPlaySource", "none") != "none":
                box = processed_data.create_box_score()

            result = {
                "id": game_id,
                "count" : len(jsonified_df),
                "plays" : jsonified_df,
                "advBoxScore" : box,
                "homeTeamId": pbp["header"]["competitions"][0]["competitors"][0]["team"]["id"],
                "awayTeamId": pbp["header"]["competitions"][0]["competitors"][1]["team"]["id"],
                "drives" : pbp["drives"],
                "scoringPlays" : np.array(pbp["scoringPlays"]).tolist(),
                "winprobability" : np.array(pbp["winprobability"]).tolist(),
                "boxScore" : pbp["boxscore"],
                "homeTeamSpread" : np.array(pbp["homeTeamSpread"]).tolist(),
                "overUnder" : np.array(pbp["overUnder"]).tolist(),
                "header" : pbp["header"],
                "broadcasts" : np.array(pbp["broadcasts"]).tolist(),
                "videos" : np.array(pbp["videos"]).tolist(),
                "standings" : pbp["standings"],
                "pickcenter" : np.array(pbp["pickcenter"]).tolist(),
                "espnWinProbability" : np.array(pbp["espnWP"]).tolist(),
                "gameInfo" : np.array(pbp["gameInfo"]).tolist(),
                "season" : np.array(pbp["season"]).tolist()
            }

            fp = "{}{}.json".format(path_to_final_json, game)
            with open(fp,"w") as f:
                json.dump(result, f, indent = 0, sort_keys = False)
        except (FileNotFoundError) as e:
            logger.exception(f"FileNotFoundError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (TypeError) as e:
            logger.exception(f"TypeError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (IndexError) as e:
            logger.exception(f"IndexError:  game_id = {game}\n {traceback.format_exc()}")
            pass
        except (KeyError) as e:
            logger.exception(f"KeyError: game_id =  game_id = {game}\n {traceback.format_exc()}")
            pass
        except (ValueError) as e:
            logger.exception(f"DecodeError: game_id = {game}\n {traceback.format_exc()}")
            pass
        except (AttributeError) as e:
            logger.exception(f"AttributeError: game_id = {game}\n {traceback.format_exc()}")
            pass

    time.sleep(0.5)

def add_game_to_schedule(schedule, year):
    game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
    schedule["game_json"] = schedule["game_id"].astype(int).isin(game_files)
    schedule["game_json_url"] = np.where(
        schedule["game_json"] == True,
        schedule["game_id"].apply(lambda x: f"https://raw.githubusercontent.com/sportsdataverse/cfbfastR-raw/main/cfb/json/final/{x}.json"),
        None
    )
    schedule.to_parquet(f"cfb/schedules/parquet/cfb_schedule_{year}.parquet", index = None)
    pyreadr.write_rds(f"cfb/schedules/rds/cfb_schedule_{year}.rds", schedule, compress = "gzip")
    return

def postprocessing(game_id):
    processed_data = sdv.cfb.CFBPlayProcess(gameId = game_id)
    pbp = processed_data.espn_cfb_pbp()
    processed_data.run_processing_pipeline()
    tmp_json = processed_data.plays_json.to_json(orient="records")
    jsonified_df = json.loads(tmp_json)

    box = {}
    if pbp.get("header").get("competitions")[0].get("playByPlaySource", "none") != "none":
        box = processed_data.create_box_score()

    result = {
        "id": game_id,
        "count" : len(jsonified_df),
        "plays" : jsonified_df,
        "advBoxScore" : box,
        "homeTeamId": pbp["header"]["competitions"][0]["competitors"][0]["team"]["id"],
        "awayTeamId": pbp["header"]["competitions"][0]["competitors"][1]["team"]["id"],
        "drives" : pbp["drives"],
        "scoringPlays" : np.array(pbp["scoringPlays"]).tolist(),
        "winprobability" : np.array(pbp["winprobability"]).tolist(),
        "boxScore" : pbp["boxscore"],
        "homeTeamSpread" : np.array(pbp["homeTeamSpread"]).tolist(),
        "overUnder" : np.array(pbp["overUnder"]).tolist(),
        "header" : pbp["header"],
        "broadcasts" : np.array(pbp["broadcasts"]).tolist(),
        "videos" : np.array(pbp["videos"]).tolist(),
        "standings" : pbp["standings"],
        "pickcenter" : np.array(pbp["pickcenter"]).tolist(),
        "espnWinProbability" : np.array(pbp["espnWP"]).tolist(),
        "gameInfo" : np.array(pbp["gameInfo"]).tolist(),
        "season" : np.array(pbp["season"]).tolist()
    }
    return result

def main():

    if args.start_year < 2004:
        start_year = 2004
    else:
        start_year = args.start_year
    if args.end_year is None:
        end_year = start_year
    else:
        end_year = args.end_year
    process = args.process
    years_arr = range(start_year, end_year + 1)

    for year in years_arr:
        schedule = pd.read_parquet(f"{path_to_schedules}/parquet/cfb_schedule_{year}.parquet", engine = "auto", columns = None)
        schedule = schedule.sort_values(by = ["season", "season_type"], ascending = True)
        schedule["game_id"] = schedule["game_id"].astype(int)
        schedule = schedule[schedule["status_type_completed"] == True]
        if args.rescrape == False:
            game_files = [int(game_file.replace(".json", "")) for game_file in os.listdir(path_to_final)]
            schedule = schedule[~schedule["game_id"].isin(game_files)]
        schedule = schedule[schedule["season"]>=2004]
        logger.info(f"Scraping CFB PBP for {year}...")
        games = schedule[(schedule["season"]==year)].reset_index()["game_id"]

        if len(games) == 0:
            logger.info(f"{len(games)} Games to be scraped, skipping")
        elif len(games) > 0:
            logger.info(f"Number of Games: {len(games)}")
            bad_schedule_keys = pd.DataFrame()
            t0 = time.time()
            download_game_pbps(games, process, path_to_raw, path_to_final)
            t1 = time.time()
            logger.info(f"{(t1-t0)/60} minutes to download {len(games)} game play-by-plays.")

        logger.info(f"Finished CFB PBP for {year}...")

        schedule = add_game_to_schedule(schedule, year)

    gc.collect()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_year", "-s", type = int, required = True, help = "Start year of CFB Schedule period (YYYY)")
    parser.add_argument("--end_year", "-e", type = int, help = "End year of CFB Schedule period (YYYY)")
    parser.add_argument("--rescrape", "-r", type = bool, default = True, help = "Rescrape all games in the schedule period")
    parser.add_argument("--process", "-p", type = bool, default = True, help = "Run processing pipeline for games in the schedule period")
    args = parser.parse_args()

    main()
