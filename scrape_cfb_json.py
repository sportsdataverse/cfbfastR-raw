import os, json
import re
import http
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import sportsdataverse as sdv
import time
import urllib.request
from urllib.error import URLError, HTTPError, ContentTooShortError
from datetime import datetime
from itertools import chain, starmap
from itertools import chain, starmap
from pathlib import Path
from tqdm import tqdm
path_to_raw = "pbp_json_raw"
path_to_final = "pbp_json_final"
path_to_errors = "cfb/errors"
path_to_schedules = "cfb/schedules"
final_file_name = "cfb_schedule_master.csv"
github_url_prefix = "https://raw.githubusercontent.com/sportsdataverse/cfbfastR-raw/main/"
run_processing = True
def postprocessing(game_id):
    processed_data = sdv.cfb.CFBPlayProcess(gameId = game_id)
    pbp = processed_data.espn_cfb_pbp()
    processed_data.run_processing_pipeline()
    tmp_json = processed_data.plays_json.to_json(orient="records")
    jsonified_df = json.loads(tmp_json)
    box = {}
    if pbp.get('header').get('competitions')[0].get('playByPlaySource', "none") != "none":
        box = processed_data.create_box_score()
    bad_cols = [
        'start.distance', 'start.yardLine', 'start.team.id', 'start.down', 'start.yardsToEndzone', 'start.posTeamTimeouts', 'start.defTeamTimeouts', 
        'start.shortDownDistanceText', 'start.possessionText', 'start.downDistanceText', 'start.pos_team_timeouts', 'start.def_pos_team_timeouts',
        'clock.displayValue',
        'type.id', 'type.text', 'type.abbreviation',
        'end.distance', 'end.yardLine', 'end.team.id','end.down', 'end.yardsToEndzone', 'end.posTeamTimeouts','end.defTeamTimeouts', 
        'end.shortDownDistanceText', 'end.possessionText', 'end.downDistanceText', 'end.pos_team_timeouts', 'end.def_pos_team_timeouts',
        'expectedPoints.before', 'expectedPoints.after', 'expectedPoints.added', 
        'winProbability.before', 'winProbability.after', 'winProbability.added', 
        'scoringType.displayName', 'scoringType.name', 'scoringType.abbreviation'
    ]
    # clean records back into ESPN format
    for record in jsonified_df:
        record["clock"] = {
            "displayValue" : record["clock.displayValue"],
            "minutes" : record["clock.minutes"],
            "seconds" : record["clock.seconds"]
        }

        record["type"] = {
            "id" : record["type.id"],
            "text" : record["type.text"],
            "abbreviation" : record["type.abbreviation"],
        }
        record["modelInputs"] = {
            "start" : {
                "down" : record["start.down"],
                "distance" : record["start.distance"],
                "yardsToEndzone" : record["start.yardsToEndzone"],
                "TimeSecsRem": record["start.TimeSecsRem"],
                "adj_TimeSecsRem" : record["start.adj_TimeSecsRem"],
                "pos_score_diff" : record["pos_score_diff_start"],
                "posTeamTimeouts" : record["start.posTeamTimeouts"],
                "defTeamTimeouts" : record["start.defPosTeamTimeouts"],
                "ExpScoreDiff" : record["start.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio" : record["start.ExpScoreDiff_Time_Ratio"],
                "spread_time" : record['start.spread_time'],
                "pos_team_receives_2H_kickoff": record["start.pos_team_receives_2H_kickoff"],
                "is_home": record["start.is_home"],
                "period": record["period"]
            },
            "end" : {
                "down" : record["end.down"],
                "distance" : record["end.distance"],
                "yardsToEndzone" : record["end.yardsToEndzone"],
                "TimeSecsRem": record["end.TimeSecsRem"],
                "adj_TimeSecsRem" : record["end.adj_TimeSecsRem"],
                "posTeamTimeouts" : record["end.posTeamTimeouts"],
                "defTeamTimeouts" : record["end.defPosTeamTimeouts"],
                "pos_score_diff" : record["pos_score_diff_end"],
                "ExpScoreDiff" : record["end.ExpScoreDiff"],
                "ExpScoreDiff_Time_Ratio" : record["end.ExpScoreDiff_Time_Ratio"],
                "spread_time" : record['end.spread_time'],
                "pos_team_receives_2H_kickoff": record["end.pos_team_receives_2H_kickoff"],
                "is_home": record["end.is_home"],
                "period": record["period"]
            }
        }

        record["expectedPoints"] = {
            "before" : record["EP_start"],
            "after" : record["EP_end"],
            "added" : record["EPA"]
        }

        record["winProbability"] = {
            "before" : record["wp_before"],
            "after" : record["wp_after"],
            "added" : record["wpa"]
        }

        record["start"] = {
            "team" : {
                "id" : record["start.team.id"],
            },
            "pos_team": {
                "id" : record["start.pos_team.id"],
                "name" : record["start.pos_team.name"]
            },
            "def_pos_team": {
                "id" : record["start.def_pos_team.id"],
                "name" : record["start.def_pos_team.name"],
            },
            "distance" : record["start.distance"],
            "yardLine" : record["start.yardLine"],
            "down" : record["start.down"],
            "yardsToEndzone" : record["start.yardsToEndzone"],
            "homeScore" : record["start.homeScore"],
            "awayScore" : record["start.awayScore"],
            "pos_team_score" : record["start.pos_team_score"],
            "def_pos_team_score" : record["start.def_pos_team_score"],
            "pos_score_diff" : record["pos_score_diff_start"],
            "posTeamTimeouts" : record["start.posTeamTimeouts"],
            "defTeamTimeouts" : record["start.defPosTeamTimeouts"],
            "ExpScoreDiff" : record["start.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio" : record["start.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText" : record["start.shortDownDistanceText"],
            "possessionText" : record["start.possessionText"],
            "downDistanceText" : record["start.downDistanceText"],
            "posTeamSpread" : record["start.pos_team_spread"]
        }

        record["end"] = {
            "team" : {
                "id" : record["end.team.id"],
            },
            "pos_team": {
                "id" : record["end.pos_team.id"],
                "name" : record["end.pos_team.name"],
            }, 
            "def_pos_team": {
                "id" : record["end.def_pos_team.id"],
                "name" : record["end.def_pos_team.name"],
            }, 
            "distance" : record["end.distance"],
            "yardLine" : record["end.yardLine"],
            "down" : record["end.down"],
            "yardsToEndzone" : record["end.yardsToEndzone"],
            "homeScore" : record["end.homeScore"],
            "awayScore" : record["end.awayScore"],
            "pos_team_score" : record["end.pos_team_score"],
            "def_pos_team_score" : record["end.def_pos_team_score"],
            "pos_score_diff" : record["pos_score_diff_end"],
            "posTeamTimeouts" : record["end.posTeamTimeouts"],
            "defPosTeamTimeouts" : record["end.defPosTeamTimeouts"],
            "ExpScoreDiff" : record["end.ExpScoreDiff"],
            "ExpScoreDiff_Time_Ratio" : record["end.ExpScoreDiff_Time_Ratio"],
            "shortDownDistanceText" : record["end.shortDownDistanceText"],
            "possessionText" : record["end.possessionText"],
            "downDistanceText" : record["end.downDistanceText"]
        }

        record["players"] = {
            'passer_player_name' : record["passer_player_name"],
            'rusher_player_name' : record["rusher_player_name"],
            'receiver_player_name' : record["receiver_player_name"],
            'sack_player_name' : record["sack_player_name"],
            'sack_player_name2' : record["sack_player_name2"],
            'pass_breakup_player_name' : record["pass_breakup_player_name"],
            'interception_player_name' : record["interception_player_name"],
            'fg_kicker_player_name' : record["fg_kicker_player_name"],
            'fg_block_player_name' : record["fg_block_player_name"],
            'fg_return_player_name' : record["fg_return_player_name"],
            'kickoff_player_name' : record["kickoff_player_name"],
            'kickoff_return_player_name' : record["kickoff_return_player_name"],
            'punter_player_name' : record["punter_player_name"],
            'punt_block_player_name' : record["punt_block_player_name"],
            'punt_return_player_name' : record["punt_return_player_name"],
            'punt_block_return_player_name' : record["punt_block_return_player_name"],
            'fumble_player_name' : record["fumble_player_name"],
            'fumble_forced_player_name' : record["fumble_forced_player_name"],
            'fumble_recovered_player_name' : record["fumble_recovered_player_name"],
        }
        # remove added columns
        for col in bad_cols:
            record.pop(col, None)

    result = {
        "id": game_id,
        "count" : len(jsonified_df),
        "plays" : jsonified_df,
        "box_score" : box,
        "homeTeamId": pbp['header']['competitions'][0]['competitors'][0]['team']['id'],
        "awayTeamId": pbp['header']['competitions'][0]['competitors'][1]['team']['id'],
        "drives" : pbp['drives'],
        "scoringPlays" : np.array(pbp['scoringPlays']).tolist(),
        "winprobability" : np.array(pbp['winprobability']).tolist(),
        "boxScore" : pbp['boxscore'],
        "homeTeamSpread" : np.array(pbp['homeTeamSpread']).tolist(),
        "overUnder" : np.array(pbp['overUnder']).tolist(),
        "header" : pbp['header'],
        "broadcasts" : np.array(pbp['broadcasts']).tolist(),
        "videos" : np.array(pbp['videos']).tolist(),
        "standings" : pbp['standings'],
        "pickcenter" : np.array(pbp['pickcenter']).tolist(),
        "espnWinProbability" : np.array(pbp['espnWP']).tolist(),
        "gameInfo" : np.array(pbp['gameInfo']).tolist(),
        "season" : np.array(pbp['season']).tolist()
    }
    return result

def main():
    years_arr = range(2022,2023)
    for year in years_arr:
        print("Scraping year {}...".format(year))
        schedule = pd.read_parquet(f"{path_to_schedules}/parquet/cfb_schedules_{year}.parquet", engine='auto', columns=None)
        schedule = schedule.sort_values(by=['season','season_type'], ascending = True)
        schedule["game_id"] = schedule["game_id"].astype(str)

        schedule = schedule[schedule['status_type_completed']==True]
        schedule_with_pbp = schedule[schedule['season']>=2004]

        games = schedule[(schedule['season']==year)].reset_index()['game_id']
        print(f"Number of Games: {len(games)}")
        bad_schedule_keys = pd.DataFrame()
        # this finds our json files
        path_to_raw_json = "{}/".format(path_to_raw)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_raw_json).mkdir(parents=True, exist_ok=True)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        for game in games:
            try:
                g = sdv.cfb.CFBPlayProcess(gameId = game, raw=True).espn_cfb_pbp()

            except (TypeError) as e:
                print("TypeError: game_id = {}\n {}".format(game, e))
                # bad_schedule_keys = pd.concat([bad_schedule_keys, pd.DataFrame({"game_id": game})],ignore_index=True)
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
                    result = postprocessing(game_id=game)

                    fp = "{}{}.json".format(path_to_final_json, game)
                    with open(fp,'w') as f:
                        json.dump(result, f, indent=2, sort_keys=False)
                # except (IndexError) as e:
                #     print("IndexError: game_id = {}\n {}".format(game, e))
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
    csv_files = [pos_csv.replace('.csv', '') for pos_csv in os.listdir(path_to_schedules+'/csv') if pos_csv.endswith('.csv')]
    glued_data = pd.DataFrame()
    for index, js in enumerate(csv_files):
        x = pd.read_csv(f"{path_to_schedules}/csv/{js}.csv", low_memory=False)
        glued_data = pd.concat([glued_data,x],axis=0)
    glued_data.to_csv(final_file_name, index=False)
    glued_data.to_parquet(final_file_name.replace('.csv', '.parquet'), index=False)

if __name__ == "__main__":
    main()
