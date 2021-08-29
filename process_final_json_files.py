import os, json
import pandas as pd
from pathlib import Path
from play_handler import PlayProcess, process_cfb_raw_for_gop
import warnings
warnings.filterwarnings("ignore")
path_to_raw = "pbp_json_raw"
path_to_final = "pbp_json_final"

def main():
    years_arr = range(2021,2022)
    for year in years_arr:
        print(year)
        # this finds our json files
        path_to_raw_json = "{}/{}/".format(path_to_raw, year)
        path_to_final_json = "{}/".format(path_to_final)
        Path(path_to_final_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        # we need both the json and an index number so use enumerate()
        for index, js in enumerate(json_files):
            try:
                processed_data = PlayProcess( gameId = js, path_to_json = path_to_raw_json)
                pbp = processed_data.cfb_pbp_disk()
                processed_data.run_processing_pipeline()
                tmp_json = processed_data.plays_json.to_json(orient="records")
                jsonified_df = json.loads(tmp_json)
                box = processed_data.create_box_score()

                result = process_cfb_raw_for_gop(js, pbp, jsonified_df, box)

                fp = "{}{}.json".format(path_to_final_json, js)
                with open(fp,'w') as f:
                    json.dump(result, f, indent=2, sort_keys=False)

            except (KeyError) as e:
                print("KeyError: yo", js)
                continue
            except (ValueError) as e:
                print("DecodeError: yo", js)
                continue

if __name__ == "__main__":
    main()