import os, json
import pandas as pd
from pathlib import Path
from play_handler import PlayProcess
path_to_raw = "pbp_json_raw"
path_to_proc = "pbp_proc"

def main():
    years_arr = range(2002,2021)
    for year in years_arr:
        print(year)
        # this finds our json files
        path_to_raw_json = "{}/{}/".format(path_to_raw, year)
        path_to_proc_json = "{}/{}/".format(path_to_proc, year)
        Path(path_to_proc_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_raw_json) if pos_json.endswith('.json')]

        # we need both the json and an index number so use enumerate()
        for index, js in enumerate(json_files):
            processed_data = PlayProcess( gameId = js, path_to_json = path_to_raw_json)
            pbp = processed_data.cfb_pbp_disk()
            processed_data.run_cleaning_pipeline()
            processed_data.run_cleaning_pipeline()
            tmp_json = processed_data.plays_json.to_json(orient="records")
            jsonified_df = json.loads(tmp_json)
            pbp["plays"] = jsonified_df
            fp = "{}{}.json".format(path_to_proc_json, js)
            with open(fp,'w') as f:
                json.dump(pbp, f, indent=2, sort_keys=False)


if __name__ == "__main__":
    main()