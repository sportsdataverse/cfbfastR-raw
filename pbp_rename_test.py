import os, json
import pandas as pd
from pathlib import Path
from play_handler import PlayProcess
path_to_proc = "pbp_json_final"
path_to_final = "pbp_proc"

def main():
    years_arr = range(2002,2021)
    for year in years_arr:
        print(year)
        # this finds our json files
        path_to_proc_json = "{}/{}/".format(path_to_proc, year)
        path_to_final_json = "{}/{}/".format(path_to_final, year)
        Path(path_to_proc_json).mkdir(parents=True, exist_ok=True)
        json_files = [pos_json.replace('.json', '') for pos_json in os.listdir(path_to_proc_json) if pos_json.endswith('.json')]


if __name__ == "__main__":
    main()