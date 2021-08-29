#!/bin/bash
python scrape_cfb_schedules.py
python scrape_cfb_json.py
git add .
git commit -m "daily update" || echo "No changes to commit"
git push
"C:\Program Files\R\R-4.0.5\bin\Rscript.exe" R/01_cfb_pbp_creation.R
"C:\Program Files\R\R-4.0.5\bin\Rscript.exe" R/02_cfb_team_box_creation.R
"C:\Program Files\R\R-4.0.5\bin\Rscript.exe" R/03_cfb_player_box_creation.R
git add cfb/ cfb_schedule_2002_2021.csv
git commit -m "CFB Play-by-play and Schedules update" || echo "No changes to commit"
git push