#!/bin/bash
python scrape_cfb_schedules.py
python scrape_cfb_json.py
python cfb_pbp_creation.py
git add .
git add cfb/ cfb_schedule_master.csv cfb_schedule_master.parquet
git commit -m "CFB Play-by-Play and Schedules update" || echo "No changes to commit"
git push