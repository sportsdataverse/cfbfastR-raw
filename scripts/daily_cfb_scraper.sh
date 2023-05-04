#!/bin/bash
while getopts s:e:r: flag
do
    case "${flag}" in
        s) START_YEAR=${OPTARG};;
        e) END_YEAR=${OPTARG};;
        r) RESCRAPE=${OPTARG};;
    esac
done
for i in $(seq ${START_YEAR} ${END_YEAR})
do
    echo "$i"
    git config --local user.email "action@github.com"
    git config --local user.name "Github Action"
    git pull
    python3 python/scrape_cfb_schedules.py -s $i -e $i -r $RESCRAPE
    python3 python/scrape_cfb_json.py -s $i -e $i -r $RESCRAPE
    # python3 python/cfb_pbp_creation.py -s $i -e $i -r $RESCRAPE
    git pull
    git add .
    git pull  >> /dev/null
    git add cfb/* >> /dev/null
    git pull  >> /dev/null
    git commit -m "CFB Raw Updated (Start: $i End: $i)" || echo "No changes to commit"
    git pull --rebase  >> /dev/null
    git push --quiet
done