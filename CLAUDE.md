# CLAUDE.md — cfbfastR-raw Development Guide

## Repo Overview

`cfbfastR-raw` is the Python-side scraper that pulls ESPN college football
schedules and per-game JSON play-by-play, persists them to disk under
`cfb/schedules/` and `cfb/json/final/{game_id}.json`, and commits the
results back to this repo. Every push to `main` fires a `repository_dispatch`
that wakes up the downstream R parser in `cfbfastR-data`. This repo is the
authoritative cache of raw ESPN CFB payloads — the parsing layer never
re-hits ESPN, it reads from here.

## Pipeline Position

```
ESPN APIs --[python scrape]--> cfbfastR-raw [HERE]
                                    | push trigger
                                    v
                               cfbfastR-data --[release upload]--> sportsdataverse-data
                                                                          | piggyback
                                                                          v
                                                                    cfbfastR R package
```

The push trigger is `.github/workflows/cfbfastR_data_trigger.yaml`, which
fires `repository_dispatch` event-type `daily_cfb_data` against
`sportsdataverse/cfbfastR-data`.

## Build & Development Commands

The repo is driven by `scripts/daily_cfb_scraper.sh`, which sequences
schedule scraping then per-game JSON scraping, then commits + pushes. All
seasons are integer years.

```sh
# Full daily flow for one or more seasons (the entry point CI uses)
bash scripts/daily_cfb_scraper.sh -s 2025 -e 2025 -r false

# Or call the scrapers directly when iterating
python3 python/scrape_cfb_schedules.py -s 2025 -e 2025 -r false
python3 python/scrape_cfb_json.py      -s 2025 -e 2025 -r false

# Helpers (not in the daily flow)
python3 python/process_cfb_json.py
python3 python/cfb_pbp_creation.py     -s 2025 -e 2025 -r false
```

`-r true` forces re-scrape of games already on disk; `-r false` skips
existing files. Output paths the scrapers write under:

- `cfb/schedules/{rds,csv,parquet}/cfb_schedule_{year}.{ext}`
- `cfb/cfb_schedule_master.parquet` — concatenated master schedule
- `cfb/json/final/{game_id}.json` — final clean payload, consumed by `cfbfastR-data`
- `cfb/json/raw/{game_id}.json`   — raw ESPN response (kept for forensics)
- `cfb/pbp/parquet/`              — per-season PBP parquet outputs (when `cfb_pbp_creation.py` runs)
- `cfb/team_box/{rds,csv,parquet,json}/` — per-game team box scores
- `cfb/player_box/{rds,csv,parquet}/`   — per-game player box scores

## Project Structure

```
python/
  scrape_cfb_schedules.py     # ESPN schedule scrape -> cfb/schedules/
  scrape_cfb_json.py          # Per-game JSON scrape -> cfb/json/final/{game_id}.json
  process_cfb_json.py         # JSON post-processing helper
  cfb_pbp_creation.py         # PBP compile/processing (uses sdv.cfb.CFBPlayProcess)
scripts/
  daily_cfb_scraper.sh        # CI / scheduler entry point
cfb/                          # Committed scraped output (consumed downstream)
  schedules/, json/, pbp/, team_box/, player_box/
models/                       # CFB model artifacts (if any)
.github/workflows/
  cfbfastR_data_trigger.yaml  # Fires repository_dispatch on push
```

## Daily Scraper Workflow

`scripts/daily_cfb_scraper.sh` is the current entry point. It loops over
seasons and per season runs:

1. `python3 python/scrape_cfb_schedules.py -s $i -e $i -r $RESCRAPE`
2. `python3 python/scrape_cfb_json.py      -s $i -e $i -r $RESCRAPE`
3. `git add cfb/* && git commit -m "CFB Raw Updated (Start: $i End: $i)" && git push`

The commit message format `"CFB Raw Updated (Start: <YYYY> End: <YYYY>)"`
is load-bearing — the downstream `cfbfastR-data` trigger / parser keys off
the year tokens. Keep the format intact when editing the script.

`cfb_pbp_creation.py` is commented out of the daily flow on purpose; PBP
compile is a heavier downstream concern handled by `cfbfastR-data`.

The Python scrapers depend on `sportsdataverse-py` (declared in
`requirements.txt`); they call `sdv.cfb.espn_cfb_pbp(game_id, raw=True)`
and `sdv.cfb.CFBPlayProcess(...)`. Bug fixes to ESPN parsing belong in
`sportsdataverse-py`'s CFB modules — not here.

## Cross-Repo References

- Downstream parser: <https://github.com/sportsdataverse/cfbfastR-data>
- R package consumer: <https://github.com/sportsdataverse/cfbfastR>
- Python scraper internals (the SDK this repo calls): <https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md>
- Sister raw repos (same shape, different sport): <https://github.com/sportsdataverse/hoopR-raw>, <https://github.com/sportsdataverse/wehoop-wbb-raw>

## Project-Specific Gotchas

- `python/scrape_cfb_json.py` writes JSON under `cfb/json/final/{game_id}.json`. Downstream `cfbfastR-data` reads from `https://raw.githubusercontent.com/sportsdataverse/cfbfastR-raw/main/cfb/...`, so the file paths and commit-to-main are load-bearing.
- The per-push `cfbfastR_data_trigger.yaml` workflow currently fires only on `workflow_dispatch`. If/when this moves to `push`, every commit to `main` will dispatch the parser — coordinate before changing.
- The daily scraper script commits **per season** (one commit per year in the loop). If extending to many seasons in one run, expect many commits; consider switching to a single `git add cfb/* && git commit` after the loop if downstream rate limits become a concern.
- Large additions of `cfb/json/final/*.json` files inflate the repo. Don't reorganize the `cfb/` tree without coordinating the change in `cfbfastR-data`'s creation scripts.
- ESPN JSON schema drift is handled in `sportsdataverse-py` (the call boundary). If a scraper starts dropping fields, fix the SDK first; this repo should stay thin.
- The repo has no `DESCRIPTION` — it is not an R package, even though it lives next to `cfbfastR-raw.Rproj`. Treat it as a pure Python + data repo.

## Commit Convention

The daily scraper writes `"CFB Raw Updated (Start: <YYYY> End: <YYYY>)"`
commits programmatically — leave that format alone. For human-authored
changes (script edits, workflow edits, doc edits), use
[Conventional Commits](https://www.conventionalcommits.org/):

```
feat(scrape): add postseason ID range to scrape_cfb_schedules.py
fix(scrape): retry HTTP 429s in scrape_cfb_json with backoff
chore(deps): bump sportsdataverse-py pin in requirements.txt
ci: align cfbfastR_data_trigger.yaml secret name with org rotation
docs: refresh CLAUDE.md pipeline diagram
```

Prefer scoped subjects (`feat(scrape): ...`, `ci(trigger): ...`). Use
`type!:` or a `BREAKING CHANGE:` footer for breaking changes. Split
unrelated work into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
