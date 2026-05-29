# cfbfastR-raw Copilot Instructions

## Project Context

This repo is the Python ESPN-scrape stage for college football. It writes
per-game JSON under `cfb/json/final/{game_id}.json` and commits results
to `main`. Pushes to `main` are the upstream signal for the downstream
R parser in `cfbfastR-data`; the dispatch workflow lives at
`.github/workflows/cfbfastR_data_trigger.yaml` (event-type
`daily_cfb_data`, currently fires on `workflow_dispatch`).

Pipeline: `ESPN -> cfbfastR-raw [HERE] -> cfbfastR-data -> sportsdataverse-data -> cfbfastR`.

## Repository Workflow

- Branch from `main`; `main` is the default and release branch.
- The CI entry point is `scripts/daily_cfb_scraper.sh -s <START> -e <END> -r <true|false>`.
- The shell script commits per season with message `"CFB Raw Updated (Start: <YYYY> End: <YYYY>)"` — this format is load-bearing for downstream tooling; do not reword.
- Scrapers shell out to `sportsdataverse-py`. Fix ESPN parser bugs upstream there, not here.
- Don't reorganize the `cfb/` output tree without aligning `cfbfastR-data`'s creation scripts.

## Build & Development Commands

```sh
bash scripts/daily_cfb_scraper.sh   -s 2025 -e 2025 -r false
python3 python/scrape_cfb_schedules.py -s 2025 -e 2025 -r false
python3 python/scrape_cfb_json.py      -s 2025 -e 2025 -r false
python3 python/process_cfb_json.py
python3 python/cfb_pbp_creation.py     -s 2025 -e 2025 -r false   # not in daily flow
```

`-r true` forces re-scrape; `-r false` skips files already on disk. Outputs:

- `cfb/schedules/{rds,csv,parquet}/cfb_schedule_{year}.{ext}`
- `cfb/cfb_schedule_master.parquet` (concatenated master schedule)
- `cfb/json/final/{game_id}.json` (consumed downstream)
- `cfb/json/raw/{game_id}.json` (forensics)
- `cfb/pbp/parquet/` — per-season PBP parquet (when `cfb_pbp_creation.py` runs)
- `cfb/team_box/{rds,csv,parquet,json}/`
- `cfb/player_box/{rds,csv,parquet}/`

## Code Style

- Follow the parent SDK's Python conventions: `snake_case`, 4-space indent.
- Prefer `pathlib.Path`, `concurrent.futures` for parallelism, `tqdm` for progress.
- Don't add bespoke ESPN parsing here — call into `sportsdataverse.cfb.*` (especially `CFBPlayProcess`) and persist its output.
- Keep `requirements.txt` minimal; pin `sportsdataverse-py` if a behavior change is needed.
- Logging via `logging.basicConfig(... filename='cfbfastR_cfb_raw_logfile.txt')` is the established pattern; new scrapers should use the same logger configuration so the daily log stays unified.

## Cross-Repo References

- Downstream parser: <https://github.com/sportsdataverse/cfbfastR-data>
- R package: <https://github.com/sportsdataverse/cfbfastR>
- SDK internals: <https://github.com/sportsdataverse/sportsdataverse-py/blob/main/CLAUDE.md>

## Conventional Commits

For human-authored changes (the daily scraper's auto-commits use a fixed
`"CFB Raw Updated (Start: <YYYY> End: <YYYY>)"` format — leave that alone),
use `type(scope): description`. Common types: `feat`, `fix`, `chore`, `ci`,
`docs`, `refactor`. Use `type!:` or a `BREAKING CHANGE:` footer for
breaking changes.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot, Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By` trailers referencing AI tools. This applies whether the change was generated, refactored, or reviewed with AI assistance — the human author is the sole attributable contributor.
