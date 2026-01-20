$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = $root

python "$root\\scripts\\ingest_news_rss.py" `
  --sources "$root\\news_sources.json" `
  --aliases "$root\\news_team_aliases.json" `
  --limit-per-source 30 `
  --since-hours 72 `
  --require-team-match
