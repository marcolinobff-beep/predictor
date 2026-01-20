$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$env:PYTHONPATH = $root

$envFile = Join-Path $root ".env"
if (Test-Path $envFile) {
  Get-Content $envFile | ForEach-Object {
    if ($_ -match '^\s*ODDS_API_KEY\s*=\s*(.+)\s*$') {
      $env:ODDS_API_KEY = $matches[1]
    }
  }
}

$today = (Get-Date).ToString("yyyy-MM-dd")
python "$root\\scripts\\ingest_odds_oddsapi_day.py" `
  --date $today `
  --competition "Serie_A"
