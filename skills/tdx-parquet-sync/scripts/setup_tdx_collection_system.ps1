param(
  [string]$TdxRoot = "C:\new_tdx64",
  [string]$OutputRoot = "C:\tdx_parquet",
  [string]$DailyTime = "16:00",
  [string]$TaskName = "TDX_Incremental_Daily_1600",
  [string]$PythonExe = "",
  [switch]$SkipBootstrap,
  [switch]$SkipSchedule
)

$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Candidate)
  if ($Candidate -and (Test-Path $Candidate)) { return $Candidate }
  if (Test-Path "C:\Python314\python.exe") { return "C:\Python314\python.exe" }
  $cmd = Get-Command python -ErrorAction SilentlyContinue
  if ($cmd) { return "python" }
  throw "Python not found. Set -PythonExe explicitly."
}

if ($DailyTime -notmatch "^(?:[01]\d|2[0-3]):[0-5]\d$") {
  throw "Invalid -DailyTime. Expected HH:mm, for example 16:00."
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$syncScript = Join-Path $scriptDir "sync_tdx_full_to_parquet.py"
$requirements = Join-Path $scriptDir "requirements.txt"

if (-not (Test-Path $syncScript)) { throw "Missing sync script: $syncScript" }
if (-not (Test-Path $requirements)) { throw "Missing requirements: $requirements" }
if (-not (Test-Path (Join-Path $TdxRoot "vipdoc"))) {
  throw "Invalid TDX root: $TdxRoot (vipdoc not found)"
}

$py = Resolve-PythonExe -Candidate $PythonExe
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

Write-Host "[1/4] Install Python dependencies..." -ForegroundColor Cyan
& $py -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) { throw "pip install failed with code $LASTEXITCODE" }

if (-not $SkipBootstrap) {
  Write-Host "[2/4] Run full bootstrap sync..." -ForegroundColor Cyan
  $bootstrapSummary = Join-Path $OutputRoot ("full_bootstrap_summary_{0}.json" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
  & $py $syncScript `
    --tdx-root $TdxRoot `
    --output-root $OutputRoot `
    --datasets "daily,min5,reference" `
    --markets "sh,sz,bj" `
    --full-rebuild `
    --summary-json $bootstrapSummary
  if ($LASTEXITCODE -ne 0) { throw "Full bootstrap failed with code $LASTEXITCODE" }
}
else {
  Write-Host "[2/4] Skip full bootstrap (-SkipBootstrap)." -ForegroundColor Yellow
}

$runnerPath = Join-Path $OutputRoot "run_tdx_incremental_daily.ps1"
$runnerContent = @"
`$ErrorActionPreference = "Stop"
& "$py" "$syncScript" `
  --tdx-root "$TdxRoot" `
  --output-root "$OutputRoot" `
  --datasets "daily,min5,reference" `
  --markets "sh,sz,bj" `
  --summary-json "$OutputRoot\last_run_summary.json"
exit `$LASTEXITCODE
"@
Set-Content -LiteralPath $runnerPath -Value $runnerContent -Encoding UTF8
Write-Host "[3/4] Incremental runner prepared: $runnerPath" -ForegroundColor Cyan

if (-not $SkipSchedule) {
  Write-Host "[4/4] Create/Update daily task at $DailyTime ..." -ForegroundColor Cyan
  $taskCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$runnerPath`""
  schtasks /Create /TN $TaskName /TR $taskCmd /SC DAILY /ST $DailyTime /RU SYSTEM /F | Out-Null
  schtasks /Query /TN $TaskName /V /FO LIST
}
else {
  Write-Host "[4/4] Skip schedule creation (-SkipSchedule)." -ForegroundColor Yellow
}

Write-Host "Setup completed." -ForegroundColor Green
