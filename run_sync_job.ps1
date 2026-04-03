param(
    [string]$PythonPath = ".venv\Scripts\python.exe"
)

$repo = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $repo $PythonPath
$runner = Join-Path $repo "run_sync_job.py"

if (-not (Test-Path $pythonExe)) {
    Write-Error "Python executable not found: $pythonExe"
    exit 3
}

if (-not (Test-Path $runner)) {
    Write-Error "Runner script not found: $runner"
    exit 3
}

& $pythonExe $runner @args
exit $LASTEXITCODE
