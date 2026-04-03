Param(
    [string]$PythonExe = ".venv\Scripts\python.exe",
    [string]$OutputDir = "release"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

& $PythonExe -m pip install --upgrade pip pyinstaller

if (Test-Path "build") { Remove-Item -Recurse -Force "build" }
if (Test-Path "dist") { Remove-Item -Recurse -Force "dist" }
if (Test-Path $OutputDir) { Remove-Item -Recurse -Force $OutputDir }
New-Item -ItemType Directory -Path $OutputDir | Out-Null

& $PythonExe -m PyInstaller --clean --noconfirm --onefile --windowed --name run_sync_job run_sync_job.py

Copy-Item "dist\*.exe" $OutputDir
Copy-Item "config.example.yaml" $OutputDir
if (Test-Path "run_sync_job.ps1") { Copy-Item "run_sync_job.ps1" $OutputDir }
if (Test-Path "README.md") { Copy-Item "README.md" $OutputDir }

Write-Output "Build completed: $OutputDir"
