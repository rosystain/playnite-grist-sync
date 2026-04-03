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

$targets = @(
    "sync_playnite_to_grist.py",
    "sync_grist_to_playnite.py",
    "run_sync_job.py"
)

foreach ($t in $targets) {
    & $PythonExe -m PyInstaller --clean --noconfirm --onefile --name ([System.IO.Path]::GetFileNameWithoutExtension($t)) $t
}

Copy-Item "dist\*.exe" $OutputDir
Copy-Item "config.example.yaml" $OutputDir
if (Test-Path "run_sync_job.ps1") { Copy-Item "run_sync_job.ps1" $OutputDir }
if (Test-Path "README.md") { Copy-Item "README.md" $OutputDir }

Write-Output "Build completed: $OutputDir"
