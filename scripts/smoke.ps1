# Windows launcher for the team's unchanged Bash smoke test.
$ErrorActionPreference = 'Stop'
$project = Split-Path -Parent $PSScriptRoot
$bash = Join-Path $env:ProgramFiles 'Git\bin\bash.exe'
if (-not (Test-Path -LiteralPath $bash)) { throw 'Install Git for Windows (Git Bash is required).' }
if (-not (Test-Path -LiteralPath (Join-Path $project '.venv\Scripts\python.exe'))) {
    throw 'Create .venv and install requirements.txt first; see docs/PERSON_B.md.'
}
Push-Location -LiteralPath $project
try {
    # Translate the host path for Python, but preserve /runs and /dev/stdin for Docker.
    & $bash -c 'python3() { .venv/Scripts/python.exe "$@"; }; docker() { MSYS_NO_PATHCONV=1 docker.exe "$@"; }; export -f python3 docker; bash tests/smoke/run.sh'
    $code = $LASTEXITCODE
} finally { Pop-Location }
exit $code
