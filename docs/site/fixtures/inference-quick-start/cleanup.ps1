$ErrorActionPreference = 'Stop'

& (Join-Path $PSScriptRoot 'run.ps1') cleanup
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}
