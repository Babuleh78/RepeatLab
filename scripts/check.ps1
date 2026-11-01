[CmdletBinding()]
param([string]$UvPath = "")
$ErrorActionPreference = 'Stop'
if (-not $UvPath) {
    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($command) { $UvPath = $command.Source }
    else {
        $candidate = Join-Path $env:LOCALAPPDATA 'Microsoft/WinGet/Packages/astral-sh.uv_Microsoft.Winget.Source_8wekyb3d8bbwe/uv.exe'
        if (Test-Path -LiteralPath $candidate) { $UvPath = $candidate }
        else { throw 'uv 0.13.5 not found. Install uv or pass -UvPath.' }
    }
}
function Invoke-Uv {
    param([string[]]$Arguments)
    & $UvPath @Arguments
    if ($LASTEXITCODE -ne 0) { throw "uv $($Arguments -join ' ') failed with exit code $LASTEXITCODE" }
}
Push-Location (Split-Path $PSScriptRoot -Parent)
try {
    if ($env:JAVA_HOME) { $env:Path = (Join-Path $env:JAVA_HOME 'bin') + ';' + $env:Path }
    & ./mvnw.cmd -B -f java/pom.xml verify
    if ($LASTEXITCODE -ne 0) { throw "Maven verify failed with exit code $LASTEXITCODE" }
    Invoke-Uv @('sync', '--locked', '--dev')
    Invoke-Uv @('run', '--locked', 'ruff', 'check', '.')
    Invoke-Uv @('run', '--locked', 'ruff', 'format', '--check', '.')
    Invoke-Uv @('run', '--locked', 'mypy', 'src', 'tests')
    Invoke-Uv @('run', '--locked', 'pytest')
    Invoke-Uv @('build', '--no-sources')
} finally { Pop-Location }
