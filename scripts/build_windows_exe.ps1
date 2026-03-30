param(
    [switch]$Clean
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Resolve-PythonCommand {
    $candidates = @(
        @{ Label = "py -3.12"; Command = @("py", "-3.12") },
        @{ Label = "py -3.11"; Command = @("py", "-3.11") },
        @{ Label = "python"; Command = @("python") }
    )

    foreach ($candidate in $candidates) {
        try {
            $exe = $candidate.Command[0]
            $args = @()
            if ($candidate.Command.Length -gt 1) {
                $args = $candidate.Command[1..($candidate.Command.Length - 1)]
            }
            & $exe @args --version *> $null
            return $candidate.Command
        } catch {
            continue
        }
    }

    throw "No usable Python 3.11/3.12 installation was found. Install Python first, then rerun this script."
}

function Invoke-CommandArray {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Command,
        [Parameter(ValueFromRemainingArguments = $true)]
        [object[]]$Arguments
    )

    $exe = $Command[0]
    $prefixArgs = @()
    if ($Command.Length -gt 1) {
        $prefixArgs = $Command[1..($Command.Length - 1)]
    }
    & $exe @prefixArgs @Arguments
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$PythonCmd = Resolve-PythonCommand
$VenvPath = Join-Path $ProjectRoot ".venv"
$PythonInVenv = Join-Path $VenvPath "Scripts\python.exe"
$PipInVenv = Join-Path $VenvPath "Scripts\pip.exe"
$PyInstallerInVenv = Join-Path $VenvPath "Scripts\pyinstaller.exe"

Write-Step "Project root"
Write-Host $ProjectRoot

if (-not (Test-Path -LiteralPath $PythonInVenv)) {
    Write-Step "Creating virtual environment"
    Invoke-CommandArray -Command $PythonCmd -Arguments @("-m", "venv", $VenvPath)
}

Write-Step "Upgrading pip"
& $PythonInVenv -m pip install --upgrade pip

Write-Step "Installing application and build dependencies"
& $PipInVenv install -e '.[build]'

if ($Clean) {
    Write-Step "Cleaning previous build output"
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue (Join-Path $ProjectRoot "build")
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue (Join-Path $ProjectRoot "dist")
}

Write-Step "Building Windows executable"
& $PyInstallerInVenv --noconfirm --clean .\datalink-host-gui.spec
if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build failed with exit code $LASTEXITCODE."
}

Write-Step "Done"
Write-Host "Executable created at:"
Write-Host "  .\dist\datalink-host-gui.exe"
