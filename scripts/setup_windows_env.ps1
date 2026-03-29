param(
    [switch]$AddFirewallRules,
    [switch]$SkipImportCheck
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

function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir

Write-Step "Project root"
Write-Host $ProjectRoot
Set-Location $ProjectRoot

$PythonCmd = Resolve-PythonCommand
Write-Step "Using Python"
Write-Host ($PythonCmd -join " ")

$VenvPath = Join-Path $ProjectRoot ".venv"
$PythonInVenv = Join-Path $VenvPath "Scripts\python.exe"
$PipInVenv = Join-Path $VenvPath "Scripts\pip.exe"

if (-not (Test-Path -LiteralPath $PythonInVenv)) {
    Write-Step "Creating virtual environment"
    Invoke-CommandArray -Command $PythonCmd -Arguments @("-m", "venv", $VenvPath)
} else {
    Write-Step "Virtual environment already exists"
}

Write-Step "Upgrading pip"
& $PythonInVenv -m pip install --upgrade pip

Write-Step "Installing project dependencies"
& $PipInVenv install -e .

Write-Step "Creating runtime directories"
Ensure-Directory (Join-Path $ProjectRoot "var")
Ensure-Directory (Join-Path $ProjectRoot "var\logs")
Ensure-Directory (Join-Path $ProjectRoot "var\storage")
Ensure-Directory (Join-Path $ProjectRoot "var\captures")
Ensure-Directory (Join-Path $ProjectRoot "var\datalink-dumps")

if (-not $SkipImportCheck) {
    Write-Step "Checking imports"
    & $PythonInVenv -c "import datalink_host, PySide6, pyqtgraph, obspy; print('imports-ok')"
}

if ($AddFirewallRules) {
    Write-Step "Adding firewall rules"
    try {
        New-NetFirewallRule -DisplayName "datalink-host-data" -Direction Inbound -Protocol TCP -LocalPort 3677 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "datalink-host-control" -Direction Inbound -Protocol TCP -LocalPort 19001 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "datalink-host-datalink-out" -Direction Outbound -Protocol TCP -RemotePort 16000 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        Write-Host "Firewall rules processed."
    } catch {
        Write-Warning "Failed to add firewall rules. Run PowerShell as Administrator, or add 3677/19001/16000 manually."
    }
}

Write-Step "Done"
Write-Host "Environment is ready."
Write-Host ""
Write-Host "Start GUI for real device integration:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  python -m datalink_host.gui.app"
Write-Host ""
Write-Host "Start all-in-one local debug mode:"
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  python -m datalink_host.debug_launcher"
