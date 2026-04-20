param(
    [switch]$Clean,
    [switch]$SkipTests,
    [switch]$SkipSmokeTest
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
$ExeOutputPath = Join-Path $ProjectRoot "dist\datalink-host-gui\datalink-host-gui.exe"
$SmokeReportPath = Join-Path $ProjectRoot "dist\datalink-host-gui\smoke-report.json"

Write-Step "Project root"
Write-Host $ProjectRoot

if (-not (Test-Path -LiteralPath $PythonInVenv)) {
    Write-Step "Creating virtual environment"
    Invoke-CommandArray -Command $PythonCmd -Arguments @("-m", "venv", $VenvPath)
}

Write-Step "Upgrading packaging tools"
& $PythonInVenv -m pip install --upgrade pip setuptools wheel

Write-Step "Installing application and build dependencies"
& $PipInVenv install --no-build-isolation -e '.[build]'

if (-not $SkipTests) {
    Write-Step "Running packaging preflight tests"
    & $PythonInVenv -m unittest tests.test_packaging tests.test_protocol.ProtocolTests.test_datalink_payload_uses_float32_miniseed_encoding
    if ($LASTEXITCODE -ne 0) {
        throw "Preflight tests failed with exit code $LASTEXITCODE."
    }
}

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

if (-not (Test-Path -LiteralPath $ExeOutputPath)) {
    throw "Expected executable was not found at $ExeOutputPath."
}

if (-not $SkipSmokeTest) {
    Write-Step "Running frozen executable smoke test"
    & $ExeOutputPath --self-check --self-check-output $SmokeReportPath
    if ($LASTEXITCODE -ne 0) {
        throw "Frozen smoke test failed with exit code $LASTEXITCODE. See $SmokeReportPath."
    }
    Write-Host "Smoke report created at:"
    Write-Host "  $SmokeReportPath"
}

Write-Step "Done"
Write-Host "Executable created at:"
Write-Host "  $ExeOutputPath"
