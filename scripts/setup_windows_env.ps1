Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

param(
    [switch]$AddFirewallRules,
    [switch]$SkipImportCheck
)

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
            & $candidate.Command[0] @($candidate.Command[1..($candidate.Command.Length - 1)]) --version *> $null
            return $candidate.Command
        } catch {
            continue
        }
    }

    throw "未找到可用的 Python 3.11/3.12。请先安装 Python，再重新运行本脚本。"
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

Write-Step "项目目录"
Write-Host $ProjectRoot
Set-Location $ProjectRoot

$PythonCmd = Resolve-PythonCommand
Write-Step "使用的 Python"
Write-Host ($PythonCmd -join " ")

$VenvPath = Join-Path $ProjectRoot ".venv"
$PythonInVenv = Join-Path $VenvPath "Scripts\python.exe"
$PipInVenv = Join-Path $VenvPath "Scripts\pip.exe"

if (-not (Test-Path -LiteralPath $PythonInVenv)) {
    Write-Step "创建虚拟环境"
    Invoke-CommandArray -Command $PythonCmd -Arguments @("-m", "venv", $VenvPath)
} else {
    Write-Step "虚拟环境已存在，跳过创建"
}

Write-Step "升级 pip"
& $PythonInVenv -m pip install --upgrade pip

Write-Step "安装项目依赖"
& $PipInVenv install -e .

Write-Step "创建运行目录"
Ensure-Directory (Join-Path $ProjectRoot "var")
Ensure-Directory (Join-Path $ProjectRoot "var\logs")
Ensure-Directory (Join-Path $ProjectRoot "var\storage")
Ensure-Directory (Join-Path $ProjectRoot "var\captures")
Ensure-Directory (Join-Path $ProjectRoot "var\datalink-dumps")

if (-not $SkipImportCheck) {
    Write-Step "校验关键依赖导入"
    & $PythonInVenv -c "import datalink_host, PySide6, pyqtgraph, obspy; print('imports-ok')"
}

if ($AddFirewallRules) {
    Write-Step "配置防火墙规则"
    try {
        New-NetFirewallRule -DisplayName "datalink-host-data" -Direction Inbound -Protocol TCP -LocalPort 3677 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "datalink-host-control" -Direction Inbound -Protocol TCP -LocalPort 19001 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        New-NetFirewallRule -DisplayName "datalink-host-datalink-out" -Direction Outbound -Protocol TCP -RemotePort 16000 -Action Allow -ErrorAction SilentlyContinue | Out-Null
        Write-Host "防火墙规则已处理。"
    } catch {
        Write-Warning "防火墙规则配置失败。请以管理员身份运行 PowerShell，或手动放行 3677/19001/16000。"
    }
}

Write-Step "完成"
Write-Host "环境已就绪。"
Write-Host ""
Write-Host "启动 GUI 联调："
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  python -m datalink_host.gui.app"
Write-Host ""
Write-Host "本地一键调试："
Write-Host "  .\.venv\Scripts\Activate.ps1"
Write-Host "  python -m datalink_host.debug_launcher"
