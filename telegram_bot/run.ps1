$ErrorActionPreference = "Continue"

$ProjectDir = "C:\AI\telegram_bot"
$PythonExe  = "C:\AI\backend\.venv311\Scripts\python.exe"
$BotFile    = "bot.py"
$RestartDelaySeconds = 5

function Write-Log {
    param([string]$Message)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $Message"
}

if (-not (Test-Path $ProjectDir)) {
    Write-Host "ERRO: pasta do projeto não encontrada: $ProjectDir"
    exit 1
}

if (-not (Test-Path $PythonExe)) {
    Write-Host "ERRO: Python não encontrado em: $PythonExe"
    exit 1
}

$BotPath = Join-Path $ProjectDir $BotFile
if (-not (Test-Path $BotPath)) {
    Write-Host "ERRO: bot.py não encontrado em: $BotPath"
    exit 1
}

Set-Location $ProjectDir

Write-Log "Watchdog do StarIA iniciado."
Write-Log "Projeto: $ProjectDir"
Write-Log "Python:  $PythonExe"
Write-Log "Bot:     $BotPath"

while ($true) {
    try {
        Write-Log "Iniciando StarIA via Telegram Bot..."
        & $PythonExe $BotFile
        $exitCode = $LASTEXITCODE

        if ($exitCode -eq $null) {
            $exitCode = 0
        }

        Write-Log "Bot finalizou com exit code: $exitCode"
    }
    catch {
        Write-Log "Bot caiu com exceção: $($_.Exception.Message)"
    }

    Write-Log "Reiniciando em $RestartDelaySeconds segundos..."
    Start-Sleep -Seconds $RestartDelaySeconds
}