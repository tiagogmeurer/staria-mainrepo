# C:\AI\Start-StarIA.ps1
$ErrorActionPreference = "Stop"

# 1) Variáveis de ambiente (telemetria off, caminhos)
$env:ANONYMIZED_TELEMETRY="FALSE"
$env:OTEL_SDK_DISABLED="true"
$env:DRIVE_SYNC_ROOT="G:\Drives compartilhados\STARMKT\StarIA"

# 2) Sobe API (FastAPI)
Start-Process powershell -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy","Bypass",
  "-Command",
  "cd C:\AI\backend; .\.venv311\Scripts\Activate.ps1; python -m uvicorn app:app --host 127.0.0.1 --port 8088"
)

# 3) Sobe Bot Telegram (polling)
Start-Process powershell -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy","Bypass",
  "-Command",
  "cd C:\AI\telegram_bot; .\run.ps1"
)

Write-Host "StarIA iniciado: API em http://127.0.0.1:8088/docs e Bot rodando no Telegram."