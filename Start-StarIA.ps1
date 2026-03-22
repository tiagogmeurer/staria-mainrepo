# Mata processos antigos
Get-Process -Name "python" -ErrorAction SilentlyContinue | Stop-Process -Force

# C:\AI\Start-StarIA.ps1
$ErrorActionPreference = "Stop"

Write-Host "🚀 Inicializando StarIA..." -ForegroundColor Cyan

# ==============================
# 1) Variáveis de ambiente
# ==============================
$env:ANONYMIZED_TELEMETRY="FALSE"
$env:OTEL_SDK_DISABLED="true"
$env:DRIVE_SYNC_ROOT="G:\Drives compartilhados\STARMKT\StarIA"
$env:STAR_OLLAMA_MODEL="star-llama:latest"

# ==============================
# 2) Verificar Ollama
# ==============================
Write-Host "🔍 Verificando Ollama..."

try {
    $ollamaCheck = Invoke-WebRequest -Uri "http://127.0.0.1:11434" -UseBasicParsing -TimeoutSec 2
    Write-Host "✅ Ollama já está rodando"
}
catch {
    Write-Host "⚠️ Ollama não está rodando. Iniciando..."
    Start-Process "ollama"
    Start-Sleep -Seconds 5
}

# ==============================
# 3) Verificar GPU
# ==============================
Write-Host "🧠 Verificando GPU..."

try {
    $gpu = nvidia-smi
    Write-Host "✅ GPU detectada"
}
catch {
    Write-Host "⚠️ GPU não detectada ou nvidia-smi não disponível"
}

# ==============================
# 4) Subir API (FastAPI)
# ==============================
Write-Host "🌐 Subindo API StarIA..."

Start-Process powershell -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy","Bypass",
  "-Command",
  "cd C:\AI\backend; .\.venv311\Scripts\Activate.ps1; python -m uvicorn app:app --host 127.0.0.1 --port 8088"
)

Start-Sleep -Seconds 3

# ==============================
# 5) Health check API
# ==============================
Write-Host "🔎 Testando API..."

try {
    $health = Invoke-WebRequest -Uri "http://127.0.0.1:8088/healthz" -UseBasicParsing -TimeoutSec 3
    Write-Host "✅ API saudável"
}
catch {
    Write-Host "❌ API não respondeu corretamente"
}

# ==============================
# 6) Subir Bot Telegram
# ==============================
Write-Host "🤖 Subindo Bot Telegram..."

Start-Process powershell -ArgumentList @(
  "-NoExit",
  "-ExecutionPolicy","Bypass",
  "-Command",
  "cd C:\AI\telegram_bot; .\run.ps1"
)

# ==============================
# 7) Final
# ==============================
Write-Host ""
Write-Host "🔥 StarIA ONLINE" -ForegroundColor Green
Write-Host "🌐 API: http://127.0.0.1:8088/docs"
Write-Host "🤖 Bot: ativo no Telegram"
Write-Host ""