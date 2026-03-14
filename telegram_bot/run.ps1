cd C:\AI\telegram_bot

while ($true) {

    Write-Host "Iniciando StarIA via Telegram Bot..."

    C:\AI\backend\.venv311\Scripts\python.exe bot.py

    Write-Host "Bot caiu. Reiniciando em 5 segundos..."

    Start-Sleep -Seconds 5
}