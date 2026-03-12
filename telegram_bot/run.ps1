cd C:\AI\backend

.\.venv311\Scripts\Activate.ps1

python -c "from dotenv import load_dotenv; load_dotenv(r'C:\AI\telegram_bot\.env'); import runpy; runpy.run_path(r'C:\AI\telegram_bot\bot.py', run_name='__main__')"