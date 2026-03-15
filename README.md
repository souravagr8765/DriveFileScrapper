# 📁 Google Drive Watcher + Telegram Notifier

Monitors a shared Google Drive folder for new files, downloads them, and sends you a Telegram notification.

---

## ⚙️ One-Time Setup

### 1. Install dependencies
```bash
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib requests
```

### 2. Get Google Drive API credentials
1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project (or use existing)
3. Enable **Google Drive API** → APIs & Services → Enable APIs
4. Go to **APIs & Services → Credentials → Create Credentials → OAuth 2.0 Client ID**
5. Application type: **Desktop app**
6. Download the JSON → rename it to `credentials.json` → place in same folder as the script

### 3. Create a Telegram Bot
1. Open Telegram → search for **@BotFather**
2. Send `/newbot` and follow prompts
3. Copy the **bot token** (looks like `123456:ABCdef...`)
4. To get your **chat ID**: message [@userinfobot](https://t.me/userinfobot) on Telegram

### 4. Fill in the config at the top of `drive_watcher.py`
```python
DRIVE_FOLDER_URL   = "https://drive.google.com/drive/folders/YOUR_FOLDER_ID"
TELEGRAM_BOT_TOKEN = "123456:ABCdef..."
TELEGRAM_CHAT_ID   = "987654321"
```
> Make sure your Google account (used for OAuth) has access to the shared Drive folder.

---

## ▶️ Running the Script

```bash
python drive_watcher.py
```

- First run: a browser window will open to authenticate with Google → sign in
- A `token.json` is saved so you won't need to sign in again
- New files are saved to `./downloads/`
- `state.json` tracks what's already been downloaded

---

## 🕐 Automate with Cron (Linux/Mac)

Run every hour automatically:
```bash
crontab -e
```
Add this line:
```
0 * * * * cd /path/to/script && python drive_watcher.py >> watcher.log 2>&1
```

### Windows (Task Scheduler)
- Open Task Scheduler → Create Basic Task
- Trigger: Daily / repeat every 1 hour
- Action: `python C:\path\to\drive_watcher.py`

---

## 📂 File Structure
```
your-folder/
├── drive_watcher.py   ← main script
├── credentials.json   ← from Google Cloud Console (you add this)
├── token.json         ← auto-generated after first login
├── state.json         ← auto-generated, tracks downloaded files
└── downloads/         ← auto-created, new files go here
```

---

## 🔔 Example Telegram Notification

```
📥 Drive Watcher — 2 new file(s) downloaded!

  • Lecture_Notes_Week5.pdf
  • Assignment_3.docx

🕐 2026-03-13 14:00:01
```
