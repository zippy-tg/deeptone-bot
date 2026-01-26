# TikTok Creator Payment Tracker Bot

A Discord bot that helps track payments to TikTok creators and prevents duplicate payments for the same video.

## Features

- **Duplicate Detection**: Automatically detects if a video has already been paid for
- **Multiple URL Formats**: Handles standard TikTok URLs, short URLs (vm.tiktok.com), and mobile share links
- **Payment Tracking**: Store creator name, payment amount, currency, and notes for each video
- **Statistics**: View overall stats, creator breakdowns, and monthly summaries
- **CSV Export**: Export all payment records for bookkeeping
- **Edit/Delete**: Modify or remove entries with confirmation prompts
- **Rich Embeds**: Clean Discord embeds with color-coded status indicators

## Commands

| Command | Description |
|---------|-------------|
| `!submit [URL]` | Submit a TikTok video for payment tracking |
| `!stats` | Show summary statistics |
| `!creator [name]` | Show all videos and payments for a specific creator |
| `!recent` | Show last 10 video submissions |
| `!search [start] [end]` | Search videos by date range (YYYY-MM-DD) |
| `!monthly [month] [year]` | Show monthly payment summary |
| `!export` | Export all records to CSV |
| `!edit [video_id] [amount]` | Edit payment amount for a video |
| `!delete [video_id]` | Delete a video record (with confirmation) |
| `!lookup [video_id]` | Look up a specific video by ID |
| `!help` | List all available commands |

## Setup

### 1. Create a Discord Bot

1. Go to the [Discord Developer Portal](https://discord.com/developers/applications)
2. Click "New Application" and give it a name
3. Go to the "Bot" section in the left sidebar
4. Click "Add Bot"
5. Under "Privileged Gateway Intents", enable:
   - **Message Content Intent** (required for reading commands)
6. Click "Reset Token" and copy your bot token

### 2. Invite the Bot to Your Server

1. In the Developer Portal, go to "OAuth2" > "URL Generator"
2. Select the following scopes:
   - `bot`
   - `applications.commands`
3. Select the following bot permissions:
   - Send Messages
   - Send Messages in Threads
   - Embed Links
   - Attach Files
   - Read Message History
   - Add Reactions
   - Use External Emojis
4. Copy the generated URL and open it in your browser
5. Select your server and authorize the bot

### 3. Install Dependencies

```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
# Copy the example env file
cp .env.example .env

# Edit .env and add your bot token
# Windows: notepad .env
# Linux/Mac: nano .env
```

Your `.env` file should look like:
```
DISCORD_BOT_TOKEN=your_actual_bot_token_here
COMMAND_PREFIX=!
DEFAULT_CURRENCY=USD
```

### 5. Run the Bot

```bash
python bot.py
```

You should see:
```
2025-01-23 10:00:00 | INFO     | payment_bot | Starting Payment Tracker Bot...
2025-01-23 10:00:01 | INFO     | payment_bot | Logged in as YourBot#1234 (ID: 123456789)
```

## Usage Example

### Submitting a New Video

```
You: !submit https://www.tiktok.com/@username/video/7123456789

Bot: 🎬 New Video Detected!
     Video ID: 7123456789
     URL: https://www.tiktok.com/@username/video/7123456789

Bot: 👤 Please enter the creator's name:

You: JohnDoe

Bot: 💰 Enter payment amount (e.g., 50, $50, €50 EUR):

You: 50

Bot: 📝 Any notes? (type 'skip' or 'none' to skip):

You: skip

Bot: ⚠️ Confirm Action
     React with ✔️ to confirm or 🚫 to cancel

You: [React with ✔️]

Bot: ✅ Video Logged Successfully
     Creator: JohnDoe
     Amount: $50.00
     Date: January 23, 2026
```

### Duplicate Detection

```
You: !submit https://www.tiktok.com/@username/video/7123456789

Bot: ❌ Duplicate Payment Detected
     This video was already paid for!
     Creator: JohnDoe
     Date: January 23, 2026
     Amount: $50.00
```

## Database

The bot uses SQLite for data storage. The database file `creator_payments.db` is created automatically in the bot's directory.

### Database Schema

```sql
CREATE TABLE videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    creator_name TEXT NOT NULL,
    payment_amount REAL NOT NULL,
    currency TEXT DEFAULT 'USD',
    date_submitted TEXT NOT NULL,
    notes TEXT
);
```

### Backup

To backup your data, simply copy the `creator_payments.db` file to a safe location.

## Deployment (24/7 Running)

### Option 1: Linux VPS with systemd

1. Copy the bot files to your server
2. Create a systemd service file:

```bash
sudo nano /etc/systemd/system/payment-bot.service
```

```ini
[Unit]
Description=TikTok Payment Tracker Discord Bot
After=network.target

[Service]
Type=simple
User=youruser
WorkingDirectory=/path/to/bot
ExecStart=/path/to/bot/venv/bin/python bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

3. Enable and start the service:

```bash
sudo systemctl enable payment-bot
sudo systemctl start payment-bot
sudo systemctl status payment-bot
```

4. View logs:

```bash
sudo journalctl -u payment-bot -f
```

### Option 2: Docker

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "bot.py"]
```

Build and run:

```bash
docker build -t payment-bot .
docker run -d --name payment-bot --restart unless-stopped \
  -v $(pwd)/creator_payments.db:/app/creator_payments.db \
  --env-file .env \
  payment-bot
```

### Option 3: PM2 (Node.js process manager)

```bash
# Install PM2
npm install -g pm2

# Start the bot
pm2 start bot.py --interpreter python3

# Save the process list
pm2 save

# Setup startup script
pm2 startup
```

### Option 4: Screen/tmux

```bash
# Using screen
screen -S payment-bot
python bot.py
# Press Ctrl+A, then D to detach

# Reattach later
screen -r payment-bot
```

## File Structure

```
discord-payment-bot/
├── bot.py              # Main bot code with all commands
├── database.py         # SQLite database operations
├── utils.py            # URL parsing and helper functions
├── requirements.txt    # Python dependencies
├── .env.example        # Environment template
├── .env                # Your configuration (not in git)
├── README.md           # This file
├── bot.log             # Log file (created on first run)
└── creator_payments.db # SQLite database (created on first run)
```

## Troubleshooting

### Bot doesn't respond to commands

- Ensure "Message Content Intent" is enabled in the Discord Developer Portal
- Check that the bot has "Read Message History" and "Send Messages" permissions in the channel

### "DISCORD_BOT_TOKEN not found"

- Make sure you created a `.env` file (not just `.env.example`)
- Verify the token is correct (no extra spaces or quotes around it)

### Short URLs not resolving

- The bot makes HTTP requests to resolve short URLs
- If you're behind a firewall, ensure outbound HTTPS traffic is allowed
- Short URLs will still be accepted with a shortcode-based ID as fallback

### Permission errors

- The bot needs permission to:
  - Read and send messages
  - Add reactions
  - Embed links
  - Attach files (for CSV export)

## License

MIT License - feel free to modify and use as needed.
