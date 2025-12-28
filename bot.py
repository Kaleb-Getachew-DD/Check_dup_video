from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    CommandHandler,
    ContextTypes,
    filters
)
import sqlite3
from datetime import datetime
import asyncio
import logging
import sys
from typing import Optional
from contextlib import contextmanager
from flask import Flask, request
import threading

# === Configuration ===
import os
TOKEN = os.getenv("BOT_TOKEN")
MAX_REPORT_VIDEOS = 10  # Maximum videos to show in report
RATE_LIMIT_DELAY = 0.5  # Delay between sending videos
REPORT_COOLDOWN = 30  # Minimum seconds between /report commands per chat
DELETE_COOLDOWN = 60  # Minimum seconds between /delete_duplicates commands per chat

# === Webhook Configuration ===
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # Set this in Render environment variables
PORT = int(os.getenv("PORT", 5000))

# === Rate limiting storage ===
last_report_usage = {}
last_delete_usage = {}

# === Flask App for Webhooks ===
app = Flask(__name__)

# === Logging setup ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# === Database context manager ===
@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    try:
        conn = sqlite3.connect("videos.db", check_same_thread=False)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# === Database setup ===
def initialize_database():
    """Initialize database tables and indexes"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        # Create videos table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            file_unique_id TEXT,
            file_id TEXT,
            count INTEGER,
            first_seen TEXT,
            chat_id INTEGER,
            message_id INTEGER,
            PRIMARY KEY(file_unique_id, message_id)
        )
        """)

        # Create report_messages table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS report_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            message_id INTEGER,
            report_type TEXT,
            created_at TEXT
        )
        """)

        # Add indexes for better performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_unique_id ON videos(file_unique_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_videos_chat_id ON videos(chat_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_messages_chat_id ON report_messages(chat_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_report_messages_type ON report_messages(report_type)")

        conn.commit()
        logger.info("Database initialized successfully")

# Initialize database on startup
initialize_database()

# === Video handler: store message info + count ===
async def video_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message or not update.message.video:
            logger.warning("Received update without video")
            return

        video = update.message.video
        file_id = video.file_id
        unique_id = video.file_unique_id
        chat_id = update.effective_chat.id
        message_id = update.message.message_id

        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Insert this message if not exist
            cursor.execute(
                "INSERT OR IGNORE INTO videos VALUES (?, ?, ?, ?, ?, ?)",
                (unique_id, file_id, 0, datetime.now().isoformat(), chat_id, message_id)
            )

            # Count total appearances
            cursor.execute(
                "SELECT COUNT(*) FROM videos WHERE file_unique_id = ?",
                (unique_id,)
            )
            result = cursor.fetchone()
            if result:
                total_for_file = result[0]
            else:
                total_for_file = 1

            cursor.execute(
                "UPDATE videos SET count = ? WHERE file_unique_id = ?",
                (total_for_file, unique_id)
            )

            conn.commit()

        logger.info(f"Processed video {unique_id} in chat {chat_id}, now appears {total_for_file} times")

    except Exception as e:
        logger.error(f"Error processing video message: {e}")
        # Don't re-raise to prevent bot crashes

# === /report command: shows duplicates with video links ===
async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id
        current_time = datetime.now().timestamp()

        # Rate limiting check
        if chat_id in last_report_usage:
            time_diff = current_time - last_report_usage[chat_id]
            if time_diff < REPORT_COOLDOWN:
                remaining = int(REPORT_COOLDOWN - time_diff)
                await update.message.reply_text(f"⏰ Please wait {remaining} seconds before using /report again.")
                return

        last_report_usage[chat_id] = current_time
        logger.info(f"Report command called in chat {chat_id}")

        # How many unique repeated file_unique_id
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT file_unique_id, file_id, COUNT(*) AS total_count
                FROM videos
                GROUP BY file_unique_id
                HAVING total_count > 1
                ORDER BY total_count DESC
            """)
            repeated = cursor.fetchall()

        if not repeated:
            await update.message.reply_text("No repeated videos found yet.")
            return

        report_msg = await update.message.reply_text(f"📊 Duplicate Video Report\nTotal repeated sets: {len(repeated)}")

        # Send each repeated video (first MAX_REPORT_VIDEOS)
        videos_sent = 0
        messages_to_store = [(chat_id, report_msg.message_id, "header", datetime.now().isoformat())]

        for i, (unique_id, file_id, total_count) in enumerate(repeated[:MAX_REPORT_VIDEOS], start=1):
            try:
                sent_msg = await context.bot.send_video(
                    chat_id=chat_id,
                    video=file_id,
                    caption=f"{i}. Repeated {total_count} times"
                )
                # Store the video message for cleanup
                messages_to_store.append((chat_id, sent_msg.message_id, "video", datetime.now().isoformat()))
                videos_sent += 1
                await asyncio.sleep(RATE_LIMIT_DELAY)  # slight delay to prevent timeouts
            except Exception as e:
                logger.warning(f"Could not send video {i} ({unique_id}): {e}")
                try:
                    error_msg = await update.message.reply_text(f"⚠️ Could not send video {i} (skipped).")
                    # Store error message for cleanup
                    messages_to_store.append((chat_id, error_msg.message_id, "error", datetime.now().isoformat()))
                except Exception as inner_e:
                    logger.error(f"Could not send error message: {inner_e}")

        # Store all messages in database
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(
                "INSERT INTO report_messages (chat_id, message_id, report_type, created_at) VALUES (?, ?, ?, ?)",
                messages_to_store
            )
            conn.commit()
        logger.info(f"Report completed: sent {videos_sent} videos in chat {chat_id}")

    except Exception as e:
        logger.error(f"Error in report command: {e}")
        try:
            await update.message.reply_text("❌ An error occurred while generating the report.")
        except:
            pass

# === /delete_duplicates command: only delete duplicates (keep first instance) ===
async def delete_duplicates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat = update.effective_chat
        user = update.effective_user
        chat_id = update.effective_chat.id
        current_time = datetime.now().timestamp()

        # Rate limiting check
        if chat_id in last_delete_usage:
            time_diff = current_time - last_delete_usage[chat_id]
            if time_diff < DELETE_COOLDOWN:
                remaining = int(DELETE_COOLDOWN - time_diff)
                await update.message.reply_text(f"⏰ Please wait {remaining} seconds before using /delete_duplicates again.")
                return

        last_delete_usage[chat_id] = current_time

        # Check admin status
        try:
            member = await chat.get_member(user.id)
            if member.status not in ["administrator", "creator"]:
                await update.message.reply_text("❌ Only admins can run this command")
                return
        except Exception as e:
            logger.error(f"Could not check admin status: {e}")
            await update.message.reply_text("❌ Could not verify admin status")
            return

        chat_id = update.effective_chat.id
        total_deleted = 0
        logger.info(f"Delete duplicates command called by admin {user.id} in chat {chat_id}")

        # Get report messages to delete
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT message_id FROM report_messages WHERE chat_id = ?", (chat_id,))
            report_messages = cursor.fetchall()

        report_deleted = 0
        for (message_id,) in report_messages:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                total_deleted += 1
                report_deleted += 1
            except Exception as e:
                logger.warning(f"Could not delete report message {message_id}: {e}")

        # Clear report messages from database
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM report_messages WHERE chat_id = ?", (chat_id,))
            conn.commit()

        # Find all duplicates (keep first message_id per unique video)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT file_unique_id, message_id, chat_id
                FROM videos
                WHERE file_unique_id IN (
                    SELECT file_unique_id
                    FROM videos
                    GROUP BY file_unique_id
                    HAVING COUNT(*) > 1
                )
                ORDER BY file_unique_id, message_id
            """)
            rows = cursor.fetchall()

        duplicates_deleted = 0
        videos_to_delete = []
        last_unique = None
        for unique_id, message_id, video_chat_id in rows:
            # Skip first occurrence
            if unique_id != last_unique:
                last_unique = unique_id
                continue
            try:
                await context.bot.delete_message(chat_id=video_chat_id, message_id=message_id)
                total_deleted += 1
                duplicates_deleted += 1
                videos_to_delete.append((unique_id, message_id))
            except Exception as e:
                logger.warning(f"Could not delete duplicate message {message_id}: {e}")

        # Remove deleted videos from database
        if videos_to_delete:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.executemany(
                    "DELETE FROM videos WHERE file_unique_id = ? AND message_id = ?",
                    videos_to_delete
                )
                conn.commit()
        logger.info(f"Delete duplicates completed: deleted {total_deleted} total messages "
                   f"({report_deleted} reports, {duplicates_deleted} duplicates) in chat {chat_id}")
        await update.message.reply_text(f"✅ Deleted {total_deleted} messages "
                                      f"({report_deleted} reports, {duplicates_deleted} duplicates).")

    except Exception as e:
        logger.error(f"Error in delete_duplicates command: {e}")
        try:
            await update.message.reply_text("❌ An error occurred while deleting duplicates.")
        except:
            pass

# === /stats command: show bot statistics ===
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = update.effective_chat.id

        with get_db_connection() as conn:
            cursor = conn.cursor()

            # Get total videos processed
            cursor.execute("SELECT COUNT(*) FROM videos")
            total_videos = cursor.fetchone()[0]

            # Get unique videos
            cursor.execute("SELECT COUNT(DISTINCT file_unique_id) FROM videos")
            unique_videos = cursor.fetchone()[0]

            # Get duplicate sets
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT file_unique_id
                    FROM videos
                    GROUP BY file_unique_id
                    HAVING COUNT(*) > 1
                )
            """)
            duplicate_sets = cursor.fetchone()[0]

            # Get total duplicates
            cursor.execute("""
                SELECT SUM(COUNT(*) - 1) FROM videos
                GROUP BY file_unique_id
                HAVING COUNT(*) > 1
            """)
            result = cursor.fetchone()
            total_duplicates = result[0] if result and result[0] else 0

            # Get report messages count
            cursor.execute("SELECT COUNT(*) FROM report_messages WHERE chat_id = ?", (chat_id,))
            report_messages = cursor.fetchone()[0]

            # Get oldest video date
            cursor.execute("SELECT MIN(first_seen) FROM videos")
            oldest_result = cursor.fetchone()
            oldest_date = oldest_result[0] if oldest_result and oldest_result[0] else "N/A"

        stats_text = f"""📈 **Bot Statistics**

**Videos Processed:** {total_videos:,}
**Unique Videos:** {unique_videos:,}
**Duplicate Sets:** {duplicate_sets:,}
**Total Duplicates:** {total_duplicates:,}
**Report Messages:** {report_messages}
**First Video:** {oldest_date}

**Commands:**
• /report - Show duplicate videos
• /delete_duplicates - Remove duplicates (admin only)
• /stats - Show this statistics"""

        await update.message.reply_text(stats_text, parse_mode='Markdown')
        logger.info(f"Stats command used in chat {chat_id}")

    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await update.message.reply_text("❌ Could not retrieve statistics.")

# === Webhook Handler ===
@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming Telegram updates via webhook"""
    if request.method == "POST":
        update = Update.de_json(request.get_json(force=True), application.bot)
        if update:
            # Process update in a separate thread to avoid blocking
            threading.Thread(target=lambda: asyncio.run(application.process_update(update))).start()
        return 'OK', 200
    return 'Method not allowed', 405

# === Global application instance ===
application = None

async def setup_webhook():
    """Set up webhook for the bot"""
    global application
    application = ApplicationBuilder().token(TOKEN).build()

    # Handlers
    application.add_handler(MessageHandler(filters.VIDEO, video_handler))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("delete_duplicates", delete_duplicates))
    application.add_handler(CommandHandler("stats", stats_command))

    if WEBHOOK_URL:
        # Set webhook
        await application.bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
        logger.info(f"Webhook set to: {WEBHOOK_URL}/webhook")
    else:
        logger.warning("WEBHOOK_URL not set, running in polling mode")
        await application.run_polling()

# === Main function ===
def main():
    global application

    if not TOKEN:
        logger.error("BOT_TOKEN environment variable not set!")
        return

    # Initialize database
    initialize_database()

    if WEBHOOK_URL:
        # Webhook mode for production
        print("🤖 Setting up webhook...")
        asyncio.run(setup_webhook())

        # Run Flask app
        print(f"🌐 Starting Flask server on port {PORT}...")
        app.run(host='0.0.0.0', port=PORT)
    else:
        # Polling mode for local development
        print("🤖 Running in polling mode (local development)...")
        asyncio.run(setup_webhook())

if __name__ == "__main__":
    main()
