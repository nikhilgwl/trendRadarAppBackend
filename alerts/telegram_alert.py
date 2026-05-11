import os
import asyncio
from telegram import Bot
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

async def send_telegram_alert(opportunity, image_path=None):
    """
    Send a formatted Telegram message for a trend opportunity, optionally with an image.
    Supports both individual trends and custom summary messages.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        print("Telegram credentials missing.")
        return

    bot = Bot(token=token)
    
    # Check if a pre-formatted message is provided
    if opportunity.get("custom_message"):
        message = opportunity.get("custom_message")
        trend_name = opportunity.get("trend_name", "Summary Report")
    else:
        trend_name = opportunity.get("trend_name", "Unknown Trend")
        urgency = opportunity.get("urgency_score", opportunity.get("relevance_score", 0))
        category = opportunity.get("category", "N/A")
        summary = opportunity.get("summary", opportunity.get("why_it_works_for_india", "N/A"))
        
        priority_prefix = "🚨 HIGH PRIORITY\n" if urgency >= 8 else ""
        
        message = (
            f"{priority_prefix}🔥 *TREND ALERT: {trend_name}*\n"
            f"📂 *Category:* {category}\n"
            f"⚡ *Relevance Score:* {urgency}/10\n\n"
            f"📖 *Summary:*\n{summary}\n\n"
            f"⏰ Detected at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} IST"
        )

    try:
        if image_path and os.path.exists(image_path):
            with open(image_path, "rb") as photo:
                await bot.send_photo(chat_id=chat_id, photo=photo, caption=message, parse_mode='Markdown')
        else:
            await bot.send_message(chat_id=chat_id, text=message, parse_mode='Markdown')
        print(f"Alert sent for: {trend_name}")
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")

if __name__ == "__main__":
    # Test alert
    test_opp = {
        "trend_name": "Test Trend",
        "relevance_score": 9,
        "category": "Skin Care",
        "summary": "This is a test trend for debugging."
    }
    # asyncio.run(send_telegram_alert(test_opp))
