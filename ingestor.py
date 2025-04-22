import asyncio
import json
import os
import logging
import base64
import csv
import time
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession # Default session
from telethon.tl.patched import Message # Import Message type hint
from aiohttp import ClientSession

import logging # Ensure logging is imported if not already at the very top

# --- Early Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("Logging configured.") # Confirm logging is active

# Load environment variables from .env file if it exists
logger.info("Loading environment variables from .env...")
# Explicitly find .env in the current directory and force override
from dotenv import find_dotenv
load_dotenv(find_dotenv(usecwd=True), override=True)
logger.info("Environment variables loaded.")

# --- Configuration ---
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE_NUMBER = os.getenv('TELEGRAM_PHONE') # Required for initial login if no string session
SESSION_STRING = os.getenv('TELEGRAM_SESSION_STRING') # Prioritized session storage for deployment
SESSION_NAME = os.getenv('TELEGRAM_SESSION_NAME', "telegram_ingestor_session") # Fallback file session name (mainly for local use/setup)
CHANNEL_USERNAME = os.getenv('TELEGRAM_CHANNEL') # Username/ID of the target channel/chat
# Optional external Model API
MODEL_API_URL = os.getenv('MODEL_API_URL')
MODEL_API_KEY = os.getenv('MODEL_API_KEY')
MODEL_SYSTEM_PROMPT = os.getenv('MODEL_SYSTEM_PROMPT', "You are an AI assistant processing Telegram messages.")
# Output CSV file
OUTPUT_CSV_FILE = os.getenv('OUTPUT_CSV_FILE', 'output_log.csv')
# Target chat for results
RESULT_TARGET_CHAT = os.getenv('RESULT_TARGET_CHAT') # e.g., @my_username or chat_id

# --- Validate Required Configuration ---
REQUIRED_CONFIG = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_PHONE', 'TELEGRAM_CHANNEL']
missing_config = []
if not API_ID: missing_config.append('TELEGRAM_API_ID')
if not API_HASH: missing_config.append('TELEGRAM_API_HASH')
if not PHONE_NUMBER: missing_config.append('TELEGRAM_PHONE')
if not CHANNEL_USERNAME: missing_config.append('TELEGRAM_CHANNEL')

if missing_config:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_config)}")

# --- Log loaded phone number for debugging ---
# Removed the PHONE_NUMBER debug log line as it was causing confusion

# --- CSV Setup ---
CSV_HEADER = ['timestamp', 'message_id', 'sender_id', 'message_text', 'action', 'confidence', 'reason', 'raw_model_response'] # Added structured fields
# Create CSV file and write header if it doesn't exist
if not os.path.exists(OUTPUT_CSV_FILE):
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
    logger.info(f"Created output CSV file: {OUTPUT_CSV_FILE}")

# --- Telethon Client ---
# Prioritize StringSession for deployment, fall back to file session for local use/setup
# Always use StringSession. Initialize with env var if present, otherwise None.
logger.info("Initializing client with StringSession.")
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
# Note: The file session (SESSION_NAME) is no longer used by the client directly.

async def process_with_model(message_data):
    """
    Processes the message text using an external model API if configured.
    Takes the deserialized message data.
    Returns the model's response text or an error indicator.
    """
    message_id = message_data.get('id', 'N/A')
    message_text = message_data.get('message', '') # Extract the text content

    if not message_text:
        logger.warning(f"Message ID {message_id} has no text content to process.")
        return "no_text_content"

    logger.info(f"Processing message ID: {message_id} with model.")

    if MODEL_API_URL:
        logger.debug(f"Using external model API: {MODEL_API_URL}")
        headers = {"Content-Type": "application/json"}
        if MODEL_API_KEY:
            headers['Authorization'] = f"Bearer {MODEL_API_KEY}"

        payload = {
            "model": "gpt-3.5-turbo", # Consider making model name configurable
            "messages": [
                {"role": "system", "content": MODEL_SYSTEM_PROMPT},
                {"role": "user", "content": message_text}
            ]
        }

        try:
            async with ClientSession() as session:
                async with session.post(MODEL_API_URL, json=payload, headers=headers, timeout=30) as response:
                    response.raise_for_status()
                    result = await response.json()
                    model_response = result.get('choices', [{}])[0].get('message', {}).get('content', 'api_no_response')
                    logger.info(f"Message ID: {message_id} - Model Response Received.")
                    logger.debug(f"Message ID: {message_id} - Response: {model_response[:100]}...")
                    return model_response
        except Exception as e:
            logger.error(f"Error calling model API for message ID {message_id}: {e}")
            return "model_api_error"
    else:
        logger.info(f"No MODEL_API_URL configured. Skipping model processing for message ID: {message_id}.")
        return "no_model_configured"

def append_to_csv(data_row):
    """Appends a row to the CSV file."""
    try:
        with open(OUTPUT_CSV_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(data_row)
    except Exception as e:
        logger.error(f"Error writing to CSV file {OUTPUT_CSV_FILE}: {e}")

async def process_and_log_message(message: Message):
    """Processes a single message object, calls model, parses JSON, and logs structured data to CSV."""
    if not message:
        logger.warning("Attempted to process an invalid message object.")
        return

    message_id = message.id
    sender_id = message.sender_id
    process_timestamp = datetime.now().isoformat() # Timestamp when processing starts
    message_timestamp = message.date # Telegram message timestamp (datetime object, UTC)
    logger.info(f"Processing message ID: {message_id}")

    action = "ERROR"
    confidence = 0.0
    reason = "Processing error before model call"
    model_response_raw = ""
    message_text = "" # Initialize message_text

    try:
        message_data = message.to_dict()
        message_text = message_data.get('message', '')

        # --- Check if message is too old ---
        current_time_utc = datetime.now(message_timestamp.tzinfo) # Use same timezone awareness
        time_difference = current_time_utc - message_timestamp
        if time_difference.total_seconds() > 300: # 5 minutes * 60 seconds
            logger.warning(f"Message ID {message_id} is older than 5 minutes ({time_difference}). Skipping model processing.")
            action = "NONE"
            confidence = 0.0
            reason = "stale_or_irrelevant"
            model_response_raw = json.dumps({"action": action, "confidence": confidence, "reason": reason})
        elif not message_text:
             logger.warning(f"Message ID {message_id} has no text content. Skipping model processing.")
             action = "NONE"
             confidence = 0.0
             reason = "no_text_content"
             model_response_raw = json.dumps({"action": action, "confidence": confidence, "reason": reason})
        else:
            # Process with model (if configured)
            model_response_raw = await process_with_model(message_data)

            # --- Parse Model Response ---
            try:
                # Attempt to parse the raw response as JSON
                if isinstance(model_response_raw, str) and model_response_raw.startswith('{'):
                     model_response_json = json.loads(model_response_raw)
                     action = model_response_json.get('action', 'MISSING_ACTION')
                     confidence = float(model_response_json.get('confidence', 0.0)) # Ensure float conversion
                     reason = model_response_json.get('reason', 'MISSING_REASON')

                     # Basic validation based on prompt rules
                     if action not in ["LONG", "SHORT", "NONE"]:
                         logger.warning(f"Invalid action '{action}' received from model for message {message_id}. Defaulting to NONE.")
                         reason = f"Invalid action received: {action}"
                         action = "NONE"
                         confidence = 0.0 # Reset confidence for invalid action
                     elif confidence < 0.6 and action != "NONE":
                         logger.info(f"Confidence {confidence:.2f} < 0.6 for action '{action}' (message {message_id}). Overriding to NONE.")
                         reason = f"Confidence below threshold ({confidence:.2f})"
                         action = "NONE"
                         # Keep original confidence from model in raw response, but log 0? Or log original? Let's log original for now.

                elif model_response_raw == "no_model_configured":
                     action = "NONE"
                     confidence = 0.0
                     reason = "no_model_configured"
                     model_response_raw = json.dumps({"action": action, "confidence": confidence, "reason": reason})
                elif model_response_raw == "model_api_error":
                     action = "ERROR"
                     confidence = 0.0
                     reason = "model_api_error"
                     model_response_raw = json.dumps({"action": action, "confidence": confidence, "reason": reason})
                else: # Handle cases where model didn't return valid JSON string
                     logger.warning(f"Model for message {message_id} did not return valid JSON: {model_response_raw}")
                     action = "ERROR"
                     confidence = 0.0
                     reason = "invalid_model_response_format"
                     # Keep raw response as is

            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to parse model JSON response for message {message_id}: {json_err}")
                action = "ERROR"
                confidence = 0.0
                reason = f"JSONDecodeError: {json_err}"
                model_response_raw = f"INVALID_JSON: {model_response_raw}" # Log the invalid JSON
            except ValueError as val_err:
                 logger.error(f"Failed to parse confidence as float for message {message_id}: {val_err}")
                 action = "ERROR"
                 confidence = 0.0 # Default confidence on error
                 reason = f"ValueError: {val_err}"
            except Exception as parse_err:
                logger.error(f"Unexpected error parsing model response for message {message_id}: {parse_err}", exc_info=True)
                action = "ERROR"
                confidence = 0.0
                reason = f"Unexpected Parse Error: {parse_err}"

    except Exception as e:
        logger.error(f"Error processing message ID {message_id}: {e}", exc_info=True)
        action = "ERROR"
        confidence = 0.0
        reason = f"PROCESSING_ERROR: {e}"
        model_response_raw = "" # No model response available

    # Log to CSV - now logging structured fields
    # Ensure confidence is formatted reasonably for CSV
    formatted_confidence = f"{confidence:.2f}" if isinstance(confidence, (float, int)) else "N/A"
    csv_row = [process_timestamp, message_id, sender_id, message_text, action, formatted_confidence, reason, model_response_raw]
    append_to_csv(csv_row)
    logger.info(f"Logged message ID {message_id} to CSV with action: {action}")

    # --- Send result message to target chat ---
    if RESULT_TARGET_CHAT:
        try:
            # Construct the message content (using the raw JSON string is simplest)
            # Or format it nicely: f"Msg {message_id}: {action} ({confidence:.2f}) - {reason}"
            result_message_content = model_response_raw if model_response_raw else json.dumps({"action": action, "confidence": confidence, "reason": reason})

            await client.send_message(RESULT_TARGET_CHAT, result_message_content)
            logger.info(f"Sent result for message ID {message_id} to {RESULT_TARGET_CHAT}")
        except ValueError:
             logger.error(f"Invalid RESULT_TARGET_CHAT: '{RESULT_TARGET_CHAT}'. Could not send message.")
        except Exception as send_err:
            logger.error(f"Failed to send result message for ID {message_id} to {RESULT_TARGET_CHAT}: {send_err}")


@client.on(events.NewMessage(chats=CHANNEL_USERNAME))
async def handle_new_message(event):
    """Handles incoming messages by calling the processing function."""
    logger.info(f"New message event received: ID {event.message.id}")
    await process_and_log_message(event.message)


async def main():
    """Main function to connect the client and run."""
    logger.info("Starting Simplified Telegram Ingestor...")

    logger.info(f"Initializing Telegram client for channel: {CHANNEL_USERNAME}")
    try:
        logger.info("Starting Telegram client...")
        # Attempt to connect/login. If session is empty/invalid, it will prompt for phone/code.
        await client.start(phone=PHONE_NUMBER)
        logger.info("Telegram client started successfully.")

        # --- Print session string AFTER successful start/login ---
        # This works because client.session is now guaranteed to be a StringSession
        try:
            session_str = client.session.save()
            if session_str: # Check if string is not None/empty
                 logger.info("="*50)
                 logger.info("IMPORTANT: Copy the following session string and set it as the")
                 logger.info("TELEGRAM_SESSION_STRING environment variable for deployment:")
                 print(f"\nTELEGRAM_SESSION_STRING:\n{session_str}\n") # Print clearly to console
                 logger.info("="*50)
                 logger.info("You can now stop this script (Ctrl+C) if you only needed the string.")
            else:
                 logger.error("Could not retrieve a valid session string after login.")
        except Exception as e_session:
            logger.error(f"Error saving/printing session string: {e_session}")
        # --- End session string printing ---

    except Exception as e:
        logger.error(f"Failed to start Telegram client: {e}", exc_info=True)
        return

    # Check authorization again, especially after file-based login attempt
    if not await client.is_user_authorized():
        logger.error("Client is not authorized. Please ensure login was successful or session string is valid.")
        if client.is_connected():
             await client.disconnect()
        return

    try:
        # Verify channel access without fetching messages
        await client.get_entity(CHANNEL_USERNAME)
        logger.info(f"Verified access to channel: {CHANNEL_USERNAME}")

    except ValueError:
        logger.error(f"Channel username '{CHANNEL_USERNAME}' not found or invalid.")
        await client.disconnect()
        return
    except Exception as e:
        logger.error(f"Error accessing channel: {e}", exc_info=True)
        await client.disconnect()
        return

    logger.info(f"Listening for new messages... Output will continue to be logged to {OUTPUT_CSV_FILE}")
    await client.run_until_disconnected()


if __name__ == '__main__':
    main_task = None
    # Use asyncio.run() for cleaner startup/shutdown if Python >= 3.7
    # If older Python, the get_event_loop approach is necessary.
    # Assuming Python 3.7+ for simplicity here.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Service stopped manually.")
        # asyncio.run() handles task cancellation and loop closing on KeyboardInterrupt
    except Exception as e:
        logger.error(f"Unhandled exception in main execution: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        # Disconnection should ideally happen within main() or via signal handlers
        # but we add a final check here.
        # Note: Accessing 'client' here might be problematic if main() failed early.
        # A more robust solution involves signal handlers or context managers.
        try:
            if client and client.is_connected():
                logger.info("Attempting final disconnection...")
                # Need a running loop to disconnect
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running() and not loop.is_closed():
                         loop.run_until_complete(client.disconnect())
                         logger.info("Final disconnection successful.")
                    else:
                         # Fallback if loop isn't usable
                         asyncio.run(client.disconnect())
                         logger.info("Final disconnection successful (using new loop).")
                except RuntimeError as e:
                     logger.warning(f"Could not get running loop for final disconnect: {e}")
                except Exception as e:
                     logger.error(f"Error during final client disconnection: {e}")
        except NameError:
             logger.info("Client object likely not initialized, skipping final disconnect check.")

        logger.info("Simplified Telegram Ingestor shut down complete.")