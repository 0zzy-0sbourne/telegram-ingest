import asyncio
import json
import os
import logging
import time
import threading
from dotenv import load_dotenv
import redis.asyncio as redis
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from aiohttp import web, ClientSession

# Load environment variables from .env file if it exists
load_dotenv()

# --- Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_STREAM_NAME = os.getenv('REDIS_STREAM', 'telegram_messages')
REDIS_CONSUMER_GROUP = os.getenv('REDIS_CONSUMER_GROUP', 'ai_worker_group')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD') # Added for completeness
# Generate a unique consumer name per instance or use a fixed one if only one worker runs
REDIS_CONSUMER_NAME = os.getenv('REDIS_CONSUMER_NAME', f'worker_instance_{os.getpid()}')
METRICS_PORT = int(os.getenv('AI_WORKER_METRICS_PORT', 8001))
GDPR_API_PORT = int(os.getenv('GDPR_API_PORT', 9090))
# Optional external Model API
MODEL_API_URL = os.getenv('MODEL_API_URL')
MODEL_API_KEY = os.getenv('MODEL_API_KEY')
MODEL_SYSTEM_PROMPT = os.getenv('MODEL_SYSTEM_PROMPT', "You are an AI assistant processing Telegram messages.")
# Results stream
RESULTS_REDIS_STREAM = os.getenv('RESULTS_REDIS_STREAM', 'telegram_model_results')


# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Prometheus Metrics ---
MESSAGES_PROCESSED = Counter('ai_worker_messages_processed_total', 'Total messages processed by the AI worker', ['group', 'consumer'])
MESSAGES_ACKNOWLEDGED = Counter('ai_worker_messages_acknowledged_total', 'Total messages acknowledged in Redis Stream', ['group', 'consumer'])
PROCESSING_ERRORS = Counter('ai_worker_processing_errors_total', 'Total errors encountered during message processing', ['group', 'consumer', 'error_type'])
REDIS_OPERATIONS_ERRORS = Counter('ai_worker_redis_errors_total', 'Total errors during Redis operations (read, ack, etc.)', ['group', 'consumer', 'operation'])
MESSAGE_PROCESSING_TIME = Histogram('ai_worker_message_processing_duration_seconds', 'Histogram of message processing time', ['group', 'consumer'])
MODEL_API_CALLS = Counter('ai_worker_model_api_calls_total', 'Total calls made to the external model API', ['group', 'consumer', 'status']) # status: success, failure
RESULTS_PUSHED = Counter('ai_worker_results_pushed_total', 'Total results pushed to the results Redis stream', ['group', 'consumer'])

# --- Redis Connection ---
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

async def process_with_model(message_data, consumer_label):
    """
    Processes the message text using an external model API if configured.
    Takes the deserialized message data and consumer labels for metrics.
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

        # Construct payload (example for OpenAI compatible API)
        payload = {
            "model": "gpt-3.5-turbo", # Or your desired model - consider making this configurable too
            "messages": [
                {"role": "system", "content": MODEL_SYSTEM_PROMPT},
                {"role": "user", "content": message_text}
            ]
        }

        try:
            async with ClientSession() as session:
                async with session.post(MODEL_API_URL, json=payload, headers=headers, timeout=30) as response: # Increased timeout for models
                    response.raise_for_status()
                    result = await response.json()

                    # --- Adapt based on actual API response structure ---
                    # Example for OpenAI format:
                    model_response = result.get('choices', [{}])[0].get('message', {}).get('content', 'api_no_response')
                    # --- End Adapt ---

                    logger.info(f"Message ID: {message_id} - Model Response Received.")
                    logger.debug(f"Message ID: {message_id} - Response: {model_response[:100]}...") # Log snippet
                    MODEL_API_CALLS.labels(**consumer_label, status='success').inc()
                    return model_response
        except Exception as e:
            logger.error(f"Error calling model API for message ID {message_id}: {e}")
            MODEL_API_CALLS.labels(**consumer_label, status='failure').inc()
            return "model_api_error"
    else:
        # Placeholder logic if no API URL is configured
        logger.info(f"No MODEL_API_URL configured. Skipping model processing for message ID: {message_id}.")
        return "no_model_configured"

async def process_stream_messages():
    """Continuously reads and processes messages from the Redis stream."""
    consumer_label = {'group': REDIS_CONSUMER_GROUP, 'consumer': REDIS_CONSUMER_NAME}
    logger.info(f"AI Worker starting. Consumer: {REDIS_CONSUMER_NAME}, Group: {REDIS_CONSUMER_GROUP}, Stream: {REDIS_STREAM_NAME}")

    # Ensure the consumer group exists
    try:
        await redis_client.xgroup_create(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, id='0', mkstream=True)
        logger.info(f"Consumer group '{REDIS_CONSUMER_GROUP}' created or already exists for stream '{REDIS_STREAM_NAME}'.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" in str(e):
            logger.info(f"Consumer group '{REDIS_CONSUMER_GROUP}' already exists for stream '{REDIS_STREAM_NAME}'.")
        else:
            REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xgroup_create').inc()
            logger.error(f"Error creating/checking consumer group '{REDIS_CONSUMER_GROUP}': {e}")
            return # Stop if we can't ensure the group exists
    except redis.exceptions.ConnectionError as e:
        REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xgroup_create').inc()
        logger.error(f"Redis connection error during group creation: {e}")
        return
    except Exception as e: # Catch any other unexpected error during group creation
        REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xgroup_create').inc()
        logger.error(f"Unexpected error during group creation: {e}")
        return

    last_processed_id = '0' # Start reading from the beginning for this consumer initially

    while True:
        try:
            # Read new messages using XREADGROUP
            response = None
            try:
                response = await redis_client.xreadgroup(
                    groupname=REDIS_CONSUMER_GROUP,
                    consumername=REDIS_CONSUMER_NAME,
                    streams={REDIS_STREAM_NAME: '>'}, # Read new messages for this consumer
                    count=1,                           # Process one message at a time
                    block=5000                         # Block for 5 seconds, then loop (allows checking connection)
                )
            except redis.exceptions.ConnectionError as e:
                REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xreadgroup').inc()
                logger.error(f"Redis connection error during xreadgroup: {e}. Retrying...")
                await asyncio.sleep(5)
                continue # Retry reading
            except Exception as e:
                REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xreadgroup').inc()
                logger.error(f"Unexpected error during xreadgroup: {e}. Retrying...")
                await asyncio.sleep(5)
                continue # Retry reading

            if not response:
                # Timed out (block=5000ms), loop again to check connection or stop signal
                # Should not happen with block=0, but good practice
                continue

            # response format: [['stream_name', [('message_id', {'field': 'value'})]]]
            stream_name, messages = response[0]
            stream_name, messages = response[0]
            message_id, message_fields = messages[0]
            start_time = time.time()

            logger.info(f"Processing message ID: {message_id} from stream: {stream_name}")
            MESSAGES_PROCESSED.labels(**consumer_label).inc()

            ack_message = True # Assume we will acknowledge unless a critical error occurs
            try:
                message_json = message_fields.get('message')
                if not message_json:
                    logger.warning(f"Message ID {message_id} has no 'message' field.")
                    PROCESSING_ERRORS.labels(**consumer_label, error_type='missing_field').inc()
                    # Acknowledge malformed entry to prevent reprocessing loop
                    ack_message = True
                    raise ValueError("Missing 'message' field") # Skip further processing

                # --- Processing Steps ---
                message_data = json.loads(message_json)

                # 1. Process with Model
                model_response = await process_with_model(message_data, consumer_label)

                # 2. Push results to results stream
                result_data = {
                    'original_message_id': message_id,
                    'stream_name': stream_name,
                    'message_text': message_data.get('message', ''), # Include original text for context
                    'model_response': model_response,
                    'processed_at': time.time() # Timestamp of processing completion
                }
                try:
                    # Ensure all values are strings for xadd
                    stringified_result_data = {k: str(v) for k, v in result_data.items()}
                    await redis_client.xadd(RESULTS_REDIS_STREAM, stringified_result_data)
                    RESULTS_PUSHED.labels(**consumer_label).inc()
                    logger.info(f"Pushed model response for message ID {message_id} to stream '{RESULTS_REDIS_STREAM}'")
                except redis.exceptions.ConnectionError as e:
                    REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xadd_results').inc()
                    logger.error(f"Redis connection error pushing results for message ID {message_id}: {e}")
                    ack_message = False # Don't ACK if result wasn't stored
                except Exception as e:
                    REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xadd_results').inc()
                    logger.error(f"Error pushing results for message ID {message_id}: {e}")
                    ack_message = False # Don't ACK if result wasn't stored

                # --- End Processing Steps ---

            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON for message ID {message_id}: {e}")
                PROCESSING_ERRORS.labels(**consumer_label, error_type='json_decode').inc()
                ack_message = True # Acknowledge poison pill
            except Exception as e:
                logger.error(f"Error processing message ID {message_id}: {e}", exc_info=True) # Log traceback
                PROCESSING_ERRORS.labels(**consumer_label, error_type='processing').inc()
                ack_message = False # Do not acknowledge, allow retry or manual intervention

            # 3. Acknowledge if processing was successful or if it's a poison pill
            if ack_message:
                try:
                    await redis_client.xack(REDIS_STREAM_NAME, REDIS_CONSUMER_GROUP, message_id)
                    MESSAGES_ACKNOWLEDGED.labels(**consumer_label).inc()
                    logger.info(f"Acknowledged message ID: {message_id}")
                except redis.exceptions.ConnectionError as e:
                    REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xack').inc()
                    logger.error(f"Redis connection error during xack for message ID {message_id}: {e}")
                    # Failed ack means message might be reprocessed, which is acceptable here
                except Exception as e:
                    REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='xack').inc()
                    logger.error(f"Unexpected error during xack for message ID {message_id}: {e}")

            processing_time = time.time() - start_time
            MESSAGE_PROCESSING_TIME.labels(**consumer_label).observe(processing_time)
            logger.debug(f"Message {message_id} processing time: {processing_time:.4f} seconds")

        except asyncio.CancelledError:
             logger.info("Stream processing task cancelled.")
             break # Exit the loop if cancelled
        except Exception as e: # Catch-all for the outer loop (e.g., issues with response parsing)
            logger.error(f"An unexpected error occurred in the main worker loop: {e}", exc_info=True)
            await asyncio.sleep(5) # Wait before continuing

# --- GDPR Deletion Endpoint Logic ---

async def handle_delete_user_data(request: web.Request):
    """Handles HTTP GET requests to delete user data from the Redis stream."""
    user_id_str = request.match_info.get('user_id')
    if not user_id_str:
        return web.Response(status=400, text="User ID must be provided in the path.")

    try:
        # Telethon user IDs are typically integers
        user_id_to_delete = int(user_id_str)
    except ValueError:
        return web.Response(status=400, text="Invalid User ID format. Must be an integer.")

    logger.info(f"Received GDPR deletion request for user ID: {user_id_to_delete}")

    deleted_count = 0
    message_ids_to_delete = []
    consumer_label = {'group': REDIS_CONSUMER_GROUP, 'consumer': REDIS_CONSUMER_NAME} # For potential metrics

    try:
        # Scan the entire stream. Warning: This can be slow for very large streams.
        # Consider alternative strategies (e.g., secondary index) for production.
        stream_entries = await redis_client.xrange(REDIS_STREAM_NAME, '-', '+')

        for message_id, message_fields in stream_entries:
            try:
                message_json = message_fields.get('message')
                if message_json:
                    message_data = json.loads(message_json)
                    # IMPORTANT: Verify the actual field name for sender ID in your stored JSON
                    # Common possibilities: 'sender_id', 'from_id', 'peer_id' within 'from_id' object etc.
                    # Adjust this logic based on the actual structure saved by ingestor.py
                    sender_id = message_data.get('sender_id') # Assuming 'sender_id' exists directly

                    # Handle cases where sender_id might be nested (e.g., inside 'from_id')
                    if sender_id is None and 'from_id' in message_data and isinstance(message_data['from_id'], dict):
                         sender_id = message_data['from_id'].get('user_id') # Example if nested

                    if sender_id == user_id_to_delete:
                        message_ids_to_delete.append(message_id)

            except json.JSONDecodeError:
                logger.warning(f"Skipping message ID {message_id} during GDPR scan due to JSON decode error.")
            except Exception as e:
                logger.warning(f"Skipping message ID {message_id} during GDPR scan due to processing error: {e}")

        if not message_ids_to_delete:
            logger.info(f"No messages found for user ID {user_id_to_delete} in stream {REDIS_STREAM_NAME}.")
            return web.Response(status=404, text=f"No messages found for user ID {user_id_to_delete}")

        # Delete the identified messages
        logger.info(f"Attempting to delete {len(message_ids_to_delete)} messages for user ID {user_id_to_delete}...")
        deleted_count = await redis_client.xdel(REDIS_STREAM_NAME, *message_ids_to_delete)
        logger.info(f"Successfully deleted {deleted_count} messages for user ID {user_id_to_delete} using XDEL.")

        return web.Response(status=200, text=f"Deleted {deleted_count} messages for user ID {user_id_to_delete}.")

    except redis.exceptions.ConnectionError as e:
        REDIS_OPERATIONS_ERRORS.labels(**consumer_label, operation='gdpr_scan_delete').inc()
        logger.error(f"Redis connection error during GDPR deletion for user ID {user_id_to_delete}: {e}")
        return web.Response(status=500, text="Internal server error (Redis connection).")
    except Exception as e:
        logger.error(f"Error during GDPR deletion process for user ID {user_id_to_delete}: {e}", exc_info=True)
        return web.Response(status=500, text="Internal server error during deletion process.")


# --- Web Server Setup ---

async def setup_web_app():
    """Sets up the aiohttp web application."""
    app = web.Application()
    app.add_routes([
        web.get('/delete/{user_id}', handle_delete_user_data),
        # Add other potential endpoints here (e.g., health check)
        web.get('/health', lambda r: web.Response(text="OK"))
    ])
    return app

async def start_web_server(app: web.Application):
    """Starts the aiohttp web server."""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', GDPR_API_PORT)
    logger.info(f"Starting GDPR API server on port {GDPR_API_PORT}...")
    await site.start()
    logger.info("GDPR API server started.")
    # Keep the server running until cancelled
    # Use try/finally to ensure cleanup on cancellation
    try:
        while True:
            await asyncio.sleep(3600) # Keep running, waking up occasionally
    except asyncio.CancelledError:
        logger.info("Web server task cancelled. Shutting down...")
        await runner.cleanup() # Clean up the runner
        logger.info("Web server shut down.")
        raise # Re-raise CancelledError

async def start_metrics_server():
    """Starts the Prometheus metrics server in a separate thread."""
    logger.info(f"Starting Prometheus metrics server on port {METRICS_PORT}")
    start_http_server(METRICS_PORT)

async def main():
    # Start metrics server in a background thread
    logger.info("Attempting to start metrics server thread...")
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    logger.info(f"Metrics server thread started, exposing metrics on port {METRICS_PORT}")

    # Setup Web App for GDPR API
    web_app = await setup_web_app()
    web_server_task = asyncio.create_task(start_web_server(web_app), name="WebServerTask")

    # Start Redis connection check and stream processing
    stream_processor_task = asyncio.create_task(run_stream_processor_with_checks(), name="StreamProcessorTask")

    # Run both tasks concurrently
    logger.info("Running AI Worker (Stream Processor and GDPR API)...")
    done, pending = await asyncio.wait(
        [web_server_task, stream_processor_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Log which task finished first and handle potential errors
    for task in done:
        try:
            result = task.result() # Check for exceptions
            logger.info(f"Task {task.get_name()} finished unexpectedly with result: {result}")
        except asyncio.CancelledError:
             logger.info(f"Task {task.get_name()} was cancelled.")
        except Exception as e:
            logger.error(f"Task {task.get_name()} failed with exception:", exc_info=e)


    # If one task finishes (e.g., crashes), cancel the other pending tasks
    logger.info("Attempting to cancel pending tasks...")
    for task in pending:
        logger.info(f"Cancelling task {task.get_name()}...")
        task.cancel()

    # Wait for cancellations to complete
    await asyncio.gather(*pending, return_exceptions=True)
    logger.info("All pending tasks cancelled.")
    logger.info("Main AI Worker process exiting.")


async def run_stream_processor_with_checks():
    """Wrapper to connect to Redis and start processing."""
    logger.info("Connecting to Redis for stream processing...")
    try:
        await redis_client.ping()
        logger.info("Successfully connected to Redis for stream processing.")
    except redis.exceptions.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}. AI Worker cannot start stream processing.")
        # Optionally increment a metric here
        return # Stop this task
    except Exception as e:
        logger.error(f"Unexpected error connecting to Redis: {e}. AI Worker cannot start stream processing.")
        return # Stop this task

    await process_stream_messages()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("AI Worker stopped manually.")
    finally:
        logger.info("AI Worker shut down.")