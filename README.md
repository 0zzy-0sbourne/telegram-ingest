# Telegram Ingestion Pipeline

This project implements a simplified application to ingest messages from a public Telegram channel, optionally process the text with an external AI model, and log the results to a CSV file.

## Features

*   **Telegram Ingestion**: Connects to Telegram as a user using Telethon, joins a specified channel, and listens for new messages.
*   **AI Model Processing (Optional)**: If configured via environment variables, calls an external Model API (e.g., an LLM endpoint) with the message text and a system prompt.
*   **CSV Logging**: Appends the timestamp, message ID, sender ID, message text, and structured classification results (`action`, `confidence`, `reason`, `raw_model_response`) for each message to a CSV file (`output_log.csv` by default).
*   **Result Messaging (Optional)**: If `RESULT_TARGET_CHAT` is configured in the `.env` file, sends the final JSON classification result as a message to the specified Telegram username or chat ID.

## Project Structure

```
telegram_ingestor/
├── .env.example        # Example environment variable configuration
├── requirements.txt    # Python dependencies
├── ingestor.py         # Single script for ingestion, processing, and logging
└── README.md           # This file
```
(Note: `ai_worker.py` has been removed in this simplified version)

## Setup

1.  **Clone the repository (or create the files as generated).**

2.  **Create a virtual environment (recommended):**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # On Windows use `.\venv\Scripts\activate` or `.\venv\Scripts\Activate.ps1` for PowerShell
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    *   Copy `.env.example` to `.env`:
        ```bash
        cp .env.example .env
        ```
    *   **Edit `.env`** and fill in the required values:
        *   `TELEGRAM_API_ID` and `TELEGRAM_API_HASH`: Obtain from [https://my.telegram.org/apps](https://my.telegram.org/apps).
        *   `TELEGRAM_PHONE`: Your phone number associated with the Telegram account (e.g., `+12223334444`).
        *   `TELEGRAM_CHANNEL`: The username (e.g., `@channel_name`) or chat ID of the target channel/chat.
        *   `TELEGRAM_SESSION_KEY`: A **secure, 32-byte encryption key** encoded in Base64. You can generate one using:
            ```bash
            # Generate a 32-byte key and base64 encode it
            openssl rand -base64 32
            ```
            Copy the output into the `.env` file.
        *   `MODEL_API_URL` (Optional): URL of an external Model API (e.g., OpenAI compatible). If provided, the script will POST the system prompt and message text to this URL.
        *   `MODEL_API_KEY` (Optional): API key for the Model API (sent as a Bearer token).
        *   `MODEL_SYSTEM_PROMPT`: The system prompt to use when calling the Model API (defaults to the detailed SignalClassifier-GPT prompt).
        *   `RESULT_TARGET_CHAT` (Optional): The Telegram username (e.g., `@your_username`) or chat ID (numeric) where the final JSON result should be sent as a message.
        *   `OUTPUT_CSV_FILE` (Optional): Path to the output CSV file (defaults to `output_log.csv`).

## Running the Application

1.  **Navigate** to the `telegram_ingestor` directory in your terminal.
2.  **Activate** your virtual environment (e.g., `.\venv\Scripts\activate`).
3.  **Run the script:**
    ```bash
    python ingestor.py
    ```
4.  **First Run:** The first time you run the script, Telethon will likely ask for your phone code and potentially your 2FA password via the terminal to authorize the session. Enter these when prompted.
5.  **Operation:** The script will connect to Telegram, process the latest message, then listen for new messages. For each message, it checks the timestamp, optionally calls the model API, parses the result, logs structured data to the CSV file, and optionally sends the result JSON as a message to `RESULT_TARGET_CHAT`. Press `Ctrl+C` to stop the script.

## Output Format (CSV)

The output CSV file (`output_log.csv` by default) will contain the following columns:

*   `timestamp`: ISO format timestamp when the processing *started*.
*   `message_id`: The unique ID of the Telegram message.
*   `sender_id`: The unique ID of the message sender.
*   `message_text`: The text content of the Telegram message.
*   `action`: The classification result (`LONG`, `SHORT`, `NONE`) from the model, or an error indicator (`ERROR`, `MISSING_ACTION`, `INVALID_ACTION`).
*   `confidence`: The confidence score (0.0-1.0) from the model, formatted to 2 decimal places, or `N/A` if parsing failed. Note: If the model returns confidence < 0.6, the action is overridden to `NONE`, but the original confidence might still be logged here depending on exact parsing logic. Check the `reason` field for clarification.
*   `reason`: The justification from the model, or an error/status message (`stale_or_irrelevant`, `no_text_content`, `no_model_configured`, `model_api_error`, `JSONDecodeError`, `ValueError`, etc.).
*   `raw_model_response`: The raw string response received from the model API, or the generated JSON for skipped messages (stale, no text). Useful for debugging model output format.

## Notes

*   **Timestamp Check:** Messages older than 5 minutes (based on the message's timestamp vs. current UTC time) will automatically result in `{"action":"NONE","confidence":0.0,"reason":"stale_or_irrelevant"}` being logged, and the model API will not be called.
*   **JSON Parsing:** The script attempts to parse the model's response as JSON and extract `action`, `confidence`, and `reason`. If parsing fails or validation rules (action type, confidence threshold) are not met, appropriate error indicators or default values are logged. Check the `reason` and `raw_model_response` columns for details.
*   This simplified version uses Telethon's default unencrypted file session (`.session` file).
*   Error handling is basic. For production use, consider adding more robust error handling, retries for API calls, etc.
*   The previous features like Redis queuing, separate worker, metrics, and GDPR endpoint have been removed for simplicity.