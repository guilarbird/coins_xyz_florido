# ==============================================================================
# SCRIPT: run_modern_assistant.py
# PURPOSE: Interacts with the new OpenAI Responses API using a "Prompt" object
#          and correctly manages a persistent Code Interpreter Container.
# VERSION: 6.1 (Final - Added container.type to tool definition)
# ==============================================================================

import os
from openai import OpenAI
import json

# --- Securely load environment variables ---
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
except ImportError:
    print("Warning: python-dotenv not found. API key should be set as an environment variable.")

# ==============================================================================
# CONFIGURATION
# ==============================================================================

API_KEY = os.getenv("OPENAI_API_KEY")
PROMPT_ID = "pmpt_68b3ea01d3b481939d82679226830e5d05916d36df5598e5" # Replace with your Prompt ID with NO tools enabled

CONVERSATION_HISTORY_FILE = os.path.join(os.path.dirname(__file__), '..', 'conversation_history.json')
CONTAINER_ID_FILE = os.path.join(os.path.dirname(__file__), '..', 'container_id.txt')

# ==============================================================================
# SCRIPT LOGIC
# ==============================================================================

def load_conversation_history(session_id: str):
    if not os.path.exists(CONVERSATION_HISTORY_FILE): return []
    try:
        with open(CONVERSATION_HISTORY_FILE, 'r') as f: return json.load(f).get(session_id, [])
    except (json.JSONDecodeError, FileNotFoundError): return []

def save_conversation_history(session_id: str, history: list):
    full_history = {}
    if os.path.exists(CONVERSATION_HISTORY_FILE):
        try:
            with open(CONVERSATION_HISTORY_FILE, 'r') as f: full_history = json.load(f)
        except json.JSONDecodeError: full_history = {}
    full_history[session_id] = history
    with open(CONVERSATION_HISTORY_FILE, 'w') as f: json.dump(full_history, f, indent=2)

def setup_container(client):
    """Creates or retrieves a Code Interpreter container."""
    if os.path.exists(CONTAINER_ID_FILE):
        with open(CONTAINER_ID_FILE, 'r') as f:
            container_id = f.read().strip()
            # Verify the container still exists on OpenAI's side
            try:
                client.containers.retrieve(container_id)
                print(f"Using existing and valid container: {container_id}")
                return container_id
            except Exception as e:
                print(f"Warning: Found container ID {container_id}, but it's invalid or deleted. Recreating. Error: {e}")

    print("No valid container found. Creating a new one...")
    
    file_ids = []
    clean_data_dir = os.path.join(os.path.dirname(__file__), '..', 'clean_data')
    if not os.path.exists(clean_data_dir):
        print(f"ERROR: Clean data directory not found at {clean_data_dir}")
        return None

    for filename in os.listdir(clean_data_dir):
        if filename.endswith(".csv"):
            file_path = os.path.join(clean_data_dir, filename)
            with open(file_path, "rb") as f:
                try:
                    file = client.files.create(file=f, purpose='user_data')
                    file_ids.append(file.id)
                    print(f"  - Uploaded {filename} (ID: {file.id})")
                except Exception as e: print(f"  - FAILED to upload {filename}: {e}")
    
    if not file_ids: print("No files were uploaded. Cannot create container."); return None

    try:
        print("Creating container and pre-loading files...")
        container = client.containers.create(name="Florido_Data_Analysis_Container", file_ids=file_ids)
        container_id = container.id
        print(f"SUCCESS: Container created with ID: {container_id}")
        with open(CONTAINER_ID_FILE, 'w') as f: f.write(container_id)
        return container_id
    except Exception as e:
        print(f"FATAL: Could not create container. Error: {e}")
        return None

def run_conversation(client, container_id):
    """Starts an interactive chat session."""
    print("\nStarting conversation with Florido (Responses API)...")
    session_id = "terminal_chat"
    
    while True:
        user_input = input("\nYour prompt (or type 'exit' to quit): ")
        if user_input.lower() == 'exit': break

        messages = load_conversation_history(session_id)
        messages.append({"role": "user", "content": [{"type": "input_text", "text": user_input}]})
        
        print("Assistant is processing...")
        try:
            # FINAL FIX APPLIED HERE:
            # The 'container' object requires a 'type' parameter.
            tools = [
                {
                    "type": "code_interpreter",
                    "container": {
                        "type": "container_id", 
                        "id": container_id
                    }
                }
            ]

            response = client.responses.create(
                prompt={"id": PROMPT_ID},
                input=messages,
                tools=tools
            )

            assistant_reply = ""
            for item in response.output:
                if item.type == 'message' and item.role == 'assistant':
                    for content in item.content:
                        if content.type == 'output_text': assistant_reply += content.text
            
            print("\n--- Assistant's Response ---"); print(assistant_reply); print("--------------------------")

            messages.append({"role": "assistant", "content": assistant_reply})
            save_conversation_history(session_id, messages)

        except Exception as e: print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    if not API_KEY or "sk-" not in API_KEY: print("ERROR: OPENAI_API_KEY is not configured correctly.")
    elif "pmpt_..." in PROMPT_ID: print("ERROR: Please edit the script and set your `PROMPT_ID`.")
    else:
        client = OpenAI(api_key=API_KEY)
        active_container_id = setup_container(client)
        if active_container_id:
            run_conversation(client, active_container_id)
        else:
            print("Could not start conversation without a valid container.")