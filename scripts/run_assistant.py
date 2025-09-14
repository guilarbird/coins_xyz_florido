# ==============================================================================
# SCRIPT: run_assistant.py
# PURPOSE: Interacts with the OpenAI Responses API using a modern "Prompt" object.
#          This script AUTOMATICALLY uploads necessary data files from Codespaces
#          and handles conversation orchestration.
# VERSION: 5.3 (Final - Corrects the 'attachments' parameter to 'tool_resources')
# ==============================================================================

import os
import time
from openai import OpenAI
import json

# ==============================================================================
# CONFIGURATION
# ==============================================================================
try:
    from dotenv import load_dotenv
    dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    load_dotenv(dotenv_path=dotenv_path)
except ImportError:
    print("Warning: python-dotenv not found. API key should be set as an environment variable.")

API_KEY = os.getenv("OPENAI_API_KEY")

PROMPT_ID = "pmpt_68b3ea01d3b481939d82679226830e5d05916d36df5598e5" 
VECTOR_STORE_ID = "vs_6899674ed4a08191b2a730856abefdc6"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CLEAN_DATA_DIR = os.path.join(BASE_DIR, '..', 'clean_data')
CONVERSATION_HISTORY_FILE = os.path.join(BASE_DIR, '..', 'conversation_history.json')

# ==============================================================================
# SCRIPT LOGIC
# ==============================================================================

def upload_files_for_interpreter(client, directory):
    """Finds all .csv files, uploads them, and returns a list of their file IDs."""
    uploaded_file_ids = []
    print(f"Uploading data files from '{directory}' for Code Interpreter...")
    if not os.path.exists(directory):
        print(f"  - WARNING: Directory not found: {directory}")
        return []
        
    for filename in os.listdir(directory):
        if filename.startswith("cleaned_") and filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "rb") as f:
                try:
                    file_object = client.files.create(file=f, purpose='assistants')
                    uploaded_file_ids.append(file_object.id)
                    print(f"  - Successfully uploaded {filename} with File ID: {file_object.id}")
                except Exception as e:
                    print(f"  - FAILED to upload {filename}: {e}")
    return uploaded_file_ids

def load_conversation_history(session_id: str):
    """Loads the message history for a given session_id."""
    try:
        if not os.path.exists(CONVERSATION_HISTORY_FILE):
            return [] 
        with open(CONVERSATION_HISTORY_FILE, 'r') as f:
            full_history = json.load(f)
        return full_history.get(session_id, [])
    except (IOError, json.JSONDecodeError):
        return []

def save_conversation_history(session_id: str, history: list):
    """Saves the message history for a given session_id."""
    full_history = {}
    if os.path.exists(CONVERSATION_HISTORY_FILE):
        try:
            with open(CONVERSATION_HISTORY_FILE, 'r') as f:
                full_history = json.load(f)
        except (IOError, json.JSONDecodeError):
            pass 
    full_history[session_id] = history
    with open(CONVERSATION_HISTORY_FILE, 'w') as f:
        json.dump(full_history, f, indent=2)

def run_conversation_with_new_api(client, code_interpreter_files, vector_store_id):
    """Starts an interactive chat session using the new Responses API with multiple tools."""
    print("\nStarting conversation with Florido...")
    if not code_interpreter_files:
        print("WARNING: No data files were uploaded for Code Interpreter. Analysis queries on CSVs will fail.")
        print("Knowledge base queries using File Search will still work.")

    session_id = "codespaces_terminal_session"
    
    while True:
        user_input = input("\nYour prompt (or type 'exit' to quit): ")
        if user_input.lower() == 'exit':
            break

        messages = load_conversation_history(session_id)
        
        # BUG FIX: The 'attachments' parameter was incorrect for the Responses API.
        # The correct way is to use the 'tool_resources' parameter on the user message.
        tool_resources_for_message = [
            {
                "tool": "code_interpreter",
                "files": code_interpreter_files
            }
        ]
        
        messages.append({
            "role": "user", 
            "content": user_input,
            "tool_resources": tool_resources_for_message
        })

        print("Assistant is processing...")
        
        try:
            # The tool_resources for File Search are passed at the top level of the create call.
            tool_resources_for_run = {"file_search": {"vector_store_ids": [vector_store_id]}}

            response = client.responses.create(
                prompt=PROMPT_ID,
                input=messages,
                tool_resources=tool_resources_for_run
            )

            assistant_reply = ""
            for item in response.output:
                if item.type == 'message' and item.role == 'assistant':
                    for content in item.content:
                        if content.type == 'output_text':
                            assistant_reply += content.text
            
            print("\n--- Assistant's Response ---")
            print(assistant_reply)
            print("--------------------------")

            # Clean the tool_resources from the user message before saving to history
            if "tool_resources" in messages[-1]:
                messages[-1].pop("tool_resources")
            messages.append({"role": "assistant", "content": assistant_reply})
            save_conversation_history(session_id, messages)

        except Exception as e:
            print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    if not API_KEY or "sk-" not in str(API_KEY):
        print("ERROR: OPENAI_API_KEY is not configured correctly in your .env file.")
    elif "pmpt_" not in PROMPT_ID:
        print("ERROR: Please edit the script and set your `PROMPT_ID` from the OpenAI Playground.")
    else:
        client = OpenAI(api_key=API_KEY)
        
        code_interpreter_file_ids = upload_files_for_interpreter(client, CLEAN_DATA_DIR)
        
        run_conversation_with_new_api(client, code_interpreter_file_ids, VECTOR_STORE_ID)