# ==============================================================================
# SCRIPT: run_assistant.py
# PURPOSE: Interacts with the OpenAI "Florido" assistant using a hardcoded API key.
#          This is a temporary workaround for networks that block pip installs.
# VERSION: 2.0 (with robust error handling for API calls)
# ==============================================================================

import os
import time
from openai import OpenAI, BadRequestError

# ==============================================================================
# CONFIGURATION
# ==============================================================================
API_KEY = "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" 
ASSISTANT_ID = "asst_Zw2Node6fTBF8RLeDbmAREMS"
CLEAN_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'clean_data')

# ==============================================================================
# MAIN SCRIPT FUNCTIONS
# ==============================================================================

def upload_files_to_openai(client, directory):
    uploaded_file_ids = []
    print(f"Uploading files from '{directory}'...")
    if not os.path.exists(directory):
        print(f"  - ERROR: Directory not found: {directory}")
        return []
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            with open(file_path, "rb") as f:
                try:
                    file_object = client.files.create(file=f, purpose='assistants')
                    uploaded_file_ids.append(file_object.id)
                    print(f"  - Successfully uploaded {filename} with File ID: {file_object.id}")
                except Exception as e:
                    print(f"  - FAILED to upload {filename}: {e}")
    return uploaded_file_ids

def run_conversation(client, assistant_id, file_ids):
    print("\nCreating a new conversation thread...")
    thread = client.beta.threads.create()
    print(f"Thread created with ID: {thread.id}")

    while True:
        user_input = input("\nYour prompt (or type 'exit' to quit): ")
        if user_input.lower() == 'exit':
            print("Ending conversation.")
            break
        
        # If the user just presses Enter, ask again.
        if not user_input.strip():
            print("Please enter a prompt.")
            continue

        try:
            # --- START OF ERROR HANDLING BLOCK ---
            attachments = [{"file_id": file_id, "tools": [{"type": "code_interpreter"}]} for file_id in file_ids]
            
            client.beta.threads.messages.create(
                thread_id=thread.id,
                role="user",
                content=user_input,
                attachments=attachments
            )

            run = client.beta.threads.runs.create(
                thread_id=thread.id,
                assistant_id=assistant_id,
                instructions="Please use the Code Interpreter tool and the attached CSV files to answer the user's request. Be precise and analytical."
            )
            print("Assistant is processing...")

            while run.status in ['queued', 'in_progress']:
                time.sleep(2)
                run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
                print(f"  - Run status: {run.status}")

            if run.status == 'completed':
                messages = client.beta.threads.messages.list(thread_id=thread.id)
                assistant_message = messages.data[0]
                for content in assistant_message.content:
                    if content.type == 'text':
                        print("\n--- Assistant's Response ---")
                        print(content.text.value)
                        print("--------------------------")
            else:
                print(f"\nRun ended with status: {run.status}")
                if run.last_error:
                    print(f"Error: {run.last_error.message}")
            
        except BadRequestError as e:
            # This will catch errors like the one you received.
            print("\n--- API Error ---")
            print(f"The API returned an error with your prompt: {e.message}")
            print("This can happen with unusual inputs. Please try rephrasing your prompt.")
            print("-----------------")
        except Exception as e:
            # This catches any other unexpected error.
            print(f"\nAn unexpected error occurred: {e}")
            break
            # --- END OF ERROR HANDLING BLOCK ---

def verify_assistant_config(client, assistant_id):
    print(f"\nVerifying Assistant '{assistant_id}' configuration...")
    try:
        assistant = client.beta.assistants.retrieve(assistant_id)
        tool_types = [tool.type for tool in assistant.tools]
        if "code_interpreter" not in tool_types:
            print("\n!!! CRITICAL WARNING: Assistant is not configured for Code Interpreter.")
            return False
        else:
            print("Assistant configuration is correct.")
            return True
    except Exception as e:
         print(f"Could not retrieve assistant. Error: {e}")
         return False

if __name__ == "__main__":
    if "YOUR_NEW_SECURE_API_KEY" in API_KEY:
        print("ERROR: Please open the script and replace 'sk-YOUR_NEW_SECURE_API_KEY' with your actual OpenAI API key.")
    else:
        client = OpenAI(api_key=API_KEY)
        if verify_assistant_config(client, ASSISTANT_ID):
            file_ids = upload_files_to_openai(client, CLEAN_DATA_DIR)
            if file_ids:
                run_conversation(client, ASSISTANT_ID, file_ids)
            else:
                print("No clean data files were found or uploaded. Cannot start conversation.")