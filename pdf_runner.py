import os
import subprocess
import sys

def run_batch_processing():
    """
    Finds all subdirectories in a main 'input_folder' and runs the 
    pdf_processor.py script on each one.
    """
    # Define the main input and output directories relative to this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    main_input_folder = os.path.join(script_dir, "input2")
    main_output_folder = os.path.join(script_dir, "output_folder_2")
    processor_script = os.path.join(script_dir, "pdf_main.py")

    # --- Pre-flight Checks ---
    if not os.path.exists(main_input_folder):
        print(f"Error: The main 'input_folder' was not found at: {main_input_folder}")
        print("Please create it and place your PDF folders inside.")
        return

    if not os.path.exists(processor_script):
        print(f"Error: The processor script was not found at: {processor_script}")
        return

    # Create the main output folder if it doesn't exist
    os.makedirs(main_output_folder, exist_ok=True)
    print(f"Outputs will be saved in: {main_output_folder}")

    # --- Find and Process Subdirectories ---
    subfolders_to_process = [
        f.path for f in os.scandir(main_input_folder) if f.is_dir()
    ]

    if not subfolders_to_process:
        print(f"No subfolders found inside '{main_input_folder}'. Nothing to process.")
        return

    print(f"\nFound {len(subfolders_to_process)} folders to process...")

    # --- Run Processing for Each Folder ---
    for folder_path in subfolders_to_process:
        folder_name = os.path.basename(folder_path)
        print(f"\n{'='*20}\nProcessing folder: {folder_name}\n{'='*20}")
        
        try:
            # Use sys.executable to ensure we use the same python interpreter
            command = [sys.executable, processor_script, folder_path, main_output_folder]
            
            # Run the processor script as a separate process
            # This is robust and prevents issues with memory or library conflicts
            subprocess.run(command, check=True)
            
            print(f"\nFinished processing folder: {folder_name}")

        except subprocess.CalledProcessError as e:
            print(f"!!! An error occurred while processing folder: {folder_name} !!!")
            print(f"Error details: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    print(f"\n{'='*20}\nBatch processing complete.\n{'='*20}")

if __name__ == "__main__":
    run_batch_processing()