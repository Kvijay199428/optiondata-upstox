import os
import time
import schedule
from multiprocessing import Process
import random
import sys
import logging
from datetime import datetime
import traceback
import calendar
import argparse

OPTION_CHAIN_DIR = os.path.join(os.getcwd(), "historic_optionChain")

# Define paths to the scripts - Update these with full paths
SCRIPTS = {
    "logoutCLI": os.path.join(OPTION_CHAIN_DIR, "logoutCLI.py"),
    "loginCLI": os.path.join(OPTION_CHAIN_DIR, "loginCLI.py"),
    "options": [
        os.path.join(OPTION_CHAIN_DIR, "OC_Bankex.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_BankNifty.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_FinNifty.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_Nifty50.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_NiftyMidcpSelect.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_NiftyNXT50.py"),
        os.path.join(OPTION_CHAIN_DIR, "OC_Sensex.py"),
       
    ]
}

# Enhanced logging configuration
LOG_FILE = os.path.join(os.getcwd(), "api", "autorun.log")
DEBUG_MODE = True

def is_trading_hours():
    """Check if current time is within trading hours (9:00-15:30)."""
    current_time = datetime.now().time()
    start_time = datetime.strptime("09:00", "%H:%M").time()
    end_time = datetime.strptime("15:30", "%H:%M").time()
    return start_time <= current_time <= end_time

def is_weekday():
    """Check if today is a weekday."""
    return datetime.now().weekday() < 5

def get_current_day():
    """Get current day name and check if it's a weekday."""
    current_day = datetime.now().strftime("%A")
    is_weekday = datetime.now().weekday() < 5
    return current_day, is_weekday

def setup_logging():
    """Configure logging with both file and console output."""
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        
        logging.basicConfig(
            level=logging.DEBUG if DEBUG_MODE else logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        current_day, is_weekday = get_current_day()
        logging.info(f"Today is {current_day} - {'Weekday' if is_weekday else 'Weekend'}")
        
    except Exception as e:
        print(f"Error setting up logging: {str(e)}")
        sys.exit(1)

def verify_scripts_exist():
    """Verify all script files exist before attempting to run them."""
    missing_scripts = []
    
    # Check logoutCLI and loginCLI scripts
    for script_type in ["logoutCLI", "loginCLI"]:
        if not os.path.isfile(SCRIPTS[script_type]):
            missing_scripts.append(SCRIPTS[script_type])
    
    # Check option scripts
    for script in SCRIPTS["options"]:
        if not os.path.isfile(script):
            missing_scripts.append(script)
    
    if missing_scripts:
        logging.error(f"Missing script files: {', '.join(missing_scripts)}")
        return False
    return True

def run_script(script_path):
    """Run a Python script synchronously with enhanced error handling."""
    try:
        if not os.path.isfile(script_path):
            logging.error(f"Script not found: {script_path}")
            return False
            
        logging.info(f"[START] Running script: {script_path}")
        
        # Use subprocess instead of os.system for better control and output capture
        import subprocess
        process = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(script_path)  # Set working directory to script location
        )
        
        # Log the output
        if process.stdout:
            logging.debug(f"Script output:\n{process.stdout}")
        if process.stderr:
            logging.error(f"Script errors:\n{process.stderr}")
        
        status = "SUCCESS" if process.returncode == 0 else "FAILED"
        logging.info(f"[END] Script {script_path} completed with status: {status}")
        
        return process.returncode == 0
    except Exception as e:
        logging.error(f"Error running {script_path}: {str(e)}")
        logging.debug(traceback.format_exc())
        return False

def run_concurrent_scripts(script_paths):
    """Run multiple Python scripts concurrently with process monitoring."""
    processes = []
    process_info = {}
    
    for script in script_paths:
        if not os.path.isfile(script):
            logging.error(f"Script not found, skipping: {script}")
            continue
            
        try:
            logging.info(f"[START] Starting script concurrently: {script}")
            p = Process(target=run_script, args=(script,))
            processes.append(p)
            process_info[p.pid] = script
            p.start()
        except Exception as e:
            logging.error(f"Failed to start {script}: {str(e)}")
    
    # Monitor and wait for all processes
    for p in processes:
        try:
            p.join()
            script_name = process_info.get(p.pid, "Unknown script")
            logging.info(f"Process for {script_name} completed with exit code: {p.exitcode}")
        except Exception as e:
            logging.error(f"Error waiting for process: {str(e)}")

def task_sequence():
    """Run the task sequence with validation and error handling."""
    current_day, is_weekday = get_current_day()
    logging.info(f"Running task sequence on {current_day}")
    
    if not is_weekday:
        logging.info("Today is weekend. Skipping task sequence.")
        return False
        
    try:
        if not verify_scripts_exist():
            logging.error("Aborting task sequence due to missing scripts")
            return False
        
        # Execute logoutCLI script
        logging.info("[TASK] Starting logoutCLI script...")
        if not run_script(SCRIPTS["logoutCLI"]):
            logging.error("logoutCLI script failed, aborting sequence")
            return False
        
        # Add delay between scripts
        time.sleep(5)  # Increased delay to 5 seconds
        
        # Execute loginCLI script
        logging.info("[TASK] Starting loginCLI script...")
        if not run_script(SCRIPTS["loginCLI"]):
            logging.error("loginCLI script failed, aborting sequence")
            return False
        
        # Add delay before option chain scripts
        time.sleep(5)  # Increased delay to 5 seconds
        
        # Execute option chain scripts
        logging.info("[TASK] Starting option chain scripts concurrently...")
        run_concurrent_scripts(SCRIPTS["options"])
        
        return True
    except Exception as e:
        logging.error(f"Error in task sequence: {str(e)}")
        logging.debug(traceback.format_exc())
        return False

def schedule_tasks():
    """Schedule tasks with improved time handling and error recovery."""
    def random_task():
        current_day, is_weekday = get_current_day()
        current_time = datetime.now().strftime("%H:%M")
        
        logging.info(f"Checking schedule: {current_day} at {current_time}")
        
        if not is_weekday:
            logging.info("Weekend detected. Skipping execution.")
            return
            
        if current_time <= "15:30":
            random_delay = random.randint(0, 3900)
            logging.info(f"[INFO] Delaying task execution by {random_delay} seconds")
            time.sleep(random_delay)
            task_sequence()
    
    # Schedule tasks for weekdays
    for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
        getattr(schedule.every(), day).at("09:00").do(random_task)
    
    logging.info("Scheduled tasks for weekdays at 09:00")
    
    while True:
        try:
            schedule.run_pending()
            current_day, is_weekday = get_current_day()
            current_time = datetime.now().strftime("%H:%M")
            
            if current_time >= "15:30":
                logging.info(f"[INFO] Stopping execution for today ({current_day})")
                # Sleep until next day
                time.sleep(3600)  # Check every hour
            else:
                time.sleep(1)
                
        except Exception as e:
            logging.error(f"Scheduler error: {str(e)}")
            logging.debug(traceback.format_exc())
            time.sleep(60)  # Wait a minute before retrying
def schedule_and_run_tasks():
    """Schedule tasks and handle immediate execution if within trading hours."""
    def random_task():
        current_day, is_weekday = get_current_day()
        current_time = datetime.now().strftime("%H:%M")
        
        logging.info(f"Checking schedule: {current_day} at {current_time}")
        
        if not is_weekday:
            logging.info("Weekend detected. Skipping execution.")
            return
            
        if current_time <= "15:30":
            random_delay = random.randint(0, 3900)
            logging.info(f"[INFO] Delaying task execution by {random_delay} seconds")
            time.sleep(random_delay)
            task_sequence()

    # Schedule for future days
    for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']:
        getattr(schedule.every(), day).at("09:00").do(random_task)
    
    logging.info("Scheduled tasks for weekdays at 09:00")

    # Check for immediate execution
    current_time = datetime.now().strftime("%H:%M")
    current_day, is_weekday = get_current_day()
    
    if is_weekday and "09:00" <= current_time <= "15:30":
        logging.info(f"Current time {current_time} is within trading hours. Starting immediate execution...")
        task_sequence()
    else:
        logging.info(f"Current time {current_time} is outside trading hours. Waiting for next scheduled run...")

    # Continue with regular schedule
    while True:
        try:
            schedule.run_pending()
            current_time = datetime.now().strftime("%H:%M")
            
            if current_time >= "15:30":
                logging.info(f"[INFO] Stopping execution for today ({current_day})")
                time.sleep(3600)  # Check every hour
            else:
                time.sleep(60)  # Check every minute during trading hours
                
        except Exception as e:
            logging.error(f"Scheduler error: {str(e)}")
            logging.debug(traceback.format_exc())
            time.sleep(60)

def execute_loginCLI_logoutCLI():
    """Execute logoutCLI and loginCLI scripts once for current day."""
    logging.info("Starting one-time execution of logoutCLI and loginCLI scripts")
    
    try:
        # Execute logoutCLI script
        logging.info("[ONE-TIME] Starting logoutCLI script...")
        if not run_script(SCRIPTS["logoutCLI"]):
            logging.error("One-time logoutCLI script failed")
            return False
        
        # Add delay between scripts
        time.sleep(5)
        
        # Execute loginCLI script
        logging.info("[ONE-TIME] Starting loginCLI script...")
        if not run_script(SCRIPTS["loginCLI"]):
            logging.error("One-time loginCLI script failed")
            return False
            
        logging.info("One-time execution completed successfully")
        return True
        
    except Exception as e:
        logging.error(f"Error in one-time execution: {str(e)}")
        logging.debug(traceback.format_exc())
        return False

def main():
    """Main function with argument parsing for different execution modes."""
    parser = argparse.ArgumentParser(description='Script Runner with multiple modes')
    parser.add_argument('--mode', choices=['schedule', 'loginCLI-logoutCLI'], 
                       default='schedule',
                       help='Execution mode: schedule (default) or loginCLI-logoutCLI')
    
    args = parser.parse_args()
    
    setup_logging()
    logging.info("=== Script Runner Started ===")
    current_day, is_weekday = get_current_day()
    current_time = datetime.now().strftime("%H:%M:%S")
    
    logging.info(f"Current day: {current_day} ({'Weekday' if is_weekday else 'Weekend'})")
    logging.info(f"Current time: {current_time}")
    
    if args.mode == 'loginCLI-logoutCLI':
        logging.info("Running in loginCLI-logoutCLI mode")
        execute_loginCLI_logoutCLI()
        sys.exit(0)
    else:
        logging.info("Running in schedule mode")
        if is_weekday:
            if "09:00" <= current_time <= "15:30":
                logging.info("Currently in trading hours (09:00-15:30)")
            else:
                logging.info("Outside trading hours (09:00-15:30)")
        schedule_and_run_tasks()

if __name__ == "__main__":
    main()
