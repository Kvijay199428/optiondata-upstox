import subprocess
import os
import PySimpleGUI as sg
import threading
import time
from datetime import datetime
import psutil
import sys
import io
# import pingPong

class ConsoleCapture(io.StringIO):
    def __init__(self):
        super().__init__()
        self.logs = []

    def write(self, txt):
        # Capture the console logs
        self.logs.append(txt)
        super().write(txt)

    def get_logs(self):
        return ''.join(self.logs)

console_output = ConsoleCapture()
sys.stdout = console_output

# Get the absolute path of the converted PNG icon
icon_path = os.path.join(os.getcwd(), "api/icon/TMA-3.png")
print(f"Debug: Icon path is {icon_path}")

# Check if the file path exists before trying to load the image
if not os.path.exists(icon_path):
    print(f"Error: Icon file not found at {icon_path}")
else:
    print("Debug: Icon file found.")

    # List of other scripts to run after login-dash.py finishes
    scripts = [
        'historic_optionChain_BankNifty.py',
        'historic_optionChain_FinNifty.py',
        'historic_optionChain_Nifty50.py',
        'historic_optionChain_NiftyMidcpSelect.py',
        'historic_optionChain_NiftyNXT50.py',
        'historic_optionChain_Sensex.py',
        'historic_optionChain_Bankex.py',
    ]
    print(f"Debug: Scripts to run - {scripts}")

    # Dictionary to store logs for each script
    script_logs = {script: '' for script in ['logout.py', 'login-dash.py'] + scripts}
    print("Debug: Initialized script logs.")

    # Market timings
    market_timing = {
        "NSE/BSE/NFO/BFO": {"start": "09:15", "end": "15:30", "days": [0, 1, 2, 3, 4]},  # Monday to Friday
        "MCX": {"start": "09:00", "end": "23:30", "days": [0, 1, 2, 3, 4]}  # Monday to Friday
    }

    def is_market_open():
        now = datetime.now()
        current_time = now.strftime("%H:%M")
        day_of_week = now.weekday()
        print(f"Debug: Checking market status at {current_time} on day {day_of_week}")

        market_status = {}
        for market, timing in market_timing.items():
            if day_of_week in timing["days"]:
                if timing["start"] <= current_time <= timing["end"]:
                    market_status[market] = "Open"
                else:
                    market_status[market] = "Closed"
            else:
                market_status[market] = "Closed"
        print(f"Debug: Market status - {market_status}")
        return market_status

    # Function to run a script and capture its logs
    def run_script(script, window, script_status):
        script_path = os.path.join(os.getcwd(), script)
        print(f"Debug: Running script {script} from {script_path}")
        
        if os.path.isfile(script_path):
            window[script_status].update("Running", text_color='green')
            print(f"Debug: Script {script} found, starting execution.")
            
            try:
                process = subprocess.Popen(
                    ['python', script_path], 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    text=True, 
                    encoding='utf-8',
                    errors='replace'
                )
                print(f"Debug: Subprocess started for {script}")

                # Stream the output to avoid buffer overflow
                while True:
                    output = process.stdout.readline()
                    if output == '' and process.poll() is not None:
                        break
                    if output:
                        print(f"Debug: Output from {script} - {output.strip()}")
                        script_logs[script] += output.strip() + '\n'

                stdout, stderr = process.communicate()
                script_logs[script] += stdout + stderr

                if process.returncode == 0:
                    print(f"Debug: {script} finished successfully.")
                    window[script_status].update("Stopped", text_color='red')
                else:
                    print(f"Error: {script} encountered an error.")
                    window[script_status].update("Error", text_color='red')
                    script_logs[script] += f"\nError occurred: {stderr}"

            except Exception as e:
                print(f"Exception: {script} - {str(e)}")
                window[script_status].update(f"Error: {str(e)}", text_color='red')
                script_logs[script] += f"\nException: {str(e)}"
        else:
            print(f"Error: Script {script} not found at {script_path}")
            window[script_status].update("Not found", text_color='red')

    def run_logout_script(window, script_status):
        print("Debug: Running logout script.")
        run_script('logout1.py', window, script_status)

    def run_all_scripts(window, script_status):
        def run_script_and_wait(script, status_key):
            print(f"Debug: Running {script} and waiting for completion.")
            run_script(script, window, status_key)
            while window[status_key].get() == "Running":
                time.sleep(0.1)
            
            time.sleep(2)
            return window[status_key].get() != "Error"

        if run_script_and_wait('logout1.py', script_status[0]):
            if run_script_and_wait('login-dash.py', script_status[1]):
                print("Debug: Starting concurrent script execution.")
                threads = []
                for i, script in enumerate(scripts):
                    thread = threading.Thread(target=run_script, args=(script, window, script_status[i + 2]))
                    threads.append(thread)
                    thread.start()
                    print(f"Debug: Started thread for {script}.")

                for thread in threads:
                    thread.join()
                    print(f"Debug: Thread for {script} finished.")
            else:
                print("Error: login-dash.py failed.")
                window[script_status[1]].update("Failed", text_color='red')
        else:
            print("Error: logout.py failed.")
            window[script_status[0]].update("Failed", text_color='red')

    # Function to display the captured logs in the GUI
    def show_logs(window):
        logs = console_output.get_logs()
        sg.popup_scrolled(logs, title="Logs", size=(60, 20))

    # def update_ltp_values(window):
    #     while True:
    #         window['nifty50ltp'].update(f"{pingPong.nifty50ltp:.2f}")
    #         window['niftynxt50ltp'].update(f"{pingPong.niftynxt50ltp:.2f}")
    #         window['niftyfinserviceltp'].update(f"{pingPong.niftyfinserviceltp:.2f}")
    #         window['niftymidselectltp'].update(f"{pingPong.niftymidselectltp:.2f}")
    #         window['niftybankltp'].update(f"{pingPong.niftybankltp:.2f}")
    #         window['sensexltp'].update(f"{pingPong.sensexltp:.2f}")
    #         window['bankexltp'].update(f"{pingPong.bankexltp:.2f}")
    #         time.sleep(1)  # Update every second

    def create_window():
        print("Debug: Creating the PySimpleGUI window.")
        sg.theme('DarkAmber')
                
        layout = [
            [sg.Column([[sg.Image(filename=icon_path), sg.Text('TMA Trade Max Algo', size=(40, 1), font=('Helvetica', 16), justification='left')]])],
            [sg.Text('Market Status', font=('Helvetica', 12), justification='center')],
            [sg.Text('NSE/BSE/NFO/BFO', size=(20, 1)), sg.Text('', key='nse_bse_status', size=(10, 1))],
            [sg.Text('MCX', size=(20, 1)), sg.Text('', key='mcx_status', size=(10, 1))],
            [sg.Text('Current Time', size=(20, 1)), sg.Text('', key='time', size=(20, 1))],
            [sg.Text('Logout', size=(30, 1)), sg.Text('', key='logout_status', size=(10, 1))],
            [sg.Text('Login', size=(30, 1)), sg.Text('', key='login_status', size=(10, 1))],
            # [sg.Column([[sg.Text('LTP', font=('Helvetica', 12))]], justification='center')],
            [sg.Text('Nifty50', size=(30, 1)), sg.Text('', key='nifty50ltp', size=(10, 1)), sg.Text('', key='nifty50_status', size=(10, 1))],
            [sg.Text('Nifty NEXT 50', size=(30, 1)), sg.Text('', key='niftynxt50ltp', size=(10, 1)), sg.Text('', key='niftynxt50_status', size=(10, 1))],
            [sg.Text('Fin Nifty', size=(30, 1)), sg.Text('', key='niftyfinserviceltp', size=(10, 1)), sg.Text('', key='finnifty_status', size=(10, 1))],
            [sg.Text('Nifty Midcp Select', size=(30, 1)), sg.Text('', key='niftymidselectltp', size=(10, 1)), sg.Text('', key='niftymidselect_status', size=(10, 1))],
            [sg.Text('Bank Nifty', size=(30, 1)), sg.Text('', key='niftybankltp', size=(10, 1)), sg.Text('', key='banknifty_status', size=(10, 1))],
            [sg.Text('Sensex', size=(30, 1)), sg.Text('', key='sensexltp', size=(10, 1)), sg.Text('', key='sensex_status', size=(10, 1))],
            [sg.Text('Bankex', size=(30, 1)), sg.Text('', key='bankexltp', size=(10, 1)), sg.Text('', key='bankex_status', size=(10, 1))],
            [sg.Button('Logout', size=(10, 1), key='logout'), sg.Button('Run TMA', size=(10, 1), key='run_scripts')],
            [sg.Button('View Logs', size=(10, 1), key='view_logs'), sg.Button('Close', size=(10, 1), key='close')]
        ]
        
        window = sg.Window('TMA Trade Max Algo', layout, finalize=True)
        return window

    def update_market_status(window):
        print("Debug: Updating market status.")
        while True:
            market_status = is_market_open()
            window['nse_bse_status'].update(market_status["NSE/BSE/NFO/BFO"])
            window['mcx_status'].update(market_status["MCX"])
            window['time'].update(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            time.sleep(1)

    # Main function
    def main():
        print("Debug: Starting main function.")
        window = create_window()

        script_status_keys = ['logout_status', 'login_status', 'banknifty_status', 'finnifty_status', 'nifty50_status',
                              'niftymidselect_status', 'niftynxt50_status', 'sensex_status', 'bankex_status']

        threading.Thread(target=update_market_status, args=(window,), daemon=True).start()

        # threading.Thread(target=update_ltp_values, args=(window,), daemon=True).start()

        while True:
            event, values = window.read()

            if event == sg.WIN_CLOSED or event == 'close':
                print("Debug: Window closed.")
                break
            elif event == 'logout':
                print("Debug: Logout button clicked.")
                threading.Thread(target=run_logout_script, args=(window, 'logout_status')).start()
            elif event == 'run_scripts':
                print("Debug: Run All Scripts button clicked.")
                threading.Thread(target=run_all_scripts, args=(window, script_status_keys)).start()
            elif event == 'view_logs':
                print("Debug: View Logs button clicked.")
                show_logs(window)

        window.close()

    if __name__ == '__main__':
        main()
