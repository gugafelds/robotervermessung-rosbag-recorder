import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import subprocess
import os
from pathlib import Path

class CSVToPostgreSQLGUI:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title('CSV to PostgreSQL')
        self.window.geometry('700x450')

        self.style = ttk.Style()
        self.style.configure('.', font=('Ubuntu', 10))
        self.style.configure('TLabelFrame', font=('Ubuntu', 14, 'bold'), foreground='black')
        self.style.configure('TButton', font=('Ubuntu Light', 10), foreground='black', background='SkyBlue3', padding=5)
        self.style.map('TButton', foreground=[('active', '!disabled', 'black')],
                       background=[('active', 'SkyBlue2')])
        self.style.configure('TEntry', font=('Ubuntu Light', 10), foreground='black', padding=5)
        self.style.configure('TCheckbutton', font=('Ubuntu Light', 10))

        self.setup_ui()

    def setup_ui(self):
        main_frame = ttk.Frame(self.window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # File Selection Section
        file_frame = ttk.LabelFrame(main_frame, text='File Selection')
        file_frame.pack(pady=10, padx=10, fill=tk.X)

        self.file_path_entry = ttk.Entry(file_frame, width=50, style='TEntry')
        self.file_path_entry.grid(row=0, column=0, padx=5, pady=5, sticky='ew')
        ttk.Button(file_frame, text='Browse', command=self.open_file_dialog).grid(row=0, column=1, padx=5, pady=5)

        # Input Fields Section
        input_frame = ttk.LabelFrame(main_frame, text='Input Fields')
        input_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        labels = ['Robot Model:', 'Bahnplanung:', 'Source Data IST:', 'Source Data SOLL:']
        default_values = ['abb_irb4400', 'abb_steuerung', 'vicon', 'abb_websocket']
        self.entries = {}

        for i, (label, default) in enumerate(zip(labels, default_values)):
            ttk.Label(input_frame, text=label).grid(row=i, column=0, padx=5, pady=5, sticky='e')
            entry = ttk.Entry(input_frame, style='TEntry')
            entry.insert(0, default)
            entry.grid(row=i, column=1, padx=5, pady=5, sticky='ew')
            self.entries[label.lower().replace(' ', '_').replace(':', '')] = entry

        input_frame.columnconfigure(1, weight=1)

        # Upload to PostgreSQL Checkbox
        self.upload_var = tk.BooleanVar(value=True)
        self.upload_checkbox = ttk.Checkbutton(main_frame, text='Upload to PostgreSQL', 
                                               variable=self.upload_var, style='TCheckbutton')
        self.upload_checkbox.pack(pady=10)

        # Start Button
        ttk.Button(main_frame, text='Start Processing', command=self.start_processing).pack(pady=10)

        # Status Label
        self.status_label = ttk.Label(main_frame, text='')
        self.status_label.pack(pady=5)

        main_frame.columnconfigure(0, weight=1)

    def open_file_dialog(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.file_path_entry.delete(0, tk.END)
            self.file_path_entry.insert(0, file_path)

    def start_processing(self):
        file_path = self.file_path_entry.get()
        if not file_path:
            messagebox.showerror("Error", "Please select a CSV file.")
            return

        script_path = Path.home() / "robotervermessung-rosbag-recorder/scripts/insert_data_postgresql.py"
        command = [
            "python3", str(script_path),
            file_path,
            self.entries['robot_model'].get(),
            self.entries['bahnplanung'].get(),
            self.entries['source_data_ist'].get(),
            self.entries['source_data_soll'].get()
        ]

        # Use the upload_var value directly in the subprocess call
        upload_database = "y" if self.upload_var.get() else "n"

        try:
            # Use subprocess.Popen to create a pipe for input
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            # Send the upload choice to the script
            stdout, stderr = process.communicate(input=upload_database)

            if process.returncode == 0:
                self.status_label.config(text='CSV processing completed successfully.')
                if self.upload_var.get():
                    self.status_label.config(text='CSV processing and upload to PostgreSQL completed successfully.')
                else:
                    self.status_label.config(text='CSV processing completed successfully (without uploading to PostgreSQL).')
            else:
                self.status_label.config(text=f'An error occurred: {stderr}')
        except Exception as e:
            self.status_label.config(text=f'An error occurred: {str(e)}')

    def run(self):
        self.window.mainloop()

def main():
    gui = CSVToPostgreSQLGUI()
    gui.run()

if __name__ == "__main__":
    main()