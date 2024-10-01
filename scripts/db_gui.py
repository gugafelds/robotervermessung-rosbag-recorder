import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class CSVToPostgreSQLGUI:
    def __init__(self, start_processing_callback):
        self.window = tk.Tk()
        self.window.title('CSV to PostgreSQL')
        self.window.geometry('700x650')  # Increased height to accommodate new button

        self.start_processing_callback = start_processing_callback

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
        file_frame = ttk.LabelFrame(main_frame, text='File/Folder Selection')
        file_frame.pack(pady=10, padx=10, fill=tk.X)

        self.file_path_entry = ttk.Entry(file_frame, width=50, style='TEntry')
        self.file_path_entry.grid(row=0, column=0, padx=5, pady=5, sticky='ew')
        ttk.Button(file_frame, text='Select File', command=self.open_file_dialog).grid(row=0, column=1, padx=5, pady=5)
        ttk.Button(file_frame, text='Select Folder', command=self.open_folder_dialog).grid(row=0, column=2, padx=5, pady=5)

        # Input Fields Section
        input_frame = ttk.LabelFrame(main_frame, text='Input Fields')
        input_frame.pack(pady=10, padx=10, fill=tk.X)

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

        # Progress Bar
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(pady=10, fill=tk.X)

        # Status Text Widget
        self.status_text = tk.Text(main_frame, height=10, wrap=tk.WORD, font=('Ubuntu Light', 10))
        self.status_text.pack(pady=5, fill=tk.BOTH, expand=True)
        self.status_text.config(state=tk.DISABLED)  # Make it read-only

        # Add a scrollbar to the status text
        scrollbar = ttk.Scrollbar(main_frame, orient='vertical', command=self.status_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_text.config(yscrollcommand=scrollbar.set)

        main_frame.columnconfigure(0, weight=1)

    def open_file_dialog(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.file_path_entry.delete(0, tk.END)
            self.file_path_entry.insert(0, file_path)

    def open_folder_dialog(self):
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.file_path_entry.delete(0, tk.END)
            self.file_path_entry.insert(0, folder_path)

    def start_processing(self):
        file_or_folder_path = self.file_path_entry.get()
        if not file_or_folder_path:
            messagebox.showerror("Error", "Please select a CSV file or a folder.")
            return

        params = {
            'file_or_folder_path': file_or_folder_path,
            'robot_model': self.entries['robot_model'].get(),
            'bahnplanung': self.entries['bahnplanung'].get(),
            'source_data_ist': self.entries['source_data_ist'].get(),
            'source_data_soll': self.entries['source_data_soll'].get(),
            'upload_database': self.upload_var.get()
        }

        self.start_processing_callback(params)

    def update_status(self, message, append=True):
        self.status_text.config(state=tk.NORMAL)
        if append:
            self.status_text.insert(tk.END, message + "\n")
            self.status_text.see(tk.END)  # Scroll to the bottom
        else:
            self.status_text.delete(1.0, tk.END)
            self.status_text.insert(tk.END, message)
        self.status_text.config(state=tk.DISABLED)
        self.window.update_idletasks()

    def update_progress(self, value):
        self.progress_var.set(value)
        self.window.update_idletasks()

    def run(self):
        self.window.mainloop()