import threading
import tkinter as tk
import sys
import os
import webbrowser
import atexit

def create_splash():
    """Create splash screen with minimal imports"""
    root = tk.Tk()
    root.title("Loading")
    
    width, height = 300, 120
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width / 2) - (width / 2)
    y = (screen_height / 2) - (height / 2)
    root.geometry(f'{width}x{height}+{int(x)}+{int(y)}')
    
    root.overrideredirect(True)
    root.configure(bg='white')
    
    frame = tk.Frame(root, bg='white')
    frame.pack(expand=True, fill='both', padx=20, pady=20)
    
    status_label = tk.Label(frame, text="Starting Application...", 
                           font=("Helvetica", 14), bg='white')
    status_label.pack(pady=5)
    
    progress_label = tk.Label(frame, text="Loading modules...", 
                             font=("Helvetica", 10), fg='gray', bg='white')
    progress_label.pack(pady=5)
    
    root.update_idletasks()
    root.update()
    
    return root, status_label, progress_label

def create_control_window():
    """Create a small control window to stop the server"""
    control = tk.Tk()
    control.title("Server Control")
    
    width, height = 280, 120
    screen_width = control.winfo_screenwidth()
    screen_height = control.winfo_screenheight()
    x = screen_width - width - 20
    y = screen_height - height - 70
    control.geometry(f'{width}x{height}+{int(x)}+{int(y)}')
    
    control.configure(bg='#2c3e50')
    control.attributes('-topmost', False)
    
    frame = tk.Frame(control, bg='#2c3e50')
    frame.pack(expand=True, fill='both', padx=15, pady=15)
    
    label = tk.Label(frame, text="🟢 Server Running", 
                     font=("Helvetica", 11, "bold"), 
                     bg='#2c3e50', fg='#2ecc71')
    label.pack(pady=5)
    
    url_label = tk.Label(frame, text="http://127.0.0.1:8100", 
                         font=("Helvetica", 9), 
                         fg='#3498db', bg='#2c3e50',
                         cursor='hand2')
    url_label.pack(pady=2)
    
    # Make URL clickable
    url_label.bind('<Button-1>', lambda e: webbrowser.open("http://127.0.0.1:8100"))
    
    return control, label

if __name__ == '__main__':
    splash_root, status_label, progress_label = create_splash()
    
    def update_progress(text):
        try:
            progress_label.config(text=text)
            splash_root.update()
        except:
            pass
    
    update_progress("Loading server...")
    from waitress import serve
    
    update_progress("Loading application...")
    from app import app
    
    update_progress("Initializing server...")
    
    server_shutdown = threading.Event()
    
    def cleanup():
        """Force cleanup on exit"""
        print("\nShutting down...")
        server_shutdown.set()
        os._exit(0)  # Force immediate exit
    
    def run_app():
        """Starts the Waitress server."""
        print("Starting server at http://127.0.0.1:8100")
        try:
            serve(app, host='127.0.0.1', port=8100, _quiet=False)
        except Exception as e:
            print(f"Server stopped: {e}")
    
    # Daemon thread so it closes with main program
    server_thread = threading.Thread(target=run_app, daemon=True)
    server_thread.start()
    
    update_progress("Opening browser...")
    
    # Create control window
    control_window, status_label = create_control_window()
    
    # Add buttons
    btn_frame = tk.Frame(control_window.children['!frame'], bg='#2c3e50')
    btn_frame.pack(pady=8)
    
    quit_btn = tk.Button(btn_frame, 
                         text="⏹ Stop Server", 
                         command=cleanup,
                         bg='#e74c3c', fg='white',
                         font=("Helvetica", 9, "bold"),
                         relief='flat',
                         cursor='hand2',
                         padx=15, pady=5)
    quit_btn.pack()
    
    def open_browser_and_show_control():
        webbrowser.open("http://127.0.0.1:8100")
        splash_root.destroy()
        control_window.deiconify()
    
    # Handle window close
    control_window.protocol("WM_DELETE_WINDOW", cleanup)
    
    # Hide control window initially
    control_window.withdraw()
    
    splash_root.after(800, open_browser_and_show_control)
    splash_root.mainloop()
    
    # Run control window event loop
    try:
        control_window.mainloop()
    except:
        pass
    
    cleanup()