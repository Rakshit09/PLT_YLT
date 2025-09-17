import threading
import webbrowser
import tkinter as tk
from waitress import serve
from app import app

class SplashScreen:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Loading")
        
        width, height = 300, 100
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        x = (screen_width / 2) - (width / 2)
        y = (screen_height / 2) - (height / 2)
        self.root.geometry(f'{width}x{height}+{int(x)}+{int(y)}')
        
        self.root.overrideredirect(True)
        
        label = tk.Label(self.root, text="Starting Application...", font=("Helvetica", 14))
        label.pack(pady=20, padx=20)
        
        self.root.update()

    def close(self):
        self.root.destroy()

def run_app():
    """Starts the Waitress server."""
    print("Starting server at http://127.0.0.1:8100")
    serve(app, host='127.0.0.1', port=8100)

if __name__ == '__main__':
    splash = SplashScreen()

    # Start the Flask app in a background thread (NOT daemon)
    flask_thread = threading.Thread(target=run_app)
    flask_thread.daemon = False  # Important: NOT a daemon thread
    flask_thread.start()

    # Open browser and close splash after 2 seconds
    def open_and_close():
        webbrowser.open("http://127.0.0.1:8100")
        splash.close()

    splash.root.after(2000, open_and_close)
    
    # Run the splash screen event loop
    splash.root.mainloop()
    
    # Keep the main thread alive
    try:
        flask_thread.join()  # Wait for the Flask thread to finish (it won't unless killed)
    except KeyboardInterrupt:
        print("Server stopped by user")