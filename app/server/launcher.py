import os
import time
import yaml
import threading
import webbrowser

from flask import Flask
from .service import Service
from .routes import routes


def launch():
  print("Server is starting...")
  port = _load_port()
  thread = threading.Thread(target=lambda: _launch_browser(port))
  thread.start()

  print(f"Server is running on http://0.0.0.0:{port}")
  app = Flask(__name__)
  app_dir = os.path.join(__file__, "..", "..", "..", "data")
  app_dir = os.path.abspath(app_dir)
  service = Service(app_dir)
  routes(app, service)
  app.run(host="0.0.0.0", port=port)
  thread.join()

def _load_port():
  path = os.path.abspath(__file__)
  path = os.path.join(path, "..", "..", "..", "config.yaml")
  path = os.path.abspath(path)

  with open(path, "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)
    return config["port"]

def _launch_browser(port: int):
  time.sleep(0.85)
  webbrowser.open(f"http://localhost:{port}/query")