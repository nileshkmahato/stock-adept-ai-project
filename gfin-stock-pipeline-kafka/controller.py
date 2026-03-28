"""
=============================================================================
Kafka Pipeline Controller & Monitoring Dashboard
=============================================================================
This script serves as the central control plane and web dashboard for the 
GFIN stock data Kafka pipeline. Built with Flask, it manages the lifecycle 
of the underlying data streaming components and provides a real-time UI.

Key Features:
1. Process Management: Automatically orchestrates the startup and shutdown 
   of the Kafka/Zookeeper Docker containers, the Python Kafka Producer, 
   and the Python Kafka Consumer in the correct sequence.
2. Real-time Log Streaming: Captures stdout/stderr from the background 
   processes (Docker, Producer, Consumer) and serves them incrementally 
   via a cursor-based API.
3. Web UI: Hosts a responsive, single-page HTML/JS dashboard at the root 
   ("/") endpoint to start/stop the pipeline, view container health, and 
   monitor real-time logs.
4. Resource Cleanup: Ensures graceful termination of all child processes 
   and containers when the controller is stopped via API or system signals.
=============================================================================
"""

import os
import signal
import atexit
import time
import json
import subprocess
from pathlib import Path
from typing import Tuple, Optional
from threading import Thread
from flask import Flask, jsonify, render_template_string, request

# -----------------------------------------------------------------------------
# Config (env-agnostic)
# -----------------------------------------------------------------------------
BASE_DIR = Path(os.getenv("BASE_DIR", "/home/your-username/CODE")).resolve()
LOG_DIR = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs"))).resolve()
PORT = int(os.getenv("PORT", "8081"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

LOG_DIR.mkdir(parents=True, exist_ok=True)

DOCKER_LOG = LOG_DIR / "docker.log"
PRODUCER_LOG = LOG_DIR / "producer.log"
CONSUMER_LOG = LOG_DIR / "consumer.log"

# Keep references for cleanup
processes = []
start_timestamp: Optional[float] = None

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def run_bg(cmd, logfile: Path) -> subprocess.Popen:
    """Run command in background, redirecting stdout+stderr to logfile."""
    logfile.parent.mkdir(parents=True, exist_ok=True)
    # Open with 'a' for append, but we truncate in the start route
    f = open(logfile, "a", encoding="utf-8")
    p = subprocess.Popen(
        cmd,
        cwd=str(BASE_DIR),
        stdout=f,
        stderr=subprocess.STDOUT,
        start_new_session=True
    )
    processes.append((p, f))
    return p

def read_chunk_from_cursor(path: Path, cursor: Optional[int], limit_bytes: int = 64 * 1024) -> Tuple[str, int, bool]:
    if not path.exists():
        return "No logs yet.", 0, True
    size = path.stat().st_size
    if size == 0:
        return "", 0, True
    if cursor is None or cursor < 0 or cursor > size:
        start = max(0, size - limit_bytes)
    else:
        start = cursor
    with open(path, "rb") as f:
        f.seek(start)
        chunk = f.read(limit_bytes)
        text = chunk.decode(errors="ignore")
        new_cursor = start + len(chunk)
        at_end = new_cursor >= size
        return text, new_cursor, at_end

def docker_compose_available() -> bool:
    try:
        subprocess.run(["docker", "compose", "version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except Exception:
        return False

def docker_status() -> dict:
    if not docker_compose_available():
        return {"available": False, "services": [], "message": "Docker Compose not available"}
    try:
        ps_json = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True
        )
        if ps_json.returncode == 0 and ps_json.stdout.strip():
            # Handle potential multiple JSON objects (NDJSON)
            lines = ps_json.stdout.strip().splitlines()
            services = [json.loads(l) for l in lines]
            normalized = [
                {
                    "name": s.get("Name") or s.get("Service") or "unknown",
                    "state": s.get("State") or s.get("Status") or "unknown",
                    "ports": s.get("Publishers") or [],
                } for s in services
            ]
            return {"available": True, "services": normalized}
        return {"available": True, "services": [], "message": "No containers running"}
    except Exception as e:
        return {"available": False, "services": [], "message": f"Error: {e}"}

def stop_all():
    global start_timestamp
    for p, f in processes:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        except:
            try: p.terminate()
            except: pass
        try: f.close()
        except: pass
    processes.clear()
    start_timestamp = None
    if docker_compose_available():
        subprocess.run(["docker", "compose", "down"], cwd=str(BASE_DIR), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def background_startup_sequence():
    """The heavy lifting thread."""
    try:
        if docker_compose_available():
            run_bg(["docker", "compose", "up", "-d"], DOCKER_LOG)
            time.sleep(45) # Wait for Kafka to be ready
            run_bg(["python3", "-m", "producer.main"], PRODUCER_LOG)
            time.sleep(10)
            run_bg(["python3", "-m", "consumer.main"], CONSUMER_LOG)
    except Exception as e:
        with open(DOCKER_LOG, "a") as f:
            f.write(f"\n[ERROR] Startup failed: {str(e)}\n")

# -----------------------------------------------------------------------------
# Flask
# -----------------------------------------------------------------------------
app = Flask(__name__)

@app.route("/")
def index():
    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Kafka Monitoring Dashboard</title>
  <style>
    :root {
      --bg: #f8fafc; --card: #ffffff; --text: #0f172a; --muted: #64748b;
      --accent: #2563eb; --accent-weak: #dbeafe; --success: #16a34a;
      --danger: #dc2626; --warning: #f59e0b; --border: #e2e8f0;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; --round: 12px;
    }
    * { box-sizing: border-box; }
    body { background: var(--bg); color: var(--text); font-family: system-ui, sans-serif; margin: 0; line-height: 1.5; }
    header { padding: 16px 24px; border-bottom: 1px solid var(--border); background: rgba(248,250,252,0.8); position: sticky; top: 0; z-index: 10; }
    .container { padding: 20px; max-width: 1400px; margin: 0 auto; }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: var(--round); padding: 16px; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }
    .row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    button { background: var(--accent); color: white; border: none; border-radius: 10px; padding: 10px 14px; font-weight: 600; cursor: pointer; transition: opacity 0.2s; }
    button:active { opacity: 0.7; }
    button.secondary { background: var(--accent-weak); color: var(--accent); }
    button.danger { background: var(--danger); }
    .pill { display: inline-block; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--border); color: var(--muted); font-size: 11px; margin-right: 4px;}
    .service { display: grid; grid-template-columns: 2fr 1fr 2fr; gap: 8px; padding: 10px; border: 1px dashed var(--border); border-radius: 10px; margin-bottom: 8px; }
    .state-running, .state-up { color: var(--success); font-weight: 600; }
    .state-exited, .state-down { color: var(--danger); font-weight: 600; }
    .tabs { display: flex; gap: 8px; margin-bottom: 8px; }
    .tab { padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px; cursor: pointer; color: var(--muted); }
    .tab.active { color: var(--accent); border-color: var(--accent); background: var(--accent-weak); font-weight: 600; }
    .log-pane { height: 50vh; overflow: auto; border: 1px solid var(--border); border-radius: var(--round); background: #0b1021; color: #e6e8ee; font-family: var(--mono); font-size: 12px; padding: 12px; }
    pre { margin: 0; white-space: pre-wrap; }
    .timer { font-family: var(--mono); color: var(--accent); font-weight: 700; font-size: 18px; }
    .progress { height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; margin-top: 10px; display: none; }
    .progress div { height: 100%; width: 50%; background: var(--accent); animation: pulse 1.5s infinite linear; }
    @keyframes pulse { from { margin-left: -50%; } to { margin-left: 100%; } }
  </style>
</head>
<body>
  <header>
    <div style="display:flex; justify-content:space-between; align-items:center;">
        <div><h1>Pipeline Monitor</h1><div style="color:var(--muted); font-size:12px;">Real-time Kafka & Scraper Status</div></div>
        <div class="timer" id="elapsed">00:00:00</div>
    </div>
  </header>
  <div class="container">
    <div class="grid">
      <div class="card" style="grid-column: span 4;">
        <h3>Controls</h3>
        <div class="row">
          <button id="startBtn">Start</button>
          <button id="stopBtn" class="danger">Stop</button>
          <button id="refreshBtn" class="secondary">Refresh</button>
        </div>
        <div class="progress" id="loader"><div></div></div>
        <p id="hint" style="font-size:13px; color:var(--muted); margin:10px 0;">System ready.</p>
        <div>
          <span class="pill">DIR: <span id="baseDir">...</span></span>
        </div>
      </div>
      <div class="card" style="grid-column: span 8;">
        <h3>Docker Services</h3>
        <div id="dockerStatus"></div>
      </div>
      <div class="card" style="grid-column: span 12;">
        <div class="tabs">
          <div class="tab active" data-pane="producer">Producer</div>
          <div class="tab" data-pane="consumer">Consumer</div>
          <div class="tab" data-pane="docker">Docker</div>
        </div>
        <div class="log-pane" id="logPane"><pre id="logText">Waiting for logs...</pre></div>
      </div>
    </div>
  </div>
  <script>
    let activePane = "producer", cursors = {}, startTime = null, autoScroll = true;

    async function fetchStatus() {
      const res = await fetch("/api/status");
      const data = await res.json();
      document.getElementById("baseDir").innerText = data.base_dir;
      if (data.started_at && !startTime) {
          startTime = data.started_at * 1000;
      } else if (!data.started_at) {
          startTime = null;
          document.getElementById("elapsed").innerText = "00:00:00";
      }
      const container = document.getElementById("dockerStatus");
      container.innerHTML = "";
      if (data.docker.services.length === 0) container.innerHTML = "<p style='color:var(--muted)'>No services active.</p>";
      data.docker.services.forEach(s => {
        const div = document.createElement("div");
        div.className = "service";
        const stateStyle = s.state.toLowerCase().includes("up") || s.state.toLowerCase().includes("running") ? "state-up" : "state-exited";
        div.innerHTML = `<div><b>${s.name}</b></div><div class="${stateStyle}">${s.state}</div><div style="font-size:11px; color:var(--muted)">${JSON.stringify(s.ports)}</div>`;
        container.appendChild(div);
      });
    }

    async function fetchLogs() {
      const res = await fetch(`/api/logs/${activePane}?cursor=${cursors[activePane] || ""}`);
      const data = await res.json();
      const logText = document.getElementById("logText");
      if (!cursors[activePane]) logText.innerText = "";
      if (data.text) {
          logText.innerText += data.text;
          if (autoScroll) document.getElementById("logPane").scrollTop = document.getElementById("logPane").scrollHeight;
      }
      cursors[activePane] = data.cursor;
    }

    document.getElementById("startBtn").onclick = async () => {
      document.getElementById("loader").style.display = "block";
      const res = await fetch("/api/start", {method:"POST"});
      const data = await res.json();
      document.getElementById("hint").innerText = data.status;
      startTime = Date.now();
      cursors = {}; // Clear log view
      setTimeout(() => document.getElementById("loader").style.display = "none", 2000);
    };

    document.getElementById("stopBtn").onclick = async () => {
      await fetch("/api/stop", {method:"POST"});
      startTime = null;
      document.getElementById("hint").innerText = "Stopped.";
      fetchStatus();
    };

    document.getElementById("refreshBtn").onclick = fetchStatus;

    document.querySelectorAll(".tab").forEach(t => {
      t.onclick = () => {
        document.querySelectorAll(".tab").forEach(tab => tab.classList.remove("active"));
        t.classList.add("active");
        activePane = t.dataset.pane;
        document.getElementById("logText").innerText = "Switching...";
        cursors[activePane] = null;
        fetchLogs();
      };
    });

    setInterval(() => {
      fetchLogs();
      if (startTime) {
        const diff = Math.floor((Date.now() - startTime) / 1000);
        const h = String(Math.floor(diff / 3600)).padStart(2, '0');
        const m = String(Math.floor((diff % 3600) / 60)).padStart(2, '0');
        const s = String(diff % 60).padStart(2, '0');
        document.getElementById("elapsed").innerText = `${h}:${m}:${s}`;
      }
    }, 2000);
    setInterval(fetchStatus, 5000);
    fetchStatus();
  </script>
</body>
</html>
""")

@app.route("/api/start", methods=["POST"])
def api_start():
    global start_timestamp
    # 1. STOP existing if running to clean state
    stop_all()
    
    # 2. CLEAR LOGS (Truncate files)
    for log_file in [DOCKER_LOG, PRODUCER_LOG, CONSUMER_LOG]:
        open(log_file, 'w').close()

    # 3. Mark start time
    start_timestamp = time.time()
    
    # 4. START sequence in background
    Thread(target=background_startup_sequence, daemon=True).start()
    
    return jsonify({"status": "Pipeline starting in background..."})

@app.route("/api/stop", methods=["POST"])
def api_stop():
    stop_all()
    return jsonify({"status": "All stopped."})

@app.route("/api/status")
def api_status():
    return jsonify({
        "base_dir": str(BASE_DIR),
        "docker": docker_status(),
        "started_at": start_timestamp
    })

@app.route("/api/logs/<name>")
def api_logs(name: str):
    path_map = {"docker": DOCKER_LOG, "producer": PRODUCER_LOG, "consumer": CONSUMER_LOG}
    path = path_map.get(name)
    if not path or not path.exists():
        return jsonify({"text": "", "cursor": 0})
    
    q = request.args.get("cursor")
    cursor = int(q) if q and q != "null" else None
    text, new_cursor, _ = read_chunk_from_cursor(path, cursor)
    return jsonify({"text": text, "cursor": new_cursor})

@app.route('/health')
def health():
    return jsonify({"status": "ready"}), 200

def cleanup():
    stop_all()

atexit.register(cleanup)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=DEBUG)