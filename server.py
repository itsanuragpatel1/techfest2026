"""
Flask + SQLite Telemetry Server  v2.0
ESP32 (INA219 + Potentiometer) → Flask API → SQLite DB → Dashboard

Install:  pip install flask flask-cors
Run:      python server.py
Dashboard: http://localhost:8000

DB file:  telemetry.db  (auto-created on first run)

POST /api/telemetry  ← ESP32 यहाँ data भेजता है
GET  /api/latest     ← latest 1 record
GET  /api/history    ← last N records + stats
GET  /api/track      ← position + speed only (lightweight)
GET  /api/stats      ← session summary + energy
GET  /api/devices    ← all registered devices
GET  /api/export     ← CSV download
DELETE /api/clear    ← data clear
GET  /health         ← server health check
"""

from flask import Flask, request, jsonify, send_file, g, Response
from flask_cors import CORS
from datetime import datetime
import sqlite3
import time
import os
import csv
import io

# ─── CONFIG ──────────────────────────────────────────────────
HOST    = "0.0.0.0"
PORT    = 8000
DB_PATH = os.path.join(os.path.dirname(__file__), "telemetry.db")

app = Flask(__name__)
CORS(app)

# ─── DATABASE ─────────────────────────────────────────────────

def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA synchronous=NORMAL")
    return g.db


@app.teardown_appcontext
def close_db(exc=None):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id      TEXT    NOT NULL,
                server_time    TEXT    NOT NULL,
                server_unix    REAL    NOT NULL,
                uptime_sec     INTEGER DEFAULT 0,

                -- Motion
                speed_pct      REAL    NOT NULL,
                speed_ups      REAL    NOT NULL,
                pos_x          REAL    NOT NULL,
                pos_y          REAL    NOT NULL DEFAULT 0,
                pos_z          REAL    NOT NULL DEFAULT 0,
                total_distance REAL    NOT NULL,

                -- Power (from INA219)
                voltage_V      REAL    NOT NULL,
                current_mA     REAL    NOT NULL,
                current_A      REAL    NOT NULL,
                power_mW       REAL    NOT NULL,
                power_W        REAL    NOT NULL,
                drawn_mW       REAL    NOT NULL DEFAULT 0,
                generated_mW   REAL    NOT NULL DEFAULT 0,
                samples        INTEGER NOT NULL DEFAULT 25,
                interval_sec   INTEGER NOT NULL DEFAULT 5
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_device_time
            ON telemetry (device_id, server_unix DESC)
        """)
        db.commit()
    print(f"[DB] SQLite ready → {DB_PATH}")


def row_to_dict(row):
    return dict(row)


# ─── HELPERS ──────────────────────────────────────────────────

def query(sql, params=(), one=False):
    cur = get_db().execute(sql, params)
    rows = cur.fetchall()
    result = [row_to_dict(r) for r in rows]
    return result[0] if (one and result) else (None if one else result)


def compute_stats(rows):
    if not rows:
        return {}
    n = len(rows)
    return {
        "speed_avg_pct":  round(sum(r["speed_pct"]  for r in rows) / n, 2),
        "speed_max_pct":  round(max(r["speed_pct"]  for r in rows), 2),
        "power_avg_mW":   round(sum(r["power_mW"]   for r in rows) / n, 2),
        "power_max_mW":   round(max(r["power_mW"]   for r in rows), 2),
        "voltage_avg_V":  round(sum(r["voltage_V"]  for r in rows) / n, 3),
        "pos_x_latest":   round(rows[-1]["pos_x"], 3),
        "pos_x_range":    round(
            max(r["pos_x"] for r in rows) - min(r["pos_x"] for r in rows), 3
        ),
    }


def safe_float(data, key, default=0.0):
    """Safely extract float from dict, return default if missing/invalid."""
    try:
        return float(data.get(key, default))
    except (TypeError, ValueError):
        return default


# ─── ROUTES ───────────────────────────────────────────────────

@app.post("/api/telemetry")
def receive_telemetry():
    """
    ESP32 POSTs JSON here every interval.

    Required body:
    {
      "device_id"     : "esp32-tracker-01",
      "uptime_sec"    : 120,                     ← optional (default 0)
      "speed_pct"     : 73.4,                    ← 0–100
      "speed_ups"     : 7.34,                    ← speed_pct / 10
      "position"      : { "x": 182.5, "y": 0, "z": 0 },
      "total_distance": 182.5,
      "power": {
        "voltage_V"    : 3.712,
        "current_mA"   : 245.6,
        "current_A"    : 0.2456,
        "power_mW"     : 912.5,
        "power_W"      : 0.9125,
        "drawn_mW"     : 912.5,
        "generated_mW" : 0.0,
        "samples"      : 25,
        "interval_sec" : 5
      }
    }
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON"}), 400

    # Required fields validation
    required = ["device_id", "speed_pct", "speed_ups", "position", "total_distance", "power"]
    missing = [k for k in required if k not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    pos = data["position"]
    pwr = data["power"]

    # Required power fields
    required_pwr = ["voltage_V", "current_mA", "current_A", "power_mW", "power_W"]
    missing_pwr = [k for k in required_pwr if k not in pwr]
    if missing_pwr:
        return jsonify({"error": f"Missing power fields: {missing_pwr}"}), 400

    now         = datetime.now()
    server_time = now.isoformat(timespec="milliseconds")
    server_unix = time.time()

    db = get_db()
    db.execute("""
        INSERT INTO telemetry (
            device_id, server_time, server_unix, uptime_sec,
            speed_pct, speed_ups,
            pos_x, pos_y, pos_z, total_distance,
            voltage_V, current_mA, current_A,
            power_mW, power_W, drawn_mW, generated_mW,
            samples, interval_sec
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        data["device_id"],
        server_time,
        server_unix,
        data.get("uptime_sec", 0),
        float(data["speed_pct"]),
        float(data["speed_ups"]),
        safe_float(pos, "x"),
        safe_float(pos, "y"),
        safe_float(pos, "z"),
        float(data["total_distance"]),
        float(pwr["voltage_V"]),
        float(pwr["current_mA"]),
        float(pwr["current_A"]),
        float(pwr["power_mW"]),
        float(pwr["power_W"]),
        safe_float(pwr, "drawn_mW"),
        safe_float(pwr, "generated_mW"),
        int(pwr.get("samples", 25)),
        int(pwr.get("interval_sec", 5)),
    ))
    db.commit()

    print(
        f"[{server_time[:19]}] {data['device_id']} | "
        f"Spd={float(data['speed_pct']):.1f}% | "
        f"X={safe_float(pos,'x'):.3f} | "
        f"V={float(pwr['voltage_V']):.3f}V | "
        f"P={float(pwr['power_mW']):.2f}mW"
    )

    return jsonify({"status": "ok", "server_time": server_time}), 201


@app.get("/api/latest")
def get_latest():
    device_id = request.args.get("device_id")
    if device_id:
        row = query(
            "SELECT * FROM telemetry WHERE device_id=? ORDER BY server_unix DESC LIMIT 1",
            (device_id,), one=True
        )
    else:
        row = query(
            "SELECT * FROM telemetry ORDER BY server_unix DESC LIMIT 1",
            one=True
        )

    if not row:
        return jsonify({"error": "No data yet"}), 404

    total = query("SELECT COUNT(*) AS n FROM telemetry", one=True)["n"]
    return jsonify({"latest": row, "total_stored": total})


@app.get("/api/history")
def get_history():
    limit     = min(int(request.args.get("limit", 200)), 1000)  # max 1000
    device_id = request.args.get("device_id")

    if device_id:
        rows = query(
            "SELECT * FROM telemetry WHERE device_id=? ORDER BY server_unix DESC LIMIT ?",
            (device_id, limit)
        )
    else:
        rows = query(
            "SELECT * FROM telemetry ORDER BY server_unix DESC LIMIT ?",
            (limit,)
        )

    rows = list(reversed(rows))
    return jsonify({
        "readings": rows,
        "count":    len(rows),
        "stats":    compute_stats(rows),
    })


@app.get("/api/track")
def get_track():
    """Position + speed only — lightweight for real-time visualization."""
    limit     = min(int(request.args.get("limit", 500)), 2000)
    device_id = request.args.get("device_id")

    if device_id:
        rows = query(
            "SELECT server_unix, pos_x, pos_y, speed_pct FROM telemetry "
            "WHERE device_id=? ORDER BY server_unix DESC LIMIT ?",
            (device_id, limit)
        )
    else:
        rows = query(
            "SELECT server_unix, pos_x, pos_y, speed_pct FROM telemetry "
            "ORDER BY server_unix DESC LIMIT ?",
            (limit,)
        )

    rows = list(reversed(rows))
    track = [{
        "t":   r["server_unix"],
        "x":   r["pos_x"],
        "y":   r["pos_y"],
        "spd": r["speed_pct"]
    } for r in rows]
    return jsonify({"track": track, "points": len(track)})


@app.get("/api/stats")
def get_stats():
    """Full session summary with energy calculation."""
    device_id = request.args.get("device_id")

    if device_id:
        rows = query(
            "SELECT * FROM telemetry WHERE device_id=? ORDER BY server_unix ASC",
            (device_id,)
        )
    else:
        rows = query("SELECT * FROM telemetry ORDER BY server_unix ASC")

    if not rows:
        return jsonify({"error": "No data"}), 404

    n        = len(rows)
    interval = rows[-1]["interval_sec"] or 5
    total_h  = (n * interval) / 3600.0
    energy_mWh = sum(r["power_mW"] for r in rows) * (interval / 3600.0)

    return jsonify({
        "total_packets":      n,
        "total_time_minutes": round(total_h * 60, 2),
        "device_id":          rows[-1]["device_id"],
        "speed": {
            "avg_pct": round(sum(r["speed_pct"] for r in rows) / n, 2),
            "max_pct": round(max(r["speed_pct"] for r in rows), 2),
            "avg_ups": round(sum(r["speed_ups"] for r in rows) / n, 3),
        },
        "position": {
            "current_x":      round(rows[-1]["pos_x"], 3),
            "total_distance": round(rows[-1]["total_distance"], 3),
        },
        "power": {
            "voltage_avg_V":    round(sum(r["voltage_V"]  for r in rows) / n, 3),
            "current_avg_mA":   round(sum(r["current_mA"] for r in rows) / n, 2),
            "power_avg_mW":     round(sum(r["power_mW"]   for r in rows) / n, 2),
            "power_max_mW":     round(max(r["power_mW"]   for r in rows), 2),
            "total_energy_mWh": round(energy_mWh, 4),
            "drawn_total_mWh":  round(
                sum(r["drawn_mW"] for r in rows) * (interval / 3600.0), 4
            ),
        },
        "first_packet": rows[0]["server_time"],
        "last_packet":  rows[-1]["server_time"],
    })


@app.get("/api/devices")
def get_devices():
    """List all devices with last-seen info and packet count."""
    rows = query("""
        SELECT device_id,
               COUNT(*)          AS total_packets,
               MAX(server_time)  AS last_seen,
               MAX(pos_x)        AS max_x,
               MAX(speed_pct)    AS max_speed,
               AVG(voltage_V)    AS avg_voltage,
               AVG(power_mW)     AS avg_power
        FROM telemetry
        GROUP BY device_id
    """)
    devices = {r["device_id"]: r for r in rows}
    return jsonify({"devices": devices, "count": len(devices)})


@app.get("/api/export")
def export_csv():
    """Download all data as CSV."""
    device_id = request.args.get("device_id")

    if device_id:
        rows = query(
            "SELECT * FROM telemetry WHERE device_id=? ORDER BY server_unix ASC",
            (device_id,)
        )
        filename = f"telemetry_{device_id}.csv"
    else:
        rows = query("SELECT * FROM telemetry ORDER BY server_unix ASC")
        filename = "telemetry_all.csv"

    if not rows:
        return jsonify({"error": "No data"}), 404

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

    return Response(
        out.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.delete("/api/clear")
def clear_data():
    """Delete all records (or by device_id)."""
    device_id = request.args.get("device_id")
    db = get_db()
    if device_id:
        db.execute("DELETE FROM telemetry WHERE device_id=?", (device_id,))
        msg = f"Cleared data for '{device_id}'"
    else:
        db.execute("DELETE FROM telemetry")
        msg = "All data cleared"
    db.commit()
    return jsonify({"status": "ok", "message": msg})


@app.get("/health")
def health():
    row = query("SELECT COUNT(*) AS n FROM telemetry", one=True)
    return jsonify({
        "status":        "ok",
        "db":            DB_PATH,
        "total_records": row["n"] if row else 0,
        "server_time":   datetime.now().isoformat(timespec="milliseconds"),
    })


@app.get("/")
def dashboard():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    return send_file(html_path)

print("test")

# ─── MAIN ─────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    print(f"\n[SERVER] Flask running  → http://{HOST}:{PORT}")
    print(f"[SERVER] Dashboard      → http://localhost:{PORT}")
    print(f"[SERVER] API telemetry  → POST http://localhost:{PORT}/api/telemetry")
    print(f"[SERVER] DB path        → {DB_PATH}\n")
    app.run(host=HOST, port=PORT, debug=True)