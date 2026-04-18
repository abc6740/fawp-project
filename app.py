from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

# ──────────────────────────────────────────────
# App setup
# ──────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

# ──────────────────────────────────────────────
# Root route
# ──────────────────────────────────────────────
@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "message": "FAWP backend is live 🚀"
    })

# ──────────────────────────────────────────────
# Database setup
# ──────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "fawp.db")


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS farmers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            village TEXT,
            state TEXT,
            land_acres REAL,
            annual_income REAL,
            age INTEGER,
            category TEXT,
            irrigated INTEGER,
            bpl INTEGER,
            has_loan INTEGER
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS farmer_crops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            farmer_id INTEGER,
            crop TEXT
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS schemes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scheme_id TEXT,
            category TEXT,
            level TEXT,
            eligible_categories TEXT,
            eligible_states TEXT,
            irrigated_required INTEGER,
            max_land REAL,
            min_land REAL,
            bpl_only INTEGER
        )
        """)

# create DB automatically
init_db()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def query(sql, params=()):
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def execute(sql, params=()):
    with get_db() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid


# ──────────────────────────────────────────────
# FARMERS API
# ──────────────────────────────────────────────

@app.route("/api/farmers", methods=["GET"])
def list_farmers():
    sql = """
    SELECT f.*, GROUP_CONCAT(fc.crop) as crops
    FROM farmers f
    LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id
    """
    conditions, params = [], []

    state = request.args.get("state")
    bpl = request.args.get("bpl")
    size = request.args.get("size")
    crop = request.args.get("crop")

    if state:
        conditions.append("f.state = ?")
        params.append(state)

    if bpl:
        conditions.append("f.bpl = ?")
        params.append(1 if bpl.lower() == "true" else 0)

    if size == "small":
        conditions.append("f.land_acres <= 2")
    elif size == "medium":
        conditions.append("f.land_acres > 2 AND f.land_acres <= 5")
    elif size == "large":
        conditions.append("f.land_acres > 5")

    if crop:
        conditions.append("f.id IN (SELECT farmer_id FROM farmer_crops WHERE crop = ?)")
        params.append(crop)

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " GROUP BY f.id"

    rows = query(sql, params)

    for r in rows:
        r["crops"] = r["crops"].split(",") if r["crops"] else []
        r["irrigated"] = bool(r["irrigated"])
        r["bpl"] = bool(r["bpl"])
        r["has_loan"] = bool(r["has_loan"])

    return jsonify(rows)


@app.route("/api/farmers/<int:fid>", methods=["GET"])
def get_farmer(fid):
    rows = query("""
        SELECT f.*, GROUP_CONCAT(fc.crop) as crops
        FROM farmers f
        LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id
        WHERE f.id = ?
        GROUP BY f.id
    """, (fid,))

    if not rows:
        return jsonify({"error": "Farmer not found"}), 404

    r = rows[0]
    r["crops"] = r["crops"].split(",") if r["crops"] else []
    r["irrigated"] = bool(r["irrigated"])
    r["bpl"] = bool(r["bpl"])
    r["has_loan"] = bool(r["has_loan"])

    return jsonify(r)


@app.route("/api/farmers", methods=["POST"])
def create_farmer():
    data = request.get_json()

    required = ["name", "village", "state", "land_acres",
                "annual_income", "age", "category"]

    for f in required:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400

    fid = execute("""
        INSERT INTO farmers
        (name, village, state, land_acres, annual_income, age, category, irrigated, bpl, has_loan)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["name"], data["village"], data["state"], data["land_acres"],
        data["annual_income"], data["age"], data["category"],
        int(data.get("irrigated", False)),
        int(data.get("bpl", False)),
        int(data.get("has_loan", False))
    ))

    for crop in data.get("crops", []):
        execute("INSERT INTO farmer_crops (farmer_id, crop) VALUES (?, ?)", (fid, crop))

    return jsonify({"id": fid, "message": "Farmer created"}), 201


@app.route("/api/farmers/<int:fid>", methods=["DELETE"])
def delete_farmer(fid):
    execute("DELETE FROM farmer_crops WHERE farmer_id = ?", (fid,))
    execute("DELETE FROM farmers WHERE id = ?", (fid,))
    return jsonify({"message": "Farmer deleted"})


# ──────────────────────────────────────────────
# SCHEMES API
# ──────────────────────────────────────────────

@app.route("/api/schemes", methods=["GET"])
def list_schemes():
    sql = "SELECT * FROM schemes"
    conditions, params = [], []

    if request.args.get("category"):
        conditions.append("category = ?")
        params.append(request.args["category"])

    if request.args.get("level"):
        conditions.append("level = ?")
        params.append(request.args["level"])

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    rows = query(sql, params)

    for r in rows:
        r["eligible_categories"] = r["eligible_categories"].split(",") if r["eligible_categories"] else []
        r["eligible_states"] = r["eligible_states"].split(",") if r["eligible_states"] else []
        r["irrigated_required"] = None if r["irrigated_required"] is None else bool(r["irrigated_required"])

    return jsonify(rows)


# ──────────────────────────────────────────────
# ENTRY POINT (RENDER SAFE)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
