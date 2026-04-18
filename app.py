from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

app = Flask(__name__)
CORS(app)

DB_PATH = os.path.join(os.path.dirname(__file__), "fawp.db")


# ──────────────────────────────────────────────
# DB helper
# ──────────────────────────────────────────────
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
# ── FARMERS ──────────────────────────────────
# ──────────────────────────────────────────────

@app.route("/api/farmers", methods=["GET"])
def list_farmers():
    """
    GET /api/farmers
    Query params: state, crop, size (small|medium|large), bpl (true|false)
    """
    sql = "SELECT f.*, GROUP_CONCAT(fc.crop) as crops FROM farmers f LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id"
    conditions, params = [], []

    state = request.args.get("state")
    bpl   = request.args.get("bpl")
    size  = request.args.get("size")   # small ≤2, medium 2-5, large >5
    crop  = request.args.get("crop")

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
        r["bpl"]       = bool(r["bpl"])
        r["has_loan"]  = bool(r["has_loan"])
    return jsonify(rows)


@app.route("/api/farmers/<int:fid>", methods=["GET"])
def get_farmer(fid):
    rows = query(
        "SELECT f.*, GROUP_CONCAT(fc.crop) as crops FROM farmers f "
        "LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id WHERE f.id = ? GROUP BY f.id",
        (fid,)
    )
    if not rows:
        return jsonify({"error": "Farmer not found"}), 404
    r = rows[0]
    r["crops"]    = r["crops"].split(",") if r["crops"] else []
    r["irrigated"] = bool(r["irrigated"])
    r["bpl"]       = bool(r["bpl"])
    r["has_loan"]  = bool(r["has_loan"])
    return jsonify(r)


@app.route("/api/farmers", methods=["POST"])
def create_farmer():
    data = request.get_json()
    required = ["name", "village", "state", "land_acres", "annual_income", "age", "category"]
    for f in required:
        if f not in data:
            return jsonify({"error": f"Missing field: {f}"}), 400

    fid = execute(
        "INSERT INTO farmers (name, village, state, land_acres, annual_income, age, category, irrigated, bpl, has_loan) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (data["name"], data["village"], data["state"], data["land_acres"],
         data["annual_income"], data["age"], data["category"],
         int(data.get("irrigated", False)),
         int(data.get("bpl", False)),
         int(data.get("has_loan", False)))
    )
    for crop in data.get("crops", []):
        execute("INSERT INTO farmer_crops (farmer_id, crop) VALUES (?, ?)", (fid, crop))

    return jsonify({"id": fid, "message": "Farmer created"}), 201


@app.route("/api/farmers/<int:fid>", methods=["PUT"])
def update_farmer(fid):
    data = request.get_json()
    fields = ["name", "village", "state", "land_acres", "annual_income", "age",
              "category", "irrigated", "bpl", "has_loan"]
    updates = {k: data[k] for k in fields if k in data}
    if not updates:
        return jsonify({"error": "No fields to update"}), 400

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    execute(f"UPDATE farmers SET {set_clause} WHERE id = ?", list(updates.values()) + [fid])

    if "crops" in data:
        execute("DELETE FROM farmer_crops WHERE farmer_id = ?", (fid,))
        for crop in data["crops"]:
            execute("INSERT INTO farmer_crops (farmer_id, crop) VALUES (?, ?)", (fid, crop))

    return jsonify({"message": "Farmer updated"})


@app.route("/api/farmers/<int:fid>", methods=["DELETE"])
def delete_farmer(fid):
    execute("DELETE FROM farmer_crops WHERE farmer_id = ?", (fid,))
    execute("DELETE FROM farmers WHERE id = ?", (fid,))
    return jsonify({"message": "Farmer deleted"})


# ──────────────────────────────────────────────
# ── SCHEMES ───────────────────────────────────
# ──────────────────────────────────────────────

@app.route("/api/schemes", methods=["GET"])
def list_schemes():
    """GET /api/schemes?category=&level="""
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
        r["eligible_states"]     = r["eligible_states"].split(",")     if r["eligible_states"]     else []
        r["irrigated_required"]  = None if r["irrigated_required"] is None else bool(r["irrigated_required"])
    return jsonify(rows)


@app.route("/api/schemes/<scheme_id>", methods=["GET"])
def get_scheme(scheme_id):
    rows = query("SELECT * FROM schemes WHERE scheme_id = ?", (scheme_id,))
    if not rows:
        return jsonify({"error": "Scheme not found"}), 404
    r = rows[0]
    r["eligible_categories"] = r["eligible_categories"].split(",") if r["eligible_categories"] else []
    r["eligible_states"]     = r["eligible_states"].split(",")     if r["eligible_states"]     else []
    r["irrigated_required"]  = None if r["irrigated_required"] is None else bool(r["irrigated_required"])
    return jsonify(r)


# ──────────────────────────────────────────────
# ── SCHEME MATCHER ────────────────────────────
# ──────────────────────────────────────────────

def is_eligible(farmer, scheme):
    """Core eligibility engine — pure Python, no ML, no external API."""
    cats = scheme["eligible_categories"]
    if cats and farmer["category"] not in cats:
        return False, "Category not eligible"

    states = scheme["eligible_states"]
    if states and farmer["state"] not in states:
        return False, f"Scheme available only in {', '.join(states)}"

    if scheme["max_land"] and farmer["land_acres"] > scheme["max_land"]:
        return False, f"Land exceeds limit of {scheme['max_land']} acres"

    if scheme["min_land"] and farmer["land_acres"] < scheme["min_land"]:
        return False, f"Minimum land required: {scheme['min_land']} acres"

    if scheme["bpl_only"] and not farmer["bpl"]:
        return False, "BPL status required"

    irr = scheme["irrigated_required"]
    if irr is True and not farmer["irrigated"]:
        return False, "Irrigated land required"
    if irr is False and farmer["irrigated"]:
        return False, "Scheme is for un-irrigated land only"

    return True, "Eligible"


@app.route("/api/match/<int:farmer_id>", methods=["GET"])
def match_farmer(farmer_id):
    """
    GET /api/match/<farmer_id>
    Returns all schemes split into eligible / not_eligible for this farmer.
    """
    farmer_rows = query(
        "SELECT f.*, GROUP_CONCAT(fc.crop) as crops FROM farmers f "
        "LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id WHERE f.id = ? GROUP BY f.id",
        (farmer_id,)
    )
    if not farmer_rows:
        return jsonify({"error": "Farmer not found"}), 404

    farmer = farmer_rows[0]
    farmer["crops"]    = farmer["crops"].split(",") if farmer["crops"] else []
    farmer["irrigated"] = bool(farmer["irrigated"])
    farmer["bpl"]       = bool(farmer["bpl"])
    farmer["has_loan"]  = bool(farmer["has_loan"])

    all_schemes = query("SELECT * FROM schemes")
    eligible, not_eligible = [], []

    for s in all_schemes:
        s["eligible_categories"] = s["eligible_categories"].split(",") if s["eligible_categories"] else []
        s["eligible_states"]     = s["eligible_states"].split(",")     if s["eligible_states"]     else []
        s["irrigated_required"]  = None if s["irrigated_required"] is None else bool(s["irrigated_required"])

        ok, reason = is_eligible(farmer, s)
        entry = {**s, "reason": reason}
        (eligible if ok else not_eligible).append(entry)

    return jsonify({
        "farmer": farmer,
        "summary": {
            "total_schemes": len(all_schemes),
            "eligible_count": len(eligible),
            "not_eligible_count": len(not_eligible)
        },
        "eligible": eligible,
        "not_eligible": not_eligible
    })


@app.route("/api/match/scheme/<scheme_id>", methods=["GET"])
def match_scheme(scheme_id):
    """
    GET /api/match/scheme/<scheme_id>
    Returns all farmers eligible for a given scheme.
    """
    scheme_rows = query("SELECT * FROM schemes WHERE scheme_id = ?", (scheme_id,))
    if not scheme_rows:
        return jsonify({"error": "Scheme not found"}), 404

    s = scheme_rows[0]
    s["eligible_categories"] = s["eligible_categories"].split(",") if s["eligible_categories"] else []
    s["eligible_states"]     = s["eligible_states"].split(",")     if s["eligible_states"]     else []
    s["irrigated_required"]  = None if s["irrigated_required"] is None else bool(s["irrigated_required"])

    all_farmers = query(
        "SELECT f.*, GROUP_CONCAT(fc.crop) as crops FROM farmers f "
        "LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id GROUP BY f.id"
    )
    eligible, not_eligible = [], []
    for f in all_farmers:
        f["crops"]    = f["crops"].split(",") if f["crops"] else []
        f["irrigated"] = bool(f["irrigated"])
        f["bpl"]       = bool(f["bpl"])
        f["has_loan"]  = bool(f["has_loan"])
        ok, reason = is_eligible(f, s)
        entry = {**f, "reason": reason}
        (eligible if ok else not_eligible).append(entry)

    return jsonify({
        "scheme": s,
        "summary": {
            "total_farmers": len(all_farmers),
            "eligible_count": len(eligible)
        },
        "eligible_farmers": eligible,
        "not_eligible_farmers": not_eligible
    })


# ──────────────────────────────────────────────
# ── STATS ─────────────────────────────────────
# ──────────────────────────────────────────────

@app.route("/api/stats", methods=["GET"])
def stats():
    total_farmers  = query("SELECT COUNT(*) as c FROM farmers")[0]["c"]
    total_schemes  = query("SELECT COUNT(*) as c FROM schemes")[0]["c"]
    states         = query("SELECT COUNT(DISTINCT state) as c FROM farmers")[0]["c"]
    avg_land       = query("SELECT ROUND(AVG(land_acres),2) as c FROM farmers")[0]["c"]
    small_farmers  = query("SELECT COUNT(*) as c FROM farmers WHERE land_acres <= 2")[0]["c"]
    bpl_farmers    = query("SELECT COUNT(*) as c FROM farmers WHERE bpl = 1")[0]["c"]

    crop_dist = query(
        "SELECT crop, COUNT(*) as count FROM farmer_crops GROUP BY crop ORDER BY count DESC"
    )
    scheme_cats = query(
        "SELECT category, COUNT(*) as count FROM schemes GROUP BY category"
    )

    total_matches = 0
    all_farmers = query(
        "SELECT f.*, GROUP_CONCAT(fc.crop) as crops FROM farmers f "
        "LEFT JOIN farmer_crops fc ON f.id = fc.farmer_id GROUP BY f.id"
    )
    all_schemes = query("SELECT * FROM schemes")
    for f in all_farmers:
        f["irrigated"] = bool(f["irrigated"])
        f["bpl"]       = bool(f["bpl"])
        f["has_loan"]  = bool(f["has_loan"])
        for s in all_schemes:
            s2 = dict(s)
            s2["eligible_categories"] = s2["eligible_categories"].split(",") if s2["eligible_categories"] else []
            s2["eligible_states"]     = s2["eligible_states"].split(",")     if s2["eligible_states"]     else []
            s2["irrigated_required"]  = None if s2["irrigated_required"] is None else bool(s2["irrigated_required"])
            ok, _ = is_eligible(f, s2)
            if ok:
                total_matches += 1

    return jsonify({
        "total_farmers":  total_farmers,
        "total_schemes":  total_schemes,
        "total_states":   states,
        "avg_land_acres": avg_land,
        "small_farmers":  small_farmers,
        "bpl_farmers":    bpl_farmers,
        "total_matches":  total_matches,
        "crop_distribution":   crop_dist,
        "scheme_categories":   scheme_cats,
    })


app = Flask(__name__)
CORS(app)

from database import init_db, seed_db
init_db()
seed_db()

# ... all your routes ...

if __name__ == "__main__":
    print("FAWP backend running on http://localhost:5000")
    app.run(debug=True, port=5000)
