import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "fawp.db")


# ──────────────────────────────────────────────
# SCHEMA
# ──────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS farmers (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL,
    village        TEXT    NOT NULL,
    state          TEXT    NOT NULL,
    land_acres     REAL    NOT NULL,
    annual_income  INTEGER NOT NULL,
    age            INTEGER NOT NULL,
    category       TEXT    NOT NULL CHECK(category IN ('General','OBC','SC','ST')),
    irrigated      INTEGER NOT NULL DEFAULT 0,   -- 0 / 1
    bpl            INTEGER NOT NULL DEFAULT 0,   -- Below Poverty Line
    has_loan       INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS farmer_crops (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    farmer_id  INTEGER NOT NULL REFERENCES farmers(id),
    crop       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS schemes (
    scheme_id            TEXT    PRIMARY KEY,
    name                 TEXT    NOT NULL,
    full_name            TEXT    NOT NULL,
    category             TEXT    NOT NULL,
    level                TEXT    NOT NULL CHECK(level IN ('Central','State')),
    benefit              TEXT    NOT NULL,
    description          TEXT,
    -- eligibility columns (NULL = no restriction)
    max_land             REAL,
    min_land             REAL,
    bpl_only             INTEGER NOT NULL DEFAULT 0,
    eligible_categories  TEXT,   -- comma-separated, NULL = all
    eligible_states      TEXT,   -- comma-separated, NULL = all states
    irrigated_required   INTEGER -- NULL=any, 0=un-irrigated, 1=irrigated
);
"""


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print(f"[DB] Schema ready at {DB_PATH}")


# ──────────────────────────────────────────────
# SEED DATA
# ──────────────────────────────────────────────

FARMERS = [
    # (name, village, state, land_acres, annual_income, age, category, irrigated, bpl, has_loan, crops)
    ("Ramaiah Goud",     "Nalgonda",   "Telangana",       2.5,  85000,  48, "OBC",     1, 0, 1, ["Rice","Maize"]),
    ("Lakshmi Devi",     "Karimnagar", "Telangana",       1.2,  42000,  39, "SC",      0, 1, 0, ["Cotton"]),
    ("Suresh Patil",     "Bidar",      "Karnataka",       6.0, 210000,  55, "General", 1, 0, 1, ["Soybean","Jowar"]),
    ("Anita Kumari",     "Patna",      "Bihar",           0.8,  28000,  34, "ST",      0, 1, 0, ["Wheat","Mustard"]),
    ("Vijay Reddy",      "Guntur",     "Andhra Pradesh",  4.0, 145000,  42, "OBC",     1, 0, 1, ["Chilli","Rice"]),
    ("Meena Bai",        "Jhansi",     "Uttar Pradesh",   1.5,  36000,  52, "SC",      0, 1, 0, ["Wheat"]),
    ("Rajesh Kumar",     "Sikar",      "Rajasthan",       3.2,  98000,  46, "OBC",     0, 0, 1, ["Bajra","Groundnut"]),
    ("Savitri Naidu",    "Warangal",   "Telangana",       2.0,  68000,  38, "General", 1, 0, 0, ["Maize","Sunflower"]),
    ("Harikrishna Rao",  "Vizag",      "Andhra Pradesh",  8.5, 320000,  60, "General", 1, 0, 0, ["Cashew","Coconut"]),
    ("Pushpa Verma",     "Raipur",     "Chhattisgarh",    1.0,  22000,  44, "ST",      0, 1, 0, ["Rice","Vegetables"]),
    ("Mohan Lal",        "Ludhiana",   "Punjab",         12.0, 580000,  58, "General", 1, 0, 1, ["Wheat","Paddy"]),
    ("Sunita Yadav",     "Nashik",     "Maharashtra",     3.5, 175000,  41, "OBC",     1, 0, 1, ["Grapes","Onion"]),
    ("Basavaraj Nayak",  "Dharwad",    "Karnataka",       5.5, 240000,  50, "OBC",     1, 0, 1, ["Sugarcane"]),
    ("Kamla Devi",       "Jaipur",     "Rajasthan",       1.8,  52000,  36, "SC",      0, 0, 0, ["Mustard","Wheat"]),
    ("Srinivasa Murthy", "Mysuru",     "Karnataka",       2.8,  92000,  47, "General", 1, 0, 0, ["Turmeric","Ragi"]),
]

SCHEMES = [
    # (scheme_id, name, full_name, category, level, benefit, description,
    #  max_land, min_land, bpl_only, eligible_categories, eligible_states, irrigated_required)
    ("PM-KISAN", "PM-KISAN",
     "Pradhan Mantri Kisan Samman Nidhi",
     "Income Support", "Central",
     "₹6,000/year in 3 installments",
     "Direct income support of ₹6,000 per year to all landholding farmer families.",
     None, None, 0, "General,OBC,SC,ST", None, None),

    ("PMFBY", "PMFBY",
     "Pradhan Mantri Fasal Bima Yojana",
     "Crop Insurance", "Central",
     "Crop insurance at subsidised premium",
     "Comprehensive crop insurance against natural calamities, pests and diseases.",
     None, None, 0, "General,OBC,SC,ST", None, None),

    ("KCC", "KCC",
     "Kisan Credit Card",
     "Credit", "Central",
     "Short-term crop credit at low interest (4%)",
     "Flexible revolving credit for crop cultivation, post-harvest and allied activities.",
     None, 0.5, 0, "General,OBC,SC,ST", None, None),

    ("SMAM", "SMAM",
     "Sub-Mission on Agricultural Mechanisation",
     "Mechanisation", "Central",
     "50–80% subsidy on farm equipment",
     "Subsidies on tractors, harvesters, and implements for small and marginal farmers.",
     5.0, None, 0, "General,OBC,SC,ST", None, None),

    ("PMKSY", "PMKSY",
     "PM Krishi Sinchayee Yojana",
     "Irrigation", "Central",
     "Drip/sprinkler irrigation subsidy up to 90%",
     "Expanding irrigation coverage and improving water use efficiency for dry-land farmers.",
     None, None, 0, "General,OBC,SC,ST", None, 0),

    ("NFSM", "NFSM",
     "National Food Security Mission",
     "Crop Development", "Central",
     "Free seeds, demonstrations, training",
     "Increasing production of rice, wheat, pulses through area expansion and productivity.",
     None, None, 0, "General,OBC,SC,ST", None, None),

    ("RKVY", "RKVY",
     "Rashtriya Krishi Vikas Yojana",
     "Development", "Central",
     "State-tailored agriculture development grants",
     "Holistic development of agriculture including horticulture and allied sectors.",
     None, None, 0, "General,OBC,SC,ST", None, None),

    ("SCSP", "SC Sub-Plan",
     "Scheduled Caste Sub-Plan (Agriculture)",
     "Social Welfare", "State",
     "Free equipment, seeds, and training for SC farmers",
     "Special provisions ensuring SC farmers receive free implements, seeds and skill training.",
     None, None, 0, "SC", None, None),

    ("TSP", "TSP",
     "Tribal Sub-Plan (Agriculture)",
     "Social Welfare", "State",
     "Subsidised inputs and free training for ST farmers",
     "Agricultural support specifically for ST farmers with subsidised inputs and demonstrations.",
     None, None, 0, "ST", None, None),

    ("RYTHU", "Rythu Bandhu",
     "Rythu Bandhu Scheme",
     "Income Support", "State",
     "₹10,000 per acre per season",
     "Investment support directly to Telangana farmers' accounts every sowing season.",
     None, None, 0, "General,OBC,SC,ST", "Telangana", None),

    ("YSRRC", "YSR Rythu Bharosa",
     "YSR Rythu Bharosa & PM Kisan",
     "Income Support", "State",
     "₹13,500/year combined support",
     "Andhra Pradesh state top-up on PM-KISAN for comprehensive income support to farmers.",
     None, None, 0, "General,OBC,SC,ST", "Andhra Pradesh", None),

    ("PMKUSUM", "PM-KUSUM Solar",
     "PM-KUSUM Solar Pump Component",
     "Renewable Energy", "Central",
     "90% subsidy on solar-powered irrigation pumps",
     "Solar-powered irrigation pumps replacing diesel pumps for un-irrigated farm land.",
     None, None, 0, "General,OBC,SC,ST", None, 0),

    ("PKVY", "PKVY",
     "Paramparagat Krishi Vikas Yojana",
     "Organic Farming", "Central",
     "₹50,000/hectare over 3 years for organic conversion",
     "Financial support to farmer groups transitioning to certified organic farming.",
     None, None, 0, "General,OBC,SC,ST", None, None),

    ("MIDH", "MIDH",
     "Mission for Integrated Development of Horticulture",
     "Horticulture", "Central",
     "40–50% subsidy on horticulture infrastructure",
     "Development of horticulture sector including fruits, vegetables, spices and flowers.",
     None, None, 0, "General,OBC,SC,ST", None, None),
]


def seed_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    existing = cur.execute("SELECT COUNT(*) FROM farmers").fetchone()[0]
    if existing > 0:
        print("[DB] Already seeded, skipping.")
        conn.close()
        return

    for (name, village, state, land, income, age, cat, irr, bpl, loan, crops) in FARMERS:
        cur.execute(
            "INSERT INTO farmers (name, village, state, land_acres, annual_income, age, "
            "category, irrigated, bpl, has_loan) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (name, village, state, land, income, age, cat, irr, bpl, loan)
        )
        fid = cur.lastrowid
        for crop in crops:
            cur.execute("INSERT INTO farmer_crops (farmer_id, crop) VALUES (?,?)", (fid, crop))

    for row in SCHEMES:
        cur.execute(
            "INSERT INTO schemes (scheme_id, name, full_name, category, level, benefit, "
            "description, max_land, min_land, bpl_only, eligible_categories, "
            "eligible_states, irrigated_required) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row
        )

    conn.commit()
    conn.close()
    print(f"[DB] Seeded {len(FARMERS)} farmers and {len(SCHEMES)} schemes.")


if __name__ == "__main__":
    init_db()
    seed_db()