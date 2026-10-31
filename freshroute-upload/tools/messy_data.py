#!/usr/bin/env python3
"""Messy data simulator for the oral-defence projects.

Generates realistic, deliberately DIRTY source data for a fake company, so that your silver
layer has real cleaning work to do and your gold layer has real numbers to compute.
Standard library only: no pip install needed (Python 3.9+).

  python messy_data.py --list                                  # the 8 companies
  python messy_data.py --company bytebrew                      # medium mess, 1 batch
  python messy_data.py --company velocity --mess high --batches 3 --rows 5000 --seed 7
  python messy_data.py --company shopnest --format jsonl       # JSON lines instead of CSV

Output:  out/<company>/batch_001/<table>.csv (or .jsonl)
         out/<company>/issues_report.json   <- what was broken, and how many times (your answer key)

Batches 2+ simulate the next days: new rows, late-arriving rows, updates of earlier rows and,
from batch 2, a SCHEMA DRIFT (a renamed or extra column) to test your pipeline's robustness.
"""
import argparse
import csv
import json
import random
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

FIRST = ["Olivia", "Noah", "Amelia", "Leo", "Isla", "Oscar", "Ava", "Arthur", "Mia", "Muhammad", "Ivy", "Harry",
         "Freya", "Jack", "Lily", "George", "Sofia", "Theo", "Grace", "Finley", "Chloé", "Zoë", "Mateo", "Aaliyah"]
LAST = ["Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson", "Davies", "Patel", "Robinson",
        "Wright", "Thompson", "Evans", "Walker", "White", "Roberts", "Green", "Hall", "Nguyen", "Müller", "O'Brien"]
CITIES = ["London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Bristol", "Liverpool", "Edinburgh", "Cardiff",
          "Belfast", "Paris", "Lyon"]
DOMAINS = ["gmail.com", "outlook.com", "yahoo.co.uk", "icloud.com", "proton.me"]

# --------------------------------------------------------------------------- company definitions
# Column spec: (name, kind, options). Kinds: id, fk, person, email, phone, city, date, datetime, money, int,
# float, category, text, bool, json. The first table of each company is generated first (dimensions first).
COMPANIES = {
    "bytebrew": {
        "title": "ByteBrew Coffee - 14 coffee shops, loyalty app, card terminals",
        "tables": {
            "stores": (14, [("store_id", "id", {}), ("store_name", "text", {"choices": ["ByteBrew {city}"]}),
                            ("city", "city", {}), ("opened_on", "date", {"start": "2019-01-01", "days": 2000}),
                            ("seats", "int", {"min": 12, "max": 80})]),
            "products": (40, [("product_id", "id", {}), ("product_name", "text", {"choices": [
                "Flat white", "Latte", "Cappuccino", "Espresso", "Mocha", "Chai latte", "Croissant", "Banana bread",
                "Bagel", "Iced latte", "Hot chocolate", "Matcha", "Cookie", "Muffin", "Toastie"]}),
                ("category", "category", {"values": ["coffee", "tea", "bakery", "food", "cold drinks"]}),
                ("price_gbp", "money", {"min": 1.8, "max": 7.5})]),
            "loyalty_members": (800, [("member_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}),
                                      ("phone", "phone", {}), ("signup_date", "date", {"start": "2023-01-01", "days": 1000}),
                                      ("home_store_id", "fk", {"table": "stores"})]),
            "transactions": (6000, [("transaction_id", "id", {}), ("store_id", "fk", {"table": "stores"}),
                                    ("member_id", "fk", {"table": "loyalty_members", "nullable": 0.45}),
                                    ("product_id", "fk", {"table": "products"}), ("quantity", "int", {"min": 1, "max": 4}),
                                    ("amount_gbp", "money", {"min": 1.8, "max": 30}),
                                    ("payment_method", "category", {"values": ["card", "contactless", "cash", "app"]}),
                                    ("sold_at", "datetime", {"start": "2026-09-01", "days": 30})]),
        },
    },
    "velocity": {
        "title": "VeloCity Bikes - city bike sharing: stations, bikes, trips",
        "tables": {
            "stations": (60, [("station_id", "id", {}), ("station_name", "text", {"choices": ["{city} Central", "{city} Park", "{city} Station", "{city} Uni"]}),
                              ("city", "city", {}), ("capacity", "int", {"min": 10, "max": 40}),
                              ("latitude", "float", {"min": 51.40, "max": 51.60}), ("longitude", "float", {"min": -0.25, "max": 0.05})]),
            "bikes": (400, [("bike_id", "id", {}), ("model", "category", {"values": ["classic", "e-bike", "cargo"]}),
                            ("commissioned_on", "date", {"start": "2021-01-01", "days": 1500})]),
            "riders": (1500, [("rider_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}),
                              ("plan", "category", {"values": ["pay-as-you-go", "monthly", "annual", "student"]}),
                              ("signup_date", "date", {"start": "2022-01-01", "days": 1300})]),
            "trips": (8000, [("trip_id", "id", {}), ("bike_id", "fk", {"table": "bikes"}), ("rider_id", "fk", {"table": "riders"}),
                             ("start_station_id", "fk", {"table": "stations"}), ("end_station_id", "fk", {"table": "stations"}),
                             ("started_at", "datetime", {"start": "2026-09-01", "days": 30}),
                             ("duration_min", "int", {"min": 2, "max": 90}), ("distance_km", "float", {"min": 0.3, "max": 18}),
                             ("price_gbp", "money", {"min": 0, "max": 12})]),
        },
    },
    "medinova": {
        "title": "MediNova Clinics - private clinics: patients (GDPR!), doctors, appointments, invoices",
        "tables": {
            "doctors": (35, [("doctor_id", "id", {}), ("full_name", "person", {}),
                             ("specialty", "category", {"values": ["general practice", "cardiology", "dermatology", "paediatrics", "physiotherapy"]}),
                             ("clinic_city", "city", {})]),
            "patients": (1200, [("patient_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}), ("phone", "phone", {}),
                                ("date_of_birth", "date", {"start": "1940-01-01", "days": 29000}),
                                ("nhs_number", "text", {"pattern": "###-###-####"}), ("city", "city", {})]),
            "appointments": (5000, [("appointment_id", "id", {}), ("patient_id", "fk", {"table": "patients"}),
                                    ("doctor_id", "fk", {"table": "doctors"}), ("scheduled_at", "datetime", {"start": "2026-08-15", "days": 60}),
                                    ("status", "category", {"values": ["booked", "completed", "cancelled", "no-show"]}),
                                    ("reason", "text", {"choices": ["check-up", "follow-up", "back pain", "skin rash", "vaccination", "chest pain"]})]),
            "invoices": (4000, [("invoice_id", "id", {}), ("appointment_id", "fk", {"table": "appointments"}),
                                ("amount_gbp", "money", {"min": 40, "max": 450}), ("paid", "bool", {}),
                                ("issued_on", "date", {"start": "2026-08-15", "days": 60})]),
        },
    },
    "freshroute": {
        "title": "FreshRoute Logistics - grocery delivery vans: depots, vehicles, routes, deliveries, GPS pings (JSON)",
        "tables": {
            "depots": (8, [("depot_id", "id", {}), ("city", "city", {}), ("docks", "int", {"min": 4, "max": 20})]),
            "vehicles": (120, [("vehicle_id", "id", {}), ("plate", "text", {"pattern": "FR## ###"}),
                               ("vehicle_type", "category", {"values": ["small van", "large van", "electric van", "refrigerated truck"]}),
                               ("depot_id", "fk", {"table": "depots"}), ("capacity_kg", "int", {"min": 600, "max": 3500})]),
            "routes": (2400, [("route_id", "id", {}), ("vehicle_id", "fk", {"table": "vehicles"}), ("depot_id", "fk", {"table": "depots"}),
                              ("planned_start", "datetime", {"start": "2026-09-01", "days": 30}),
                              ("planned_stops", "int", {"min": 8, "max": 35}),
                              ("status", "category", {"values": ["planned", "in progress", "completed", "cancelled"]})]),
            "deliveries": (9000, [("delivery_id", "id", {}), ("route_id", "fk", {"table": "routes"}),
                                  ("customer_ref", "text", {"pattern": "CU#####"}),
                                  ("promised_by", "datetime", {"start": "2026-09-01", "days": 30}),
                                  ("delivered_at", "datetime", {"start": "2026-09-01", "days": 30}),
                                  ("parcel_kg", "float", {"min": 0.5, "max": 40}),
                                  ("delivery_fee_gbp", "money", {"min": 0, "max": 9.99}),
                                  ("outcome", "category", {"values": ["delivered", "failed - not home", "refused", "damaged"]})]),
            "gps_pings": (15000, [("ping_id", "id", {}), ("vehicle_id", "fk", {"table": "vehicles"}),
                                  ("ping_ts", "datetime", {"start": "2026-09-01", "days": 30}),
                                  ("payload", "json", {"keys": {"lat": [51.48, 51.50, 51.52, 51.55], "lon": [-0.12, -0.10, -0.05, 0.01],
                                                                "speed_kmh": [0, 18, 32, 47, 64], "engine": ["on", "off", None]}})]),
        },
    },
    "greenwatt": {
        "title": "GreenWatt Energy - smart electricity meters: customers, meters, half-hourly readings (IoT)",
        "tables": {
            "tariffs": (6, [("tariff_id", "id", {}), ("tariff_name", "text", {"choices": ["Fixed 12M", "Variable", "Green Fix", "EV Night", "Economy 7", "Business"]}),
                            ("price_per_kwh_gbp", "float", {"min": 0.12, "max": 0.35})]),
            "customers": (900, [("customer_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}), ("city", "city", {}),
                                ("tariff_id", "fk", {"table": "tariffs"}), ("joined_on", "date", {"start": "2020-01-01", "days": 2300})]),
            "meters": (1000, [("meter_id", "id", {}), ("customer_id", "fk", {"table": "customers"}),
                              ("meter_type", "category", {"values": ["smart-v1", "smart-v2", "legacy"]}),
                              ("installed_on", "date", {"start": "2018-01-01", "days": 3000})]),
            "readings": (20000, [("reading_id", "id", {}), ("meter_id", "fk", {"table": "meters"}),
                                 ("reading_ts", "datetime", {"start": "2026-10-01", "days": 7}),
                                 ("kwh", "float", {"min": 0, "max": 3.5}), ("voltage", "float", {"min": 225, "max": 245})]),
        },
    },
    "skyhop": {
        "title": "SkyHop Airlines - regional airline: airports, flights, bookings, delays",
        "tables": {
            "airports": (18, [("airport_code", "id", {"prefix": "AP"}), ("city", "city", {}), ("runways", "int", {"min": 1, "max": 3})]),
            "passengers": (3000, [("passenger_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}),
                                  ("frequent_flyer_tier", "category", {"values": ["none", "silver", "gold", "platinum"]})]),
            "flights": (900, [("flight_id", "id", {}), ("flight_number", "text", {"pattern": "SH###"}),
                              ("origin", "fk", {"table": "airports"}), ("destination", "fk", {"table": "airports"}),
                              ("scheduled_departure", "datetime", {"start": "2026-09-01", "days": 30}),
                              ("delay_min", "int", {"min": 0, "max": 240}), ("seats", "int", {"min": 70, "max": 190})]),
            "bookings": (7000, [("booking_id", "id", {}), ("flight_id", "fk", {"table": "flights"}), ("passenger_id", "fk", {"table": "passengers"}),
                                ("fare_gbp", "money", {"min": 29, "max": 420}),
                                ("cabin", "category", {"values": ["economy", "premium", "business"]}),
                                ("booked_at", "datetime", {"start": "2026-06-01", "days": 120})]),
        },
    },
    "streamflix": {
        "title": "StreamFlix - video streaming: users, subscriptions, catalogue, viewing events",
        "tables": {
            "titles": (300, [("title_id", "id", {}), ("title_name", "text", {"choices": ["The Last Match", "Night Shift", "Deep Blue", "Kings of Leeds", "Pixel Hearts", "The Archive"]}),
                             ("genre", "category", {"values": ["drama", "comedy", "documentary", "thriller", "kids", "sport"]}),
                             ("duration_min", "int", {"min": 20, "max": 180}), ("released_on", "date", {"start": "2010-01-01", "days": 5800})]),
            "users": (2500, [("user_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}),
                             ("country", "category", {"values": ["UK", "FR", "IE", "BE", "NL"]}),
                             ("created_at", "datetime", {"start": "2022-01-01", "days": 1400})]),
            "subscriptions": (2600, [("subscription_id", "id", {}), ("user_id", "fk", {"table": "users"}),
                                     ("plan", "category", {"values": ["basic", "standard", "premium"]}),
                                     ("monthly_price_gbp", "money", {"min": 5.99, "max": 17.99}),
                                     ("started_on", "date", {"start": "2022-01-01", "days": 1400}), ("active", "bool", {})]),
            "viewing_events": (15000, [("event_id", "id", {}), ("user_id", "fk", {"table": "users"}), ("title_id", "fk", {"table": "titles"}),
                                       ("started_at", "datetime", {"start": "2026-09-01", "days": 30}),
                                       ("watched_min", "int", {"min": 1, "max": 180}),
                                       ("device", "category", {"values": ["tv", "mobile", "laptop", "tablet"]})]),
        },
    },
    "fitpulse": {
        "title": "FitPulse Gyms - 9 gyms: members, memberships, check-ins, class bookings, payments",
        "tables": {
            "gyms": (9, [("gym_id", "id", {}), ("city", "city", {}), ("opened_on", "date", {"start": "2015-01-01", "days": 3500})]),
            "members": (2200, [("member_id", "id", {}), ("full_name", "person", {}), ("email", "email", {}), ("phone", "phone", {}),
                               ("home_gym_id", "fk", {"table": "gyms"}),
                               ("membership", "category", {"values": ["off-peak", "standard", "premium", "student"]}),
                               ("joined_on", "date", {"start": "2021-01-01", "days": 2000})]),
            "checkins": (14000, [("checkin_id", "id", {}), ("member_id", "fk", {"table": "members"}), ("gym_id", "fk", {"table": "gyms"}),
                                 ("checked_in_at", "datetime", {"start": "2026-09-01", "days": 30}),
                                 ("minutes_on_site", "int", {"min": 10, "max": 180})]),
            "payments": (6000, [("payment_id", "id", {}), ("member_id", "fk", {"table": "members"}),
                                ("amount_gbp", "money", {"min": 15, "max": 75}),
                                ("status", "category", {"values": ["succeeded", "failed", "refunded"]}),
                                ("paid_at", "datetime", {"start": "2026-08-01", "days": 60})]),
        },
    },
}

# --------------------------------------------------------------------------- mess catalogue
# probability per cell, by mess level
LEVELS = {"low": 0.01, "medium": 0.04, "high": 0.10}
ISSUES = Counter()


def clean_value(kind, opt, i, rng, ctx):
    if kind == "id":
        return f"{opt['prefix']}{i:03d}" if "prefix" in opt else i
    if kind == "fk":
        if rng.random() < opt.get("nullable", 0):
            return None
        return rng.choice(ctx[opt["table"]])
    if kind == "person":
        return f"{rng.choice(FIRST)} {rng.choice(LAST)}"
    if kind == "email":
        return None  # filled from the name below
    if kind == "phone":
        return f"+44 7{rng.randint(100, 999)} {rng.randint(100000, 999999)}"
    if kind == "city":
        return rng.choice(CITIES)
    if kind == "date":
        return date.fromisoformat(opt["start"]) + timedelta(days=rng.randint(0, opt["days"]))
    if kind == "datetime":
        start = datetime.fromisoformat(opt["start"]).replace(tzinfo=timezone.utc)
        return start + timedelta(seconds=rng.randint(0, opt["days"] * 86400))
    if kind == "money":
        return round(rng.uniform(opt["min"], opt["max"]), 2)
    if kind == "int":
        return rng.randint(opt["min"], opt["max"])
    if kind == "float":
        return round(rng.uniform(opt["min"], opt["max"]), 4)
    if kind == "category":
        return rng.choice(opt["values"])
    if kind == "bool":
        return rng.random() < 0.8
    if kind == "text":
        if "pattern" in opt:
            return "".join(str(rng.randint(0, 9)) if c == "#" else c for c in opt["pattern"])
        return rng.choice(opt["choices"]).replace("{city}", rng.choice(CITIES))
    if kind == "json":
        return {k: rng.choice(v) for k, v in opt["keys"].items()}
    raise ValueError(kind)


def to_text(kind, v):
    if v is None:
        return None
    if kind == "date":
        return v.isoformat()
    if kind == "datetime":
        return v.strftime("%Y-%m-%dT%H:%M:%SZ")
    if kind == "bool":
        return "true" if v else "false"
    if kind == "json":
        return json.dumps(v)
    return v


def mess_value(kind, opt, v, rng, p):
    """Return a dirty version of v (or v itself). Every change is counted in ISSUES."""
    if v is None or rng.random() > p:
        return v
    if kind in ("id",):
        return v
    roll = rng.random()
    if roll < 0.15:
        ISSUES[f"{kind}: missing value as empty / N/A / null text"] += 1
        return rng.choice(["", "N/A", "null", "NULL", "-", " "])
    if kind == "date":
        ISSUES["date: mixed formats (dd/mm/yyyy, mm-dd-yyyy, yyyymmdd, '3 Sep 2026')"] += 1
        d = date.fromisoformat(v)
        return rng.choice([d.strftime("%d/%m/%Y"), d.strftime("%m-%d-%Y"), d.strftime("%Y%m%d"), f"{d.day} {d.strftime('%b %Y')}"])
    if kind == "datetime":
        dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%SZ")
        if roll < 0.45:
            ISSUES["datetime: other formats (local time without zone, epoch seconds, dd/mm/yyyy hh:mm)"] += 1
            return rng.choice([dt.strftime("%Y-%m-%d %H:%M:%S"), str(int(dt.replace(tzinfo=timezone.utc).timestamp())),
                               dt.strftime("%d/%m/%Y %H:%M")])
        if roll < 0.6:
            ISSUES["datetime: impossible future date (year 2099) or 1970 default"] += 1
            return rng.choice(["2099-01-01T00:00:00Z", "1970-01-01T00:00:00Z"])
        ISSUES["datetime: time zone offset instead of UTC (+01:00)"] += 1
        return (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+01:00")
    if kind == "money":
        if roll < 0.5:
            ISSUES["money: text with currency symbol / thousands separator / comma decimal"] += 1
            return rng.choice([f"£{v:,.2f}", f"{v:.2f} GBP", f"€{v:.2f}", f"{v:.2f}".replace(".", ","), f" {v} "])
        if roll < 0.75:
            ISSUES["money: negative amount (refund encoded as negative, or sign error)"] += 1
            return -v
        ISSUES["money: outlier x100 (pence instead of pounds)"] += 1
        return round(v * 100, 2)
    if kind in ("int", "float"):
        if roll < 0.5:
            ISSUES[f"{kind}: outlier / impossible value (negative or x1000)"] += 1
            return rng.choice([-abs(v), v * 1000])
        ISSUES[f"{kind}: number stored as text with spaces or units"] += 1
        return rng.choice([f" {v} ", f"{v} units", f"{v}".replace(".", ",")])
    if kind == "category":
        ISSUES["category: inconsistent spelling / case / spaces / translation"] += 1
        return rng.choice([v.upper(), v.title(), f" {v} ", v.replace(" ", "_"), v.replace("-", " "), v + "s",
                           {"paid": "payé", "cancelled": "canceled", "card": "credit card"}.get(v, v.upper())])
    if kind == "person":
        if roll < 0.5:
            ISSUES["person: wrong case / extra spaces / LAST, First"] += 1
            first, last = v.split(" ", 1)
            return rng.choice([v.upper(), v.lower(), f"  {v}", f"{last.upper()}, {first}"])
        ISSUES["person: broken encoding (Ã© instead of é)"] += 1
        return v.encode("utf-8").decode("latin-1") if any(ord(c) > 127 for c in v) else v + "  "
    if kind == "email":
        ISSUES["email: invalid or inconsistent (no @, upper case, spaces, test address)"] += 1
        return rng.choice([v.replace("@", ""), v.upper(), f" {v}", "test@test.com", v.replace(".", "..", 1)])
    if kind == "phone":
        ISSUES["phone: other formats (07..., 0044..., dots, missing digits)"] += 1
        digits = v.replace("+44 ", "0").replace(" ", "")
        return rng.choice([digits, "0044" + digits[1:], ".".join([digits[:5], digits[5:]]), digits[:-3]])
    if kind == "city":
        ISSUES["city: case / spaces / typo"] += 1
        return rng.choice([v.upper(), v.lower(), f"{v} ", v[:-1], v.replace("a", "e", 1)])
    if kind == "bool":
        ISSUES["bool: Y/N, 1/0, yes/no, TRUE/False mixed"] += 1
        return rng.choice(["Y", "N", "1", "0", "yes", "no", "TRUE", "False"])
    if kind == "fk":
        if roll > 0.35:          # keep orphans rare: they are serious, not everywhere
            return v
        ISSUES["foreign key: orphan id that does not exist in the parent table"] += 1
        return (v if isinstance(v, int) else 0) + 900000 if isinstance(v, int) else f"{v}X"
    if kind == "text":
        ISSUES["text: extra spaces / case"] += 1
        return rng.choice([f"  {v}", str(v).upper(), str(v).lower()])
    if kind == "json":
        ISSUES["json: missing key / null / different key name"] += 1
        d = json.loads(v)
        k = rng.choice(list(d))
        if roll < 0.5:
            d.pop(k)
        else:
            d[k.upper()] = d.pop(k)
        return json.dumps(d)
    return v


def email_for(name, rng):
    first, last = name.lower().replace("'", "").split(" ", 1)
    return f"{first}.{last}{rng.randint(1, 99)}@{rng.choice(DOMAINS)}"


def generate(company, mess, rows_factor, batches, seed, fmt, out_dir):
    rng = random.Random(seed)
    spec = COMPANIES[company]
    p = LEVELS[mess]
    base = Path(out_dir) / company
    ids = {}            # table -> list of clean ids (for foreign keys)
    history = {}        # table -> rows already sent (for late arriving / updates)
    for b in range(1, batches + 1):
        folder = base / f"batch_{b:03d}"
        folder.mkdir(parents=True, exist_ok=True)
        for t, (n, cols) in spec["tables"].items():
            is_fact = t == list(spec["tables"])[-1] or n >= 4000
            if b > 1 and not is_fact:
                count = max(1, n // 20)           # dimensions: a few new or changed rows per batch
            else:
                count = int(n * rows_factor) if b == 1 else int(n * rows_factor * 0.3)
            start_id = len(ids.get(t, [])) + 1
            rows = []
            for i in range(start_id, start_id + count):
                rec = {}
                for name, kind, opt in cols:
                    rec[name] = clean_value(kind, opt, i, rng, ids)
                for name, kind, opt in cols:
                    # a site name uses the site's own city ("ByteBrew Leeds" is in Leeds)
                    if kind == "text" and "city" in rec and any("{city}" in c for c in opt.get("choices", [])):
                        rec[name] = rng.choice(opt["choices"]).replace("{city}", rec["city"])
                for name, kind, opt in cols:
                    if kind == "email":
                        person = next((rec[c] for c, k, _ in cols if k == "person"), "Data Fan")
                        rec[name] = email_for(person, rng)
                ids.setdefault(t, []).append(rec[cols[0][0]])
                rows.append({name: to_text(kind, rec[name]) for name, kind, _ in cols})
            kinds = {name: (kind, opt) for name, kind, opt in cols}
            dirty = [{c: mess_value(*kinds[c], v, rng, p) for c, v in r.items()} for r in rows]
            # exact duplicates and near-duplicates (same id, one field changed)
            dups = []
            for _ in range(int(len(dirty) * p * 0.8)):
                j = rng.randrange(len(rows))
                dup = dict(dirty[j])
                if rng.random() < 0.5:
                    ISSUES["row: exact duplicate"] += 1
                else:
                    c = rng.choice([c for c in dup if c != cols[0][0]])
                    if rows[j][c] is not None:
                        dup[c] = mess_value(*kinds[c], rows[j][c], rng, 1.0)
                    ISSUES["row: near duplicate (same id, a field differs)"] += 1
                dups.append(dup)
            for dup in dups:
                dirty.insert(rng.randrange(len(dirty) + 1), dup)
            if b > 1 and t in history:
                late = rng.sample(history[t], min(len(history[t]), max(1, int(count * 0.05))))
                for r in late:
                    r = dict(r)
                    for c, (kind, opt) in kinds.items():
                        if kind in ("category", "money", "int") and rng.random() < 0.5:
                            r[c] = to_text(kind, clean_value(kind, opt, 0, rng, ids))
                    dirty.append(r)
                ISSUES["row: update of a row sent in an earlier batch (same id, new values)"] += len(late)
            history.setdefault(t, []).extend(rows)
            header = list(dirty[0].keys())
            if b >= 2 and is_fact:
                old = header[-1]
                header[-1] = old + "_v2"
                header.append("source_system")
                for r in dirty:
                    r[old + "_v2"] = r.pop(old)
                    r["source_system"] = rng.choice(["pos-eu", "pos-uk", "app"])
                ISSUES[f"schema drift: {t}.{old} renamed to {old}_v2 and column source_system added (batch {b})"] += 1
            write(folder / t, header, dirty, fmt, rng)
    report = {"company": company, "title": spec["title"], "mess": mess, "batches": batches, "seed": seed,
              "issues_injected": dict(sorted(ISSUES.items(), key=lambda kv: -kv[1]))}
    (base / "issues_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False))
    return base


def write(path_no_ext, header, rows, fmt, rng):
    if fmt == "jsonl":
        with open(f"{path_no_ext}.jsonl", "w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps({k: r.get(k) for k in header}, ensure_ascii=False) + "\n")
        return
    with open(f"{path_no_ext}.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(["" if r.get(k) is None else r.get(k) for k in header])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--company", choices=sorted(COMPANIES))
    ap.add_argument("--list", action="store_true", help="list the companies and their tables")
    ap.add_argument("--mess", choices=list(LEVELS), default="medium")
    ap.add_argument("--rows", type=float, default=1.0, help="multiply the default row counts (e.g. 0.1 or 5)")
    ap.add_argument("--batches", type=int, default=1, help="number of daily batches (2+ adds updates, late rows, schema drift)")
    ap.add_argument("--seed", type=int, default=42, help="same seed = same data (reproducible)")
    ap.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    ap.add_argument("--out", default="out")
    a = ap.parse_args()
    if a.list or not a.company:
        for k, v in COMPANIES.items():
            print(f"{k:11} {v['title']}")
            for t, (n, cols) in v["tables"].items():
                print(f"{'':11}   {t:16} ~{n:>6} rows  " + ", ".join(c for c, _, _ in cols))
        return
    base = generate(a.company, a.mess, a.rows, a.batches, a.seed, a.format, a.out)
    print(f"Data written to {base}/  -  issues report: {base}/issues_report.json")
    for k, v in list(json.loads((base / 'issues_report.json').read_text())["issues_injected"].items())[:8]:
        print(f"  {v:>6}  {k}")


if __name__ == "__main__":
    main()
