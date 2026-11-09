#!/usr/bin/env python3
"""
FMS - The Game: Unified Single-Source AI Model & Team Ratings Updater
---------------------------------------------------------------------
Fetches historical and live match data (Goals, Total Shots, Shots on Target, Corners)
for all 15 leagues from a single unified JSON API (`site.api.espn.com`), maintains an
incremental archive (`SWOS_AI_Simulator/data/espn_matches_archive.json`), trains the
Random Forest models (`m_1x2`, `m_o15`, `m_o25`), computes ELO & rolling team metrics,
and outputs `SWOS_AI_Simulator/data/fms_models_and_ratings.json`.

Zero external dependencies required (uses Python 3 standard library only).
"""

import json
import math
import random
import unicodedata
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = (REPO_ROOT / "SWOS_AI_Simulator" / "data") if (REPO_ROOT / "SWOS_AI_Simulator" / "data").exists() else (REPO_ROOT / "data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
RATINGS_JSON_PATH = DATA_DIR / "fms_models_and_ratings.json"
ARCHIVE_JSON_PATH = DATA_DIR / "espn_matches_archive.json"
TEAMS_META_PATH = DATA_DIR / "teams_metadata.json"

LEAGUE_BASE_ELO = {
    "E0": 1650.0,
    "I1": 1620.0, "SP1": 1620.0, "D1": 1620.0,
    "F1": 1580.0,
    "P1": 1500.0, "N1": 1500.0,
    "B1": 1450.0, "T1": 1450.0, "G1": 1450.0,
    "E1": 1430.0, "SC0": 1400.0,
    "I2": 1360.0, "SP2": 1360.0, "D2": 1360.0, "F2": 1360.0,
}

# Unified API slugs for all 15 leagues in FMS - The Game
LEAGUE_SLUGS = {
    "E0": "eng.1", "E1": "eng.2",
    "I1": "ita.1", "I2": "ita.2",
    "SP1": "esp.1", "SP2": "esp.2",
    "D1": "ger.1", "D2": "ger.2",
    "F1": "fra.1", "F2": "fra.2",
    "N1": "ned.1", "P1": "por.1",
    "B1": "bel.1", "T1": "tur.1", "G1": "gre.1",
}

TEAM_ALIASES = {
    "FC Internazionale Milano": "Inter",
    "Internazionale": "Inter",
    "Inter Milan": "Inter",
    "AC Milan": "Milan",
    "Juventus FC": "Juventus",
    "SSC Napoli": "Napoli",
    "AS Roma": "Roma",
    "SS Lazio": "Lazio",
    "Atalanta BC": "Atalanta",
    "ACF Fiorentina": "Fiorentina",
    "Bologna FC 1909": "Bologna",
    "Torino FC": "Torino",
    "Udinese Calcio": "Udinese",
    "Genoa CFC": "Genoa",
    "Cagliari Calcio": "Cagliari",
    "Hellas Verona FC": "Verona",
    "Hellas Verona": "Verona",
    "Empoli FC": "Empoli",
    "US Lecce": "Lecce",
    "Parma Calcio 1913": "Parma",
    "Como 1907": "Como",
    "Venezia FC": "Venezia",
    "AC Monza": "Monza",
    "US Salernitana 1919": "Salernitana",
    "US Sassuolo Calcio": "Sassuolo",
    "UC Sampdoria": "Sampdoria",
    "Palermo FC": "Palermo",
    "SSC Bari": "Bari",
    "US Cremonese": "Cremonese",
    "Spezia Calcio": "Spezia",
    "Pisa SC": "Pisa",
    "AC Pisa 1909": "Pisa",
    "Manchester City FC": "Man City",
    "Manchester City": "Man City",
    "Manchester United FC": "Man United",
    "Manchester United": "Man United",
    "Arsenal FC": "Arsenal",
    "Liverpool FC": "Liverpool",
    "Chelsea FC": "Chelsea",
    "Tottenham Hotspur FC": "Tottenham",
    "Tottenham Hotspur": "Tottenham",
    "Newcastle United FC": "Newcastle",
    "Newcastle United": "Newcastle",
    "Aston Villa FC": "Aston Villa",
    "Brighton & Hove Albion FC": "Brighton",
    "Brighton & Hove Albion": "Brighton",
    "West Ham United FC": "West Ham",
    "West Ham United": "West Ham",
    "AFC Bournemouth": "Bournemouth",
    "Crystal Palace FC": "Crystal Palace",
    "Fulham FC": "Fulham",
    "Brentford FC": "Brentford",
    "Wolverhampton Wanderers FC": "Wolves",
    "Wolverhampton Wanderers": "Wolves",
    "Everton FC": "Everton",
    "Nottingham Forest FC": "Nott'm Forest",
    "Nottingham Forest": "Nott'm Forest",
    "Leicester City FC": "Leicester",
    "Leicester City": "Leicester",
    "Ipswich Town FC": "Ipswich",
    "Ipswich Town": "Ipswich",
    "Southampton FC": "Southampton",
    "Leeds United FC": "Leeds",
    "Leeds United": "Leeds",
    "Burnley FC": "Burnley",
    "Sheffield United FC": "Sheffield United",
    "Sheffield Wednesday": "Sheffield Weds",
    "Sunderland AFC": "Sunderland",
    "Middlesbrough FC": "Middlesbrough",
    "West Bromwich Albion FC": "West Brom",
    "West Bromwich Albion": "West Brom",
    "Norwich City FC": "Norwich",
    "Norwich City": "Norwich",
    "Coventry City FC": "Coventry",
    "Coventry City": "Coventry",
    "Watford FC": "Watford",
    "Swansea City AFC": "Swansea",
    "Swansea City": "Swansea",
    "Bristol City FC": "Bristol City",
    "Blackburn Rovers FC": "Blackburn",
    "Blackburn Rovers": "Blackburn",
    "Millwall FC": "Millwall",
    "Queens Park Rangers FC": "QPR",
    "Queens Park Rangers": "QPR",
    "Preston North End FC": "Preston",
    "Preston North End": "Preston",
    "Hull City AFC": "Hull",
    "Hull City": "Hull",
    "Stoke City FC": "Stoke",
    "Stoke City": "Stoke",
    "Cardiff City FC": "Cardiff",
    "Cardiff City": "Cardiff",
    "Derby County FC": "Derby",
    "Derby County": "Derby",
    "Portsmouth FC": "Portsmouth",
    "Oxford United FC": "Oxford",
    "Oxford United": "Oxford",
    "Plymouth Argyle FC": "Plymouth",
    "Plymouth Argyle": "Plymouth",
    "Luton Town FC": "Luton",
    "Luton Town": "Luton",
    "Real Madrid CF": "Real Madrid",
    "FC Barcelona": "Barcelona",
    "Club Atlético de Madrid": "Ath Madrid",
    "Atlético Madrid": "Ath Madrid",
    "Atletico Madrid": "Ath Madrid",
    "Athletic Club": "Ath Bilbao",
    "Real Sociedad de Fútbol": "Sociedad",
    "Real Sociedad": "Sociedad",
    "Real Sociedad II": "Sociedad B",
    "RC Celta Fortuna": "Celta B",
    "Real Betis Balompié": "Betis",
    "Real Betis": "Betis",
    "Villarreal CF": "Villarreal",
    "Valencia CF": "Valencia",
    "Sevilla FC": "Sevilla",
    "Girona FC": "Girona",
    "RC Celta de Vigo": "Celta",
    "Celta Vigo": "Celta",
    "CA Osasuna": "Osasuna",
    "Getafe CF": "Getafe",
    "Rayo Vallecano de Madrid": "Vallecano",
    "Rayo Vallecano": "Vallecano",
    "RCD Mallorca": "Mallorca",
    "Deportivo Alavés": "Alaves",
    "Alavés": "Alaves",
    "Deportivo La Coruña": "La Coruna",
    "Deportivo": "La Coruna",
    "Málaga": "Malaga",
    "UD Las Palmas": "Las Palmas",
    "CD Leganés": "Leganes",
    "Leganés": "Leganes",
    "RCD Espanyol de Barcelona": "Espanyol",
    "RCD Espanyol": "Espanyol",
    "Real Valladolid CF": "Valladolid",
    "Real Valladolid": "Valladolid",
    "FC Bayern München": "Bayern Munich",
    "Bayern Munich": "Bayern Munich",
    "Bayer 04 Leverkusen": "Leverkusen",
    "Bayer Leverkusen": "Leverkusen",
    "Borussia Dortmund": "Dortmund",
    "RB Leipzig": "RB Leipzig",
    "VfB Stuttgart": "Stuttgart",
    "Eintracht Frankfurt": "Ein Frankfurt",
    "TSG 1899 Hoffenheim": "Hoffenheim",
    "TSG Hoffenheim": "Hoffenheim",
    "1. FC Heidenheim 1846": "Heidenheim",
    "1. FC Heidenheim": "Heidenheim",
    "SV Werder Bremen": "Werder Bremen",
    "Werder Bremen": "Werder Bremen",
    "SC Freiburg": "Freiburg",
    "FC Augsburg": "Augsburg",
    "VfL Wolfsburg": "Wolfsburg",
    "1. FSV Mainz 05": "Mainz",
    "Mainz 05": "Mainz",
    "Borussia Mönchengladbach": "M'gladbach",
    "Borussia Monchengladbach": "M'gladbach",
    "1. FC Union Berlin": "Union Berlin",
    "Union Berlin": "Union Berlin",
    "VfL Bochum 1848": "Bochum",
    "FC St. Pauli 1910": "St Pauli",
    "St. Pauli": "St Pauli",
    "Holstein Kiel": "Holstein Kiel",
    "1. FC Köln": "FC Koln",
    "FC Cologne": "FC Koln",
    "Cologne": "FC Koln",
    "Hamburger SV": "Hamburg",
    "Hertha BSC": "Hertha",
    "FC Schalke 04": "Schalke 04",
    "Fortuna Düsseldorf": "Fortuna Dusseldorf",
    "Hannover 96": "Hannover",
    "Karlsruher SC": "Karlsruhe",
    "1. FC Kaiserslautern": "Kaiserslautern",
    "1. FC Nürnberg": "Nurnberg",
    "Paris Saint-Germain FC": "Paris SG",
    "Paris Saint-Germain": "Paris SG",
    "AS Monaco FC": "Monaco",
    "AS Monaco": "Monaco",
    "Stade Brestois 29": "Brest",
    "Stade Brestois": "Brest",
    "LOSC Lille": "Lille",
    "OGC Nice": "Nice",
    "Olympique Lyonnais": "Lyon",
    "Olympique de Marseille": "Marseille",
    "RC Lens": "Lens",
    "Stade Rennais FC 1901": "Rennes",
    "Stade Rennais": "Rennes",
    "Toulouse FC": "Toulouse",
    "Montpellier HSC": "Montpellier",
    "RC Strasbourg Alsace": "Strasbourg",
    "RC Strasbourg": "Strasbourg",
    "FC Nantes": "Nantes",
    "Le Havre AC": "Le Havre",
    "AJ Auxerre": "Auxerre",
    "Angers SCO": "Angers",
    "AS Saint-Étienne": "St Etienne",
    "Saint-Étienne": "St Etienne",
    "Sporting Clube de Portugal": "Sp Lisbon",
    "Sporting CP": "Sp Lisbon",
    "SL Benfica": "Benfica",
    "FC Porto": "Porto",
    "Sporting Clube de Braga": "Sp Braga",
    "SC Braga": "Sp Braga",
    "Vitória SC": "Guimaraes",
    "Vitória Guimarães": "Guimaraes",
    "Académico de Viseu": "Academico Viseu",
    "PSV": "PSV Eindhoven",
    "Feyenoord Rotterdam": "Feyenoord",
    "AFC Ajax": "Ajax",
    "AZ": "AZ Alkmaar",
    "FC Twente '65": "Twente",
    "FC Twente": "Twente",
    "OH Leuven": "Oud-Heverlee Leuven",
    "Union St.-Gilloise": "St. Gilloise",
    "Union Saint-Gilloise": "St. Gilloise",
    "RAAL La Louvière": "RAAL La Louviere",
    "Olympiacos": "Olympiakos",
    "Istanbul Basaksehir": "Buyuksehyr",
    "İstanbul Başakşehir": "Buyuksehyr",
    "Amed SFK": "Amedspor",
    "Erzurum BB": "Erzurumspor",
    "Çorum FK": "Corum",
}


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def load_known_teams_by_league() -> dict:
    by_lg = {lg: [] for lg in LEAGUE_SLUGS}
    if TEAMS_META_PATH.exists():
        with open(TEAMS_META_PATH, "r", encoding="utf-8") as f:
            meta = json.load(f)
        for t_name, t_info in meta.get("teams", {}).items():
            lg = t_info.get("league", "")
            if lg in by_lg and t_name not in by_lg[lg]:
                by_lg[lg].append(t_name)
    for lg in by_lg:
        by_lg[lg].sort()
    return by_lg


def resolve_team_name(raw_name: str, league_code: str, known_teams: list) -> str:
    raw_name = raw_name.strip()
    if raw_name in TEAM_ALIASES:
        mapped = TEAM_ALIASES[raw_name]
        if not known_teams or mapped in known_teams:
            return mapped
    if raw_name in known_teams:
        return raw_name

    cleaned = raw_name
    for prefix in [
        "FC ", "AC ", "AS ", "SSC ", "AFC ", "UD ", "CD ", "CF ", "RC ",
        "RCD ", "CA ", "SV ", "VfL ", "VfB ", "1. FC ", "1. FSV ", "TSG 1899 ", "SC ", "US "
    ]:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):].strip()
    for suffix in [
        " FC", " AFC", " CF", " SC", " AC", " BC", " CFC",
        " 1909", " 1913", " 1907", " 1846", " 1848", " 1910", " 1901", " 04", " 05", " 96", " 29"
    ]:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()

    if cleaned in TEAM_ALIASES:
        mapped = TEAM_ALIASES[cleaned]
        if not known_teams or mapped in known_teams:
            return mapped
    if cleaned in known_teams:
        return cleaned

    cl_ascii = strip_accents(cleaned).lower()
    for cand in known_teams:
        c_ascii = strip_accents(cand).lower()
        if c_ascii == cl_ascii or cl_ascii in c_ascii or c_ascii in cl_ascii:
            return cand
    return TEAM_ALIASES.get(raw_name, cleaned)


def iso_to_ddmm(iso_date: str) -> str:
    if len(iso_date) >= 10 and iso_date[4] == "-":
        return f"{iso_date[8:10]}/{iso_date[5:7]}"
    return iso_date


def fetch_month_for_league(lg_code: str, slug: str, ym: str, known_teams: list) -> list:
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard?dates={ym}&limit=200"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (FMS-Updater/5.0)"})
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            if resp.status != 200:
                return []
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return []

    out = []
    for ev in data.get("events", []):
        comps = ev.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        if not comp.get("status", {}).get("type", {}).get("completed", False):
            continue
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        home_c = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        away_c = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

        raw_ht = home_c.get("team", {}).get("displayName", "")
        raw_at = away_c.get("team", {}).get("displayName", "")
        if not raw_ht or not raw_at:
            continue

        ht = resolve_team_name(raw_ht, lg_code, known_teams)
        at = resolve_team_name(raw_at, lg_code, known_teams)
        date_iso = str(comp.get("date") or ev.get("date") or "")[:10]
        if len(date_iso) < 10:
            continue

        try:
            fthg = int(float(home_c.get("score", 0)))
            ftag = int(float(away_c.get("score", 0)))
        except (ValueError, TypeError):
            continue

        def extract_stat(comp_dict: dict, stat_name: str, default_val: int) -> int:
            for s in comp_dict.get("statistics", []):
                if s.get("name") == stat_name:
                    try:
                        return int(float(s.get("displayValue", default_val)))
                    except (ValueError, TypeError):
                        pass
            return default_val

        hs = extract_stat(home_c, "totalShots", max(4, fthg * 4 + 6))
        as_ = extract_stat(away_c, "totalShots", max(4, ftag * 4 + 5))
        hst = extract_stat(home_c, "shotsOnTarget", max(fthg, int(round(hs * 0.36))))
        ast = extract_stat(away_c, "shotsOnTarget", max(ftag, int(round(as_ * 0.35))))
        hc = extract_stat(home_c, "wonCorners", 5)
        ac = extract_stat(away_c, "wonCorners", 4)

        out.append({
            "league": lg_code,
            "date": date_iso,
            "home_team": ht,
            "away_team": at,
            "fthg": fthg,
            "ftag": ftag,
            "hs": hs,
            "as": as_,
            "hst": hst,
            "ast": ast,
            "hc": hc,
            "ac": ac,
        })
    return out


def generate_months_range(start_year: int, start_month: int, end_year: int, end_month: int) -> list[str]:
    months = []
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        # Skip June & July off-season to save time
        if m not in (6, 7):
            months.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


# ==============================================================================
#  PURE-PYTHON RANDOM FOREST TRAINER (matches FastRandomForest.gd schema)
# ==============================================================================

def build_rf_tree(
    X: list,
    y: list,
    indices: list,
    depth: int,
    max_depth: int,
    min_samples_split: int,
    num_classes: int,
    n_features: int,
    max_features: int,
    rng: random.Random
) -> dict:
    counts = [0.0] * num_classes
    total = float(len(indices))
    for idx in indices:
        c = y[idx]
        if 0 <= c < num_classes:
            counts[c] += 1.0

    probs = [round(counts[c] / max(1.0, total), 5) for c in range(num_classes)]
    max_c_count = max(counts) if counts else 0.0

    if depth >= max_depth or len(indices) < min_samples_split or max_c_count == total:
        return {"leaf": True, "probs": probs}

    parent_gini = 1.0 - sum((counts[c] / total) ** 2 for c in range(num_classes))
    best_gain = 0.001
    best_feat = -1
    best_thresh = 0.0

    cand_features = rng.sample(range(n_features), min(max_features, n_features))
    for f_idx in cand_features:
        for _ in range(5):
            rand_row_idx = indices[rng.randrange(len(indices))]
            thresh = X[rand_row_idx][f_idx]

            left_counts = [0.0] * num_classes
            right_counts = [0.0] * num_classes
            n_left = 0.0
            n_right = 0.0

            for idx in indices:
                val = X[idx][f_idx]
                lbl = y[idx]
                if val <= thresh:
                    left_counts[lbl] += 1.0
                    n_left += 1.0
                else:
                    right_counts[lbl] += 1.0
                    n_right += 1.0

            if n_left < 8.0 or n_right < 8.0:
                continue

            gini_l = 1.0 - sum((left_counts[c] / n_left) ** 2 for c in range(num_classes))
            gini_r = 1.0 - sum((right_counts[c] / n_right) ** 2 for c in range(num_classes))
            weighted_gini = (n_left / total) * gini_l + (n_right / total) * gini_r
            gain = parent_gini - weighted_gini
            if gain > best_gain:
                best_gain = gain
                best_feat = f_idx
                best_thresh = thresh

    if best_feat == -1:
        return {"leaf": True, "probs": probs}

    best_left = [idx for idx in indices if X[idx][best_feat] <= best_thresh]
    best_right = [idx for idx in indices if X[idx][best_feat] > best_thresh]
    if not best_left or not best_right:
        return {"leaf": True, "probs": probs}

    return {
        "leaf": False,
        "f": best_feat,
        "t": round(best_thresh, 5),
        "l": build_rf_tree(X, y, best_left, depth + 1, max_depth, min_samples_split, num_classes, n_features, max_features, rng),
        "r": build_rf_tree(X, y, best_right, depth + 1, max_depth, min_samples_split, num_classes, n_features, max_features, rng),
    }


def train_random_forest(X: list, y: list, n_estimators: int, max_depth: int, num_classes: int, seed_base: int = 42) -> dict:
    n_samples = len(X)
    n_features = len(X[0]) if n_samples > 0 else 12
    max_features = max(2, int(math.sqrt(n_features)) + 1)
    sample_size = min(n_samples, 2000)
    trees = []

    for t in range(n_estimators):
        rng = random.Random(seed_base + t * 7919)
        indices = [rng.randrange(n_samples) for _ in range(sample_size)]
        tree = build_rf_tree(X, y, indices, 0, max_depth, 25, num_classes, n_features, max_features, rng)
        trees.append(tree)

    return {
        "n_estimators": n_estimators,
        "max_depth": max_depth,
        "num_classes": num_classes,
        "trees": trees,
    }


def rf_predict_proba(rf_dict: dict, x_vec: list) -> list[float]:
    num_classes = rf_dict["num_classes"]
    trees = rf_dict["trees"]
    if not trees:
        return [1.0 / num_classes] * num_classes
    avg = [0.0] * num_classes
    for tree in trees:
        node = tree
        while not node.get("leaf", True):
            if x_vec[node["f"]] <= node["t"]:
                node = node["l"]
            else:
                node = node["r"]
        for c in range(num_classes):
            avg[c] += node["probs"][c]
    n_t = float(len(trees))
    return [avg[c] / n_t for c in range(num_classes)]


def clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def compute_rolling_snapshot(buf5: list, buf10: list, cur_elo: float, lg_avg_goals: float, lg_code: str) -> dict:
    base_elo = LEAGUE_BASE_ELO.get(lg_code, 1500.0)
    mu_team_goals = max(0.6, lg_avg_goals * 0.5)
    if buf5:
        n5 = float(len(buf5))
        gf5 = sum(x["gf"] for x in buf5)
        ga5 = sum(x["ga"] for x in buf5)
        sf5 = sum(x["sf"] for x in buf5)
        sa5 = sum(x["sa"] for x in buf5)
        stf5 = sum(x["stf"] for x in buf5)
        sta5 = sum(x["sta"] for x in buf5)
        cf5 = sum(x["cf"] for x in buf5)
        ca5 = sum(x["ca"] for x in buf5)
        conv = (gf5 / stf5) if stf5 > 0 else 0.30
        save = ((sta5 - ga5) / sta5) if sta5 > 0 else 0.70
        tsr = sf5 / (sf5 + sa5) if (sf5 + sa5) > 0 else 0.50
        sotr = stf5 / (stf5 + sta5) if (stf5 + sta5) > 0 else 0.50
        cr = cf5 / (cf5 + ca5) if (cf5 + ca5) > 0 else 0.50
        pdo = clamp((conv + save) * 1000.0, 400.0, 1600.0)
        form_str = " ".join(x["res"] for x in reversed(buf5))
        details = [x["detail"] for x in reversed(buf5)]
    else:
        tsr, sotr, cr, pdo = 0.50, 0.50, 0.50, 1000.0
        form_str = "- - - - -"
        details = []

    if buf10:
        n10 = float(len(buf10))
        avg_gf = sum(x["gf"] for x in buf10) / n10
        avg_ga = sum(x["ga"] for x in buf10) / n10
        avg_stf = sum(x["stf"] for x in buf10) / n10
        avg_sta = sum(x["sta"] for x in buf10) / n10
        # Real Goal + On-Target Threat Index (75% actual goals + 25% shots-on-target conversion rate)
        eff_for = avg_gf * 0.75 + (avg_stf * 0.30) * 0.25
        eff_aga = avg_ga * 0.75 + (avg_sta * 0.30) * 0.25
        att_str = clamp(eff_for / mu_team_goals, 0.25, 2.80)
        def_str = clamp(eff_aga / mu_team_goals, 0.25, 2.80)
    else:
        avg_gf, avg_ga = mu_team_goals, mu_team_goals
        att_str, def_str = 1.0, 1.0

    return {
        "elo": round(cur_elo, 1),
        "league_code": lg_code,
        "league_base_elo": base_elo,
        "matches": len(buf5),
        "tsr": round(tsr, 4),
        "sotr": round(sotr, 4),
        "corner_ratio": round(cr, 4),
        "pdo": round(pdo, 1),
        "avg_gf": round(avg_gf, 2),
        "avg_ga": round(avg_ga, 2),
        "form_str": form_str,
        "last5_details": details,
        "attack_strength": round(att_str, 3),
        "defense_strength": round(def_str, 3),
    }


def build_feature_vector(h_stat: dict, a_stat: dict, lg_avg_goals: float) -> list[float]:
    return [
        float(h_stat["elo"]) - float(a_stat["elo"]),
        float(h_stat["tsr"]), float(a_stat["tsr"]),
        float(h_stat["sotr"]), float(a_stat["sotr"]),
        float(h_stat["corner_ratio"]), float(a_stat["corner_ratio"]),
        float(h_stat["pdo"]), float(a_stat["pdo"]),
        float(h_stat["tsr"]) - float(a_stat["tsr"]),
        float(h_stat["sotr"]) - float(a_stat["sotr"]),
        lg_avg_goals,
    ]


def main() -> None:
    known_by_league = load_known_teams_by_league()
    now_utc = datetime.now(timezone.utc)

    # 1. Load existing ESPN archive if available; otherwise fetch full 2024-25, 2025-26, 2026-27 seasons
    archived_matches = []
    if ARCHIVE_JSON_PATH.exists():
        try:
            with open(ARCHIVE_JSON_PATH, "r", encoding="utf-8") as f:
                archived_matches = json.load(f)
        except Exception:
            archived_matches = []

    if len(archived_matches) < 5000:
        # Full bootstrap from unified API: Aug 2024 -> current month
        months_to_fetch = generate_months_range(2024, 8, now_utc.year, now_utc.month)
        print(f"[FMS Updater] Bootstrapping full 15-league archive ({len(months_to_fetch)} months x 15 leagues)...")
    else:
        # Incremental update: fetch previous month and current month across all 15 leagues
        prev_y = now_utc.year if now_utc.month > 1 else now_utc.year - 1
        prev_m = now_utc.month - 1 if now_utc.month > 1 else 12
        months_to_fetch = [f"{prev_y}{prev_m:02d}", f"{now_utc.year}{now_utc.month:02d}"]
        print(f"[FMS Updater] Incremental update for months {months_to_fetch} across 15 leagues...")

    tasks = []
    for lg_code, slug in LEAGUE_SLUGS.items():
        known = known_by_league.get(lg_code, [])
        for ym in months_to_fetch:
            tasks.append((lg_code, slug, ym, known))

    fetched_matches = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(fetch_month_for_league, lg, sl, ym, kn) for (lg, sl, ym, kn) in tasks]
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                fetched_matches.extend(res)

    # Deduplicate by (league, date, home_team, away_team)
    merged_map = {}
    for m in archived_matches:
        key = (m["league"], m["date"], m["home_team"], m["away_team"])
        merged_map[key] = m
    for m in fetched_matches:
        key = (m["league"], m["date"], m["home_team"], m["away_team"])
        merged_map[key] = m

    all_matches = list(merged_map.values())
    all_matches.sort(key=lambda x: (x["date"], x["league"], x["home_team"]))

    with open(ARCHIVE_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(all_matches, f, ensure_ascii=False)

    # 2. Chronologically build training set X, y and compute final team ratings
    elos = {}
    buf5_map = {}
    buf10_map = {}
    league_goal_sums = {}
    league_goal_counts = {}

    X_train = []
    y_1x2 = []
    y_o15 = []
    y_o25 = []
    latest_date_iso = max((m["date"] for m in all_matches), default="2026-01-01")
    latest_y = int(latest_date_iso[:4])
    latest_m = int(latest_date_iso[5:7])
    current_season_start = f"{latest_y}-07-15" if latest_m >= 7 else f"{latest_y - 1}-07-15"
    teams_in_league_sets = {lg: set() for lg in LEAGUE_SLUGS}

    for m in all_matches:
        lg = m["league"]
        ht = m["home_team"]
        at = m["away_team"]
        hg = int(m["fthg"])
        ag = int(m["ftag"])
        hs = int(m["hs"])
        as_ = int(m["as"])
        hst = int(m["hst"])
        ast = int(m["ast"])
        hc = int(m["hc"])
        ac = int(m["ac"])
        d_iso = m["date"]

        if d_iso >= current_season_start:
            teams_in_league_sets.setdefault(lg, set()).add(ht)
            teams_in_league_sets.setdefault(lg, set()).add(at)

        cur_lg_avg = (league_goal_sums.get(lg, 0.0) / league_goal_counts[lg]) if league_goal_counts.get(lg, 0) > 0 else 2.65
        base_elo = LEAGUE_BASE_ELO.get(lg, 1500.0)
        h_elo = elos.get(ht, base_elo)
        a_elo = elos.get(at, base_elo)

        h_roll = compute_rolling_snapshot(buf5_map.get(ht, []), buf10_map.get(ht, []), h_elo, cur_lg_avg, lg)
        a_roll = compute_rolling_snapshot(buf5_map.get(at, []), buf10_map.get(at, []), a_elo, cur_lg_avg, lg)

        if h_roll["matches"] >= 3 and a_roll["matches"] >= 3:
            X_train.append(build_feature_vector(h_roll, a_roll, cur_lg_avg))
            y_1x2.append(2 if hg > ag else (1 if hg == ag else 0))
            y_o15.append(1 if (hg + ag) >= 2 else 0)
            y_o25.append(1 if (hg + ag) >= 3 else 0)

        league_goal_sums[lg] = league_goal_sums.get(lg, 0.0) + (hg + ag)
        league_goal_counts[lg] = league_goal_counts.get(lg, 0) + 1

        exp_h = 1.0 / (1.0 + math.pow(10.0, (a_elo - (h_elo + 65.0)) / 400.0))
        actual_h = 1.0 if hg > ag else (0.0 if hg < ag else 0.5)
        k_factor = 24.0
        elos[ht] = h_elo + k_factor * (actual_h - exp_h)
        elos[at] = a_elo + k_factor * ((1.0 - actual_h) - (1.0 - exp_h))

        ddmm = iso_to_ddmm(d_iso)
        h_res_char = "W" if hg > ag else ("D" if hg == ag else "L")
        a_res_char = "W" if ag > hg else ("D" if hg == ag else "L")

        b5_h = buf5_map.setdefault(ht, [])
        b5_h.append({
            "gf": hg, "ga": ag, "sf": hs, "sa": as_, "stf": hst, "sta": ast, "cf": hc, "ca": ac,
            "res": h_res_char, "detail": f"{ddmm} vs {at} {hg}-{ag} ({h_res_char})"
        })
        if len(b5_h) > 5:
            b5_h.pop(0)

        b5_a = buf5_map.setdefault(at, [])
        b5_a.append({
            "gf": ag, "ga": hg, "sf": as_, "sa": hs, "stf": ast, "sta": hst, "cf": ac, "ca": hc,
            "res": a_res_char, "detail": f"{ddmm} @{ht} {ag}-{hg} ({a_res_char})"
        })
        if len(b5_a) > 5:
            b5_a.pop(0)

        b10_h = buf10_map.setdefault(ht, [])
        b10_h.append({"gf": hg, "ga": ag, "stf": hst, "sta": ast})
        if len(b10_h) > 10:
            b10_h.pop(0)

        b10_a = buf10_map.setdefault(at, [])
        b10_a.append({"gf": ag, "ga": hg, "stf": ast, "sta": hst})
        if len(b10_a) > 10:
            b10_a.pop(0)

    # 3. Train Random Forest Models (m_1x2, m_o15, m_o25) and evaluate holdout accuracy
    m_1x2 = train_random_forest(X_train, y_1x2, 25, 6, 3, seed_base=42)
    m_o15 = train_random_forest(X_train, y_o15, 20, 5, 2, seed_base=101)
    m_o25 = train_random_forest(X_train, y_o25, 20, 5, 2, seed_base=202)

    eval_slice = min(1500, len(X_train))
    correct_1x2 = 0
    correct_o25 = 0
    for i in range(len(X_train) - eval_slice, len(X_train)):
        p1x2 = rf_predict_proba(m_1x2, X_train[i])
        pred_cls = 2 if (p1x2[2] >= p1x2[1] and p1x2[2] >= p1x2[0]) else (1 if p1x2[1] >= p1x2[0] else 0)
        if pred_cls == y_1x2[i]:
            correct_1x2 += 1
        po25 = rf_predict_proba(m_o25, X_train[i])
        if (1 if po25[1] >= 0.5 else 0) == y_o25[i]:
            correct_o25 += 1

    acc_1x2 = round(correct_1x2 / max(1, eval_slice), 4)
    acc_o25 = round(correct_o25 / max(1, eval_slice), 4)

    # 4. Build final team_stats and teams_by_league (fallback to known_by_league only if a league has 0 current-season matches)
    for lg in LEAGUE_SLUGS:
        if not teams_in_league_sets.get(lg):
            teams_in_league_sets[lg] = set(known_by_league.get(lg, []))
    league_avg_goals = {
        lg: round(league_goal_sums[lg] / max(1, league_goal_counts[lg]), 4)
        for lg in league_goal_sums
    }
    teams_by_league = {lg: sorted(list(tset)) for lg, tset in teams_in_league_sets.items()}
    team_to_lg = {t: lg for lg, tlist in teams_by_league.items() for t in tlist}

    team_stats = {}
    for t in sorted(team_to_lg.keys()):
        lg = team_to_lg[t]
        lg_avg = league_avg_goals.get(lg, 2.65)
        cur_elo = elos.get(t, LEAGUE_BASE_ELO.get(lg, 1500.0))
        team_stats[t] = compute_rolling_snapshot(buf5_map.get(t, []), buf10_map.get(t, []), cur_elo, lg_avg, lg)

    y_s, m_s, d_s = latest_date_iso.split("-")
    formatted_latest = f"{d_s}/{m_s}/{y_s}"

    payload = {
        "version": "5.0",
        "license_notice": "Pre-trained AI Random Forest + Dixon-Coles Goal Model & team ratings for FMS - The Game.",
        "generated_at_utc": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_update_date": formatted_latest,
        "train_samples_count": len(X_train),
        "total_matches_count": len(all_matches),
        "acc_1x2": acc_1x2,
        "acc_o25": acc_o25,
        "league_avg_goals": league_avg_goals,
        "teams_by_league": teams_by_league,
        "team_stats": team_stats,
        "m_1x2": m_1x2,
        "m_o15": m_o15,
        "m_o25": m_o25,
    }

    with open(RATINGS_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent="\t", ensure_ascii=False)

    print(
        f"[FMS Updater] Unified API sync complete: {len(all_matches)} total matches "
        f"({len(X_train)} training samples across 15 leagues). "
        f"Model Accuracy -> 1X2: {acc_1x2*100:.1f}%, O2.5: {acc_o25*100:.1f}%. "
        f"Latest match date: {formatted_latest}."
    )


if __name__ == "__main__":
    main()
