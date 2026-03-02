#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
record_kb_gids_to_mongo_v3.py

Goal
----
Persist KB-matched Admin1/Admin2 GIDs directly onto each article document so downstream
counting (and mapping) can be done WITHOUT re-running the reconciled_locations -> KB fuzzy
matching each time.

This script:
  1) Loads KB_fresh_start_V3.xlsx (or CSV) and builds per-country matching indexes.
  2) Iterates articles-YYYY-M collections in MongoDB (no leading-zero month).
  3) Parses reconciled_locations for each document.
  4) Matches each ADMIN1/ADMIN2 mention to the KB using:
       - strict+loose normalization
       - exact matching maps
       - fuzzy matching with thresholds + ambiguity guards
       - safe admin designator stripping
       - ISO3 fixes + aliases (same spirit as your counting script)
  5) Writes TWO primary fields to each doc (top-down by ISO3):
       - kb_admin1_gids: { "ISO3": ["ISO3.x_1", ...], ... }
       - kb_admin2_gids: { "ISO3": ["ISO3.x.y_2", ...], ... }
     plus a highly-informative companion field:
       - kb_admin_gid_records: a list of per-mention resolution records (methods, raw names,
         KB canonical names, best-vs-mappable info, and (when available) geojson path + featureidkey).

Admin2 rollup requirement
-------------------------
For every ADMIN2 match:
  - store its ADMIN2 gid under kb_admin2_gids[ISO3]
  - ALSO infer/store its ADMIN1 gid under kb_admin1_gids[ISO3]
    (rollup via KB row's final_admin1_gid_best; fallback to KB gid_admin1_from_admin2;
     fallback to GADM parent derivation if possible).

Important KB V3 change
----------------------
KB_fresh_start_V3.xlsx already contains "final_*" columns produced by your rescue/finalization
pipeline. We do NOT recompute multi-layer gid preference; we treat those final columns as the
primary source of truth.

Key KB columns used (if present)
--------------------------------
Admin1 (primary for matching/counted gid):
  - final_admin1_gid_best
  - final_admin1_method_best
  - final_admin1_pick_column_best
  - final_admin1_source_full_best

Admin2 (primary gid for ADMIN2 matching/counted gid):
  - final_admin2_gid_best
  - final_admin2_level_used_best
  - final_admin2_method_best
  - final_admin2_pick_column_best
  - final_admin2_source_full_best

Admin2 "mappable" companion (for future mapping pipelines):
  - final_admin2_gid_mappable
  - final_admin2_level_used_mappable
  - final_admin2_method_mappable
  - final_admin2_pick_column_mappable
  - final_admin2_source_full_mappable
  - final_admin2_geojson_path_mappable
  - final_admin2_featureidkey_mappable

Fallbacks exist if some final columns are missing:
  - Admin1: gid_admin1, manual_gid1, gid_admin1_original, gid_best
  - Admin2: gid_admin2, manual_gid2, gid_admin2_original, gid_best, admin2_level_used

Optional: boundary_gid_index parquet (highly recommended for richer metadata)
----------------------------------------------------------------------------
If you provide BOUNDARY_GID_INDEX_PARQUET, the script will attach per-gid mapping metadata
(geojson_relpath + featureidkey + source/collection) for BOTH admin1 and admin2 gids, even
when the KB row doesn't carry those fields for the chosen "final" gid.

Dependencies
------------
- pandas
- pymongo
- rapidfuzz (recommended; fallback to difflib)
- optional: pycountry

Usage
-----
Edit CONFIG below, then:
  python record_kb_gids_to_mongo_v3.py

"""

from __future__ import annotations

import ast
import datetime as _dt
import hashlib
import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pandas as pd

# ============================================================
# =========================== CONFIG =========================
# ============================================================

# --- Mongo ---
MONGO_URI: str = os.environ.get("ML4P_MONGO_URI", "").strip() or (
    "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true"
)
MONGO_DB: str = os.environ.get("ML4P_MONGO_DB", "ml4p").strip()

# Collections are named articles-YYYY-M (NO leading-zero month)
COLLECTION_TEMPLATE = "articles-{year}-{month}"

# Date range (inclusive months)
START_YM: str = "2012-01"
END_YM: str = "2025-12"

# Optionally restrict to these ISO3s (empty => keep all ISO3s found in reconciled_locations that exist in KB)
COUNTRIES: List[str] = []  # e.g. ["KEN","UGA"]

# --- KB ---
KB_PATH: Path = Path(
    os.environ.get(
        "ML4P_KB_PATH",
        "/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/KB_fresh_start_V3.xlsx",
    )
)
KB_SHEET: Optional[str | int] = 0

# Optional (recommended): boundary_gid_index parquet for richer per-gid mapping metadata
# Expected columns (as in your KB-finalizer docstring): iso3, adm_level, source, collection,
# kb_gid, geojson_relpath, featureidkey (optionally properties_json).
_BOUNDARY_ENV = os.environ.get(
    "ML4P_BOUNDARY_GID_INDEX_PARQUET",
    "/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/boundary_gid_index.parquet",
).strip()

BOUNDARY_GID_INDEX_PARQUET: Optional[Path] = Path(_BOUNDARY_ENV) if _BOUNDARY_ENV else None
if BOUNDARY_GID_INDEX_PARQUET is not None and not BOUNDARY_GID_INDEX_PARQUET.exists():
    raise FileNotFoundError(f"BOUNDARY_GID_INDEX_PARQUET not found: {BOUNDARY_GID_INDEX_PARQUET}")



# --- Output fields written to each article doc ---
OUT_FIELD_ADMIN1_GIDS: str = "kb_admin1_gids"         # dict iso3 -> list[str]
OUT_FIELD_ADMIN2_GIDS: str = "kb_admin2_gids"         # dict iso3 -> list[str]
OUT_FIELD_RECORDS: str = "kb_admin_gid_records"       # list[dict] detailed per-mention records
OUT_FIELD_UNMATCHED: str = "kb_admin_gid_unmatched"   # list[dict] raw items we couldn't match (capped)
OUT_FIELD_META: str = "kb_admin_gid_meta"             # dict with kb version + run metadata

# --- Write behavior ---
DRY_RUN: bool = bool(int(os.environ.get("ML4P_DRY_RUN", "0")))  # 1 => compute but don't write
OVERWRITE_EXISTING: bool = bool(int(os.environ.get("ML4P_OVERWRITE_EXISTING", "1")))  # 0 => skip docs that already have fields
SKIP_IF_KB_VERSION_MATCH: bool = True  # if doc has same kb_version tag, skip (faster reruns)

# --- Performance ---
BATCH_SIZE: int = int(os.environ.get("ML4P_BATCH_SIZE", "800"))
BULK_WRITE_SIZE: int = int(os.environ.get("ML4P_BULK_WRITE_SIZE", "700"))
MAX_DOCS_PER_COLLECTION: Optional[int] = None  # e.g. 50_000; None => all
MAX_UNMATCHED_PER_DOC: int = 25
STORE_UNMATCHED: bool = True

# --- Query filters ---
REQUIRE_INCLUDE_TRUE: bool = True
REQUIRE_RECONCILED_LOCATIONS: bool = True

# --- Fuzzy thresholds (keep as conservative as your counting script) ---
@dataclass
class MatchThresholds:
    # Admin1 fuzzy
    a1_accept: float = 90.0
    a1_gap: float = 8.0
    a1_tie_margin: float = 3.0
    # Admin2 fuzzy (combined score)
    a2_accept: float = 92.0
    a2_gap: float = 5.0
    a2_tie_margin: float = 2.0
    # When admin1 missing/uncertain, allow admin2-only fuzzy (very strict)
    a2_only_accept: float = 95.0
    a2_only_gap: float = 10.0
    # Gate for restricting admin2 candidates by admin1 fuzzy
    restrict_a1_gate: float = 85.0

THR: MatchThresholds = MatchThresholds()

# ============================================================
# ======================== End CONFIG ========================
# ============================================================

# ------------------------ utilities -------------------------

def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr)


def safe_literal_eval(s: str) -> Any:
    try:
        return ast.literal_eval(s)
    except Exception:
        return None


def parse_maybe_json_or_literal(x: Any) -> Any:
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                pass
        return safe_literal_eval(s)
    return None


def ym_to_year_month(ym: str) -> Tuple[int, int]:
    m = re.match(r"^\s*(\d{4})-(\d{2})\s*$", ym)
    if not m:
        raise ValueError(f"Invalid YM: {ym} (expected YYYY-MM)")
    return int(m.group(1)), int(m.group(2))


def iter_months(start_ym: str, end_ym: str) -> Iterator[Tuple[int, int]]:
    ys, ms = ym_to_year_month(start_ym)
    ye, me = ym_to_year_month(end_ym)
    y, m = ys, ms
    while (y < ye) or (y == ye and m <= me):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def as_float(x: Any, default: float = float("nan")) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, str) and x.strip() == "":
            return default
        v = float(x)
        if math.isnan(v):
            return default
        return v
    except Exception:
        return default


def _parse_level_int(x: Any) -> int:
    try:
        s = str(x or "").strip()
        if not s:
            return 0
        if s.isdigit():
            return int(s)
        return int(float(s))
    except Exception:
        return 0


# ------------------------ GID normalization -------------------------

GB_LEGACY_PREFIX_RE = re.compile(r"^GB\.(gbOpen|gbHumanitarian|gbAuthoritative)\.", re.IGNORECASE)
MALFORMED_GADM_RE = re.compile(r"^([A-Z]{3})(\d+\..*_\d+)$")
MALFORMED_GADM_DEPTH1_RE = re.compile(r"^([A-Z]{3})(\d+_\d+)$")
GADM_GID_RE = re.compile(r"^[A-Z]{3}\.[0-9A-Za-z_.-]+_\d+$")


def normalize_gid(gid: Any) -> str:
    gid = str(gid or "").strip()
    if not gid:
        return ""
    gid = GB_LEGACY_PREFIX_RE.sub("GB.", gid)
    m = MALFORMED_GADM_RE.match(gid)
    if m:
        gid = f"{m.group(1)}.{m.group(2)}"
    m2 = MALFORMED_GADM_DEPTH1_RE.match(gid)
    if m2:
        gid = f"{m2.group(1)}.{m2.group(2)}"
    gid = re.sub(r"__+", "_", gid)
    return gid.strip()


def is_gadm_gid(gid: str) -> bool:
    return bool(GADM_GID_RE.match(normalize_gid(gid)))


def gadm_dot_depth(gid: str) -> int:
    gid = normalize_gid(gid)
    if not is_gadm_gid(gid):
        return -1
    return gid.count(".")


def gadm_admin1_from_admin2_gid(gid: str) -> str:
    gid = normalize_gid(gid)
    if not is_gadm_gid(gid):
        return ""
    parts = gid.split(".")
    if len(parts) < 3:
        return ""
    iso3 = parts[0]
    admin1_idx = parts[1]
    return f"{iso3}.{admin1_idx}_1"


# ------------------------ name normalization -------------------------

try:
    import unicodedata
except Exception:
    unicodedata = None


def _strip_accents(s: str) -> str:
    if not s or unicodedata is None:
        return s
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def norm_name_strict(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = s.replace("’", "'").replace("`", "'")
    s = _strip_accents(s)
    s = s.lower()
    s = re.sub(r"[\(\)\[\]\{\}]", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


ADMIN_SUFFIX_TOKENS: Set[str] = {
    # English
    "province", "state", "district", "county", "region", "division", "municipality",
    "parish", "prefecture", "governorate", "department",
    # Spanish/Portuguese
    "provincia", "estado", "distrito", "municipio", "municipal", "municipalidad",
    "departamento",
    # French/Italian/German/etc
    "departement", "departemental", "regione",
    # Other common admin units / transliterations
    "oblast", "krai", "okrug",
    "rayon", "raion", "rajon",
    "bashkia", "obshtina", "vald",
    "wilaya",
    "canton",
    "commune", "comuna", "comune",
    "arrondissement",
    "prefectura",
    "gobernacion", "governacion",
    "district municipality",
    # Abbrev
    "dept", "mun", "dist", "gov",
}

PREFIX_DESIGNATORS: Set[str] = {
    "municipio", "municipality",
    "department", "departamento", "departement",
    "province", "provincia",
    "region", "regione",
    "prefecture", "prefectura",
    "governorate", "wilaya",
    "commune", "comuna", "comune",
    "canton",
    "rayon", "raion", "rajon",
    "bashkia", "obshtina", "vald",
    "state", "estado",
    "county", "division",
    "district",
}

CONNECTORS: Set[str] = {"of", "de", "del", "da", "do", "di", "du", "des"}


def norm_name_loose(s: Any) -> str:
    """
    Loose normalization: strips safe admin designators as prefix/suffix.

    Safety rule:
      - do NOT strip "district of X" (avoid collapsing "District of Columbia")
    """
    s2 = norm_name_strict(s)
    if not s2:
        return ""
    parts = s2.split()

    # "<designator> <connector> <rest>" e.g., "municipio de san juan"
    if len(parts) >= 3 and parts[0] in PREFIX_DESIGNATORS and parts[1] in CONNECTORS:
        if not (parts[0] == "district" and parts[1] == "of"):
            parts = parts[2:]

    # "<designator> <rest>" e.g., "bashkia tirana"
    if len(parts) >= 2 and parts[0] in PREFIX_DESIGNATORS and parts[0] != "district":
        parts = parts[1:]

    # strip suffix tokens repeatedly, keep at least one token
    while len(parts) > 1 and parts[-1] in ADMIN_SUFFIX_TOKENS:
        parts = parts[:-1]

    return " ".join(parts).strip()


def apply_admin_aliases(iso3: str, admin1: str, admin2: str) -> Tuple[str, str, str]:
    """
    Keep the same alias logic you used in the counting script (extend as needed).
    """
    iso3 = str(iso3 or "").strip().upper()
    a1 = str(admin1 or "").strip()
    a2 = str(admin2 or "").strip()

    # Saint abbreviations
    if re.match(r"^(st\.?|saint)\s+", a1.strip(), flags=re.IGNORECASE):
        a1 = re.sub(r"^st\.?\s+", "Saint ", a1.strip(), flags=re.IGNORECASE)
    if re.match(r"^(st\.?|saint)\s+", a2.strip(), flags=re.IGNORECASE):
        a2 = re.sub(r"^st\.?\s+", "Saint ", a2.strip(), flags=re.IGNORECASE)

    # Malacca vs Melaka
    if iso3 == "MYS" and a1.strip().lower() == "malacca":
        a1 = "Melaka"

    # Dominican Republic variants
    if iso3 == "DOM" and a1.strip().lower() == "monsignor nouel":
        a1 = "Monseñor Nouel"
    if iso3 == "DOM" and a1.strip().lower() == "montecristi":
        a1 = "Monte Cristi"

    # Philippines Manila / NCR
    if iso3 == "PHL":
        if a1.strip().lower() in {"ncr", "national capital region", "metro manila", "manila"}:
            a1 = "National Capital Region"

    return iso3, a1, a2


_US_STATES = {
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware","florida","georgia",
    "hawaii","idaho","illinois","indiana","iowa","kansas","kentucky","louisiana","maine","maryland","massachusetts",
    "michigan","minnesota","mississippi","missouri","montana","nebraska","nevada","new hampshire","new jersey","new mexico",
    "new york","north carolina","north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island","south carolina",
    "south dakota","tennessee","texas","utah","vermont","virginia","washington","west virginia","wisconsin","wyoming",
    "district of columbia",
}
_UK_CONSTITUENTS = {"england", "scotland", "wales", "northern ireland"}


def fix_iso3_common_errors(iso3: str, admin1: str, admin2: str) -> Tuple[str, str]:
    iso3 = str(iso3 or "").strip().upper()
    a1 = str(admin1 or "").strip().lower()
    a2 = str(admin2 or "").strip().lower()

    if iso3 == "US":
        return "USA", "fix_iso3_us_to_usa"
    if iso3 == "UK":
        return "GBR", "fix_iso3_uk_to_gbr"
    if iso3 == "UKR" and (a1 in _UK_CONSTITUENTS or a2 in _UK_CONSTITUENTS):
        return "GBR", "fix_iso3_ukr_to_gbr"
    if iso3 in ("VIR", "AUS") and (a1 in _US_STATES or a2 in _US_STATES):
        return "USA", f"fix_iso3_{iso3}_to_usa_us_state"
    return iso3, ""


# Optional ISO3 from country name
try:
    import pycountry  # type: ignore
except Exception:
    pycountry = None


def iso3_from_country_name(name: str) -> str:
    if not pycountry:
        return ""
    nm = (name or "").strip()
    if not nm or nm.lower() == "unknown":
        return ""
    try:
        hit = pycountry.countries.search_fuzzy(nm)[0]
        return str(hit.alpha_3).upper()
    except Exception:
        return ""


# ------------------------ KB loading/prep (FINAL columns) -------------------------

def load_kb(kb_path: Path, sheet: Optional[str | int] = None) -> pd.DataFrame:
    if not kb_path.exists():
        raise FileNotFoundError(f"KB not found: {kb_path}")
    ext = kb_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(kb_path, sheet_name=sheet if sheet is not None else 0, dtype=str)
    else:
        df = pd.read_csv(kb_path, dtype=str, low_memory=False)
    df = df.fillna("")
    if "Admin0_ISO3" not in df.columns:
        raise ValueError("KB missing required column: Admin0_ISO3")
    return df


def build_admin0_map(kb: pd.DataFrame) -> Dict[str, str]:
    """
    Map normalized Admin0 name -> ISO3, plus ISO3 -> ISO3.
    """
    m: Dict[str, str] = {}
    if "Admin0" in kb.columns and "Admin0_ISO3" in kb.columns:
        for a0, iso3 in kb[["Admin0", "Admin0_ISO3"]].drop_duplicates().itertuples(index=False):
            iso3 = str(iso3).strip().upper()
            if iso3:
                m[norm_name_strict(a0)] = iso3
    for iso3 in kb["Admin0_ISO3"].drop_duplicates().astype(str).str.strip().str.upper().tolist():
        if iso3:
            m[norm_name_strict(iso3)] = iso3
    return m


def prepare_kb_final_df(kb: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize/standardize the KB columns we need for matching using KB V3 final columns.
    """
    kb = kb.copy().fillna("")
    kb["Admin0_ISO3"] = kb["Admin0_ISO3"].astype(str).str.strip().str.upper()

    # Display names
    kb["admin1_disp"] = kb.get("admin1_matched_name", "").astype(str)
    kb.loc[kb["admin1_disp"].astype(str).str.strip() == "", "admin1_disp"] = kb.get("Admin1", "").astype(str)

    kb["admin2_disp"] = kb.get("admin2_matched_name", "").astype(str)
    kb.loc[kb["admin2_disp"].astype(str).str.strip() == "", "admin2_disp"] = kb.get("Admin2", "").astype(str)

    # Quality scores (best available)
    kb["admin1_quality"] = kb.get("admin1_canonical_score", "").apply(as_float)
    kb.loc[kb["admin1_quality"].isna(), "admin1_quality"] = kb.get("admin1_match_score", "").apply(as_float)
    kb.loc[kb["admin1_quality"].isna(), "admin1_quality"] = kb.get("admin1_score", "").apply(as_float)
    kb["admin1_quality"] = kb["admin1_quality"].fillna(0.0)

    kb["admin2_quality"] = kb.get("admin2_match_score", "").apply(as_float)
    kb.loc[kb["admin2_quality"].isna(), "admin2_quality"] = kb.get("admin2_score", "").apply(as_float)
    kb["admin2_quality"] = kb["admin2_quality"].fillna(0.0)

    # Final primary gids
    if "final_admin1_gid_best" in kb.columns:
        kb["gid_admin1_final"] = kb["final_admin1_gid_best"]
        kb["admin1_method_final"] = kb.get("final_admin1_method_best", "")
        kb["admin1_pickcol_final"] = kb.get("final_admin1_pick_column_best", "")
        kb["admin1_source_full_final"] = kb.get("final_admin1_source_full_best", "")
    else:
        # fallback logic if final columns missing
        kb["gid_admin1_final"] = kb.get("manual_gid1", kb.get("gid_admin1", ""))
        kb["admin1_method_final"] = kb.get("admin1_match_method", "")
        kb["admin1_pickcol_final"] = "fallback_no_final_admin1"
        kb["admin1_source_full_final"] = kb.get("admin1_source", "")

    # Admin2 BEST (for matching/admin2 counts)
    if "final_admin2_gid_best" in kb.columns:
        kb["gid_admin2_best"] = kb["final_admin2_gid_best"]
        kb["admin2_level_best"] = kb.get("final_admin2_level_used_best", "").apply(_parse_level_int)
        kb["admin2_method_best"] = kb.get("final_admin2_method_best", "")
        kb["admin2_pickcol_best"] = kb.get("final_admin2_pick_column_best", "")
        kb["admin2_source_full_best"] = kb.get("final_admin2_source_full_best", "")
    else:
        kb["gid_admin2_best"] = kb.get("manual_gid2", kb.get("gid_admin2", ""))
        kb["admin2_level_best"] = kb.get("admin2_level_used", "").apply(_parse_level_int)
        kb["admin2_method_best"] = kb.get("admin2_match_method", "")
        kb["admin2_pickcol_best"] = "fallback_no_final_admin2_best"
        kb["admin2_source_full_best"] = kb.get("admin2_source", "")

    # Admin2 MAPPABLE companion (optional but very useful)
    if "final_admin2_gid_mappable" in kb.columns:
        kb["gid_admin2_mappable"] = kb["final_admin2_gid_mappable"]
        kb["admin2_level_mappable"] = kb.get("final_admin2_level_used_mappable", "").apply(_parse_level_int)
        kb["admin2_method_mappable"] = kb.get("final_admin2_method_mappable", "")
        kb["admin2_pickcol_mappable"] = kb.get("final_admin2_pick_column_mappable", "")
        kb["admin2_source_full_mappable"] = kb.get("final_admin2_source_full_mappable", "")
        kb["admin2_geojson_path_mappable"] = kb.get("final_admin2_geojson_path_mappable", "")
        kb["admin2_featureidkey_mappable"] = kb.get("final_admin2_featureidkey_mappable", "")
    else:
        kb["gid_admin2_mappable"] = kb["gid_admin2_best"]
        kb["admin2_level_mappable"] = kb["admin2_level_best"]
        kb["admin2_method_mappable"] = "fallback_no_final_admin2_mappable"
        kb["admin2_pickcol_mappable"] = "fallback_no_final_admin2_mappable"
        kb["admin2_source_full_mappable"] = kb.get("admin2_source", "")
        kb["admin2_geojson_path_mappable"] = kb.get("admin2_geojson_path", "")
        kb["admin2_featureidkey_mappable"] = kb.get("admin2_featureidkey", "")

    # Admin1 rollup helpers (for admin2 -> admin1)
    kb["gid_admin1_from_admin2"] = kb.get("gid_admin1_from_admin2", "")
    kb["gid_admin1_raw"] = kb.get("gid_admin1", "")

    # Normalize gid columns
    for c in [
        "gid_admin1_final",
        "gid_admin2_best",
        "gid_admin2_mappable",
        "gid_admin1_from_admin2",
        "gid_admin1_raw",
    ]:
        kb[c] = kb[c].apply(normalize_gid)

    # Normalize some extra helpful columns if present
    for c in ["gid_admin1_original", "gid_admin2_original", "gid_best", "manual_gid1", "manual_gid2"]:
        if c in kb.columns:
            kb[c] = kb[c].apply(normalize_gid)

    return kb


# ------------------------ Optional boundary_gid_index metadata -------------------------

@dataclass
class BoundaryGidMeta:
    iso3: str
    adm_level: int
    source: str
    collection: str
    geojson_relpath: str
    featureidkey: str


def load_boundary_gid_index(path: Optional[Path]) -> Dict[str, BoundaryGidMeta]:
    """
    Load a gid->meta mapping from a boundary_gid_index parquet.

    Returns:
        dict: kb_gid -> BoundaryGidMeta

    If the parquet is missing or unreadable, returns {}.
    """
    if path is None:
        return {}
    p = Path(path)
    if not p.exists():
        eprint(f"[boundary_gid_index] not found: {p}")
        return {}

    try:
        df = pd.read_parquet(p)
    except Exception as e:
        eprint(f"[boundary_gid_index] failed to read parquet {p}: {e}")
        return {}

    # infer likely columns
    cols = {c.lower(): c for c in df.columns}
    def _col(*names: str) -> str:
        for n in names:
            if n in cols:
                return cols[n]
        return ""

    c_gid = _col("kb_gid", "gid", "kb_gid_1", "kb_gid_2")
    c_iso = _col("iso3", "admin0_iso3")
    c_lvl = _col("adm_level", "admin_level", "level")
    c_src = _col("source", "source_full")
    c_coll = _col("collection")
    c_path = _col("geojson_relpath", "geojson_path", "geojson_rel_path")
    c_fid = _col("featureidkey", "feature_id_key", "featureid_key")

    missing = [x for x in [("kb_gid", c_gid), ("iso3", c_iso), ("adm_level", c_lvl), ("geojson_relpath", c_path), ("featureidkey", c_fid)] if not x[1]]
    if missing:
        eprint(f"[boundary_gid_index] missing required columns in {p}. Found columns={list(df.columns)[:30]}")
        return {}

    meta: Dict[str, BoundaryGidMeta] = {}
    for _, r in df.iterrows():
        gid = normalize_gid(r.get(c_gid, ""))
        if not gid:
            continue
        try:
            lvl = int(_parse_level_int(r.get(c_lvl, 0)))
        except Exception:
            lvl = 0
        iso3 = str(r.get(c_iso, "") or "").strip().upper()
        if not iso3:
            # try infer iso3 from gid prefix
            if len(gid) >= 3:
                iso3 = gid[:3].upper()
        src = str(r.get(c_src, "") or "").strip()
        coll = str(r.get(c_coll, "") or "").strip() if c_coll else ""
        path_rel = str(r.get(c_path, "") or "").strip()
        fid = str(r.get(c_fid, "") or "").strip()

        # Keep the first-seen meta for a gid unless the existing one lacks path/fid and new one has them.
        if gid not in meta:
            meta[gid] = BoundaryGidMeta(
                iso3=iso3,
                adm_level=lvl,
                source=src,
                collection=coll,
                geojson_relpath=path_rel,
                featureidkey=fid,
            )
        else:
            cur = meta[gid]
            if (not cur.geojson_relpath and path_rel) or (not cur.featureidkey and fid):
                meta[gid] = BoundaryGidMeta(
                    iso3=iso3,
                    adm_level=lvl or cur.adm_level,
                    source=src or cur.source,
                    collection=coll or cur.collection,
                    geojson_relpath=path_rel or cur.geojson_relpath,
                    featureidkey=fid or cur.featureidkey,
                )

    eprint(f"[boundary_gid_index] loaded meta for {len(meta):,} gids from {p}")
    return meta


# ------------------------ CountryIndex structures -------------------------

@dataclass
class Admin1Rec:
    gid: str
    iso3: str
    admin1: str
    quality: float
    method: str = ""
    pick_column: str = ""
    source_full: str = ""
    # hints (may be blank if not knowable from KB alone)
    geojson_path_hint: str = ""
    featureidkey_hint: str = ""
    # optional external meta (preferred if provided)
    boundary_meta: Optional[BoundaryGidMeta] = None
    # extra alternatives (useful for future patches/diagnostics)
    alternative_gids: Set[str] = field(default_factory=set)


@dataclass
class Admin2Rec:
    gid_best: str
    gid_mappable: str
    iso3: str
    admin1: str
    admin2: str
    admin2_level_best: int
    admin2_level_mappable: int
    # rollup admin1 (best guess)
    gid_admin1_rollup: str
    quality: float
    method_best: str = ""
    pick_column_best: str = ""
    source_full_best: str = ""
    method_mappable: str = ""
    pick_column_mappable: str = ""
    source_full_mappable: str = ""
    geojson_path_mappable: str = ""
    featureidkey_mappable: str = ""
    boundary_meta_best: Optional[BoundaryGidMeta] = None
    boundary_meta_mappable: Optional[BoundaryGidMeta] = None
    alternative_gids: Set[str] = field(default_factory=set)
    gid_admin1_alternative_gids: Set[str] = field(default_factory=set)


@dataclass
class CountryIndex:
    iso3: str
    admin0_name: str
    a1_records: Dict[str, Admin1Rec] = field(default_factory=dict)          # gid -> rec
    a2_records: Dict[str, Admin2Rec] = field(default_factory=dict)          # gid_best -> rec

    # matching maps
    a1_exact: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))            # strict name -> {gid}
    a1_loose: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))            # loose name -> {gid}

    a2_exact: Dict[Tuple[str, str], Set[str]] = field(default_factory=lambda: defaultdict(set)) # (a1s,a2s) -> {gid2}
    a2_loose: Dict[Tuple[str, str], Set[str]] = field(default_factory=lambda: defaultdict(set)) # (a1l,a2l) -> {gid2}
    a2_only: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))            # a2 strict -> {gid2}

    # fuzzy choice scaffolding
    a1_choice_gids: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))     # loose name -> {gid}
    a2_choice_cands: Dict[str, Set[Tuple[str, str, str]]] = field(default_factory=lambda: defaultdict(set)) # a2_loose -> {(gid2, gid1_rollup, a1_loose)}
    a2_choices_by_gid1: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set)) # gid1_rollup -> {a2_loose}


def _choose_best_gid(gids: Set[str], records: Dict[str, Any]) -> str:
    """
    Choose the best gid among candidates by record quality.
    """
    if not gids:
        return ""
    if len(gids) == 1:
        return next(iter(gids))
    best_gid, best_q = "", -1e18
    for g in gids:
        rec = records.get(g)
        q = float(getattr(rec, "quality", 0.0) if rec is not None else 0.0)
        if q > best_q:
            best_q, best_gid = q, g
    return best_gid


def _split_gid_list(s: Any) -> List[str]:
    """
    Helper to interpret alt gid columns that may be single, pipe-separated, or json list.
    """
    if s is None:
        return []
    if isinstance(s, list):
        return [normalize_gid(x) for x in s if normalize_gid(x)]
    if isinstance(s, str):
        t = s.strip()
        if not t:
            return []
        if t.startswith("[") and t.endswith("]"):
            obj = parse_maybe_json_or_literal(t)
            if isinstance(obj, list):
                return [normalize_gid(x) for x in obj if normalize_gid(x)]
        if "|" in t:
            return [normalize_gid(x) for x in t.split("|") if normalize_gid(x)]
        return [normalize_gid(t)]
    return []


def build_country_index(
    kb_p: pd.DataFrame,
    *,
    iso3: str,
    boundary_meta: Optional[Dict[str, BoundaryGidMeta]] = None,
) -> CountryIndex:
    iso3 = str(iso3).strip().upper()
    df = kb_p[kb_p["Admin0_ISO3"] == iso3].copy()
    if df.empty:
        raise ValueError(f"No KB rows for ISO3={iso3}")

    admin0_name = ""
    if "Admin0" in df.columns:
        vals = df["Admin0"].astype(str).str.strip()
        vals = vals[vals != ""]
        if not vals.empty:
            admin0_name = vals.value_counts().idxmax()

    idx = CountryIndex(iso3=iso3, admin0_name=admin0_name)

    boundary_meta = boundary_meta or {}

    # 1) Build Admin1/Admin2 records (dedupe by primary gid key)
    for _, r in df.iterrows():
        gid1 = normalize_gid(r.get("gid_admin1_final") or "")
        if gid1:
            alt: Set[str] = set()
            # gather some useful alternatives if present
            for c in ["gid_admin1_original", "gid_admin1_prev_consistency", "gid_best", "manual_gid1", "gid_admin1_raw", "gid_admin1_from_admin2"]:
                if c in r.index:
                    v = normalize_gid(r.get(c) or "")
                    if v and v != gid1:
                        alt.add(v)

            rec = Admin1Rec(
                gid=gid1,
                iso3=iso3,
                admin1=str(r.get("admin1_disp") or "").strip(),
                quality=float(as_float(r.get("admin1_quality"), default=0.0) or 0.0),
                method=str(r.get("admin1_method_final") or ""),
                pick_column=str(r.get("admin1_pickcol_final") or ""),
                source_full=str(r.get("admin1_source_full_final") or ""),
                geojson_path_hint=str(r.get("admin1_geojson_path") or ""),
                featureidkey_hint=str(r.get("admin1_featureidkey") or ""),
                boundary_meta=boundary_meta.get(gid1),
                alternative_gids=alt,
            )
            cur = idx.a1_records.get(gid1)
            if cur is None:
                idx.a1_records[gid1] = rec
            else:
                cur.alternative_gids |= rec.alternative_gids
                if rec.quality > cur.quality or (rec.quality == cur.quality and len(rec.admin1) > len(cur.admin1)):
                    rec.alternative_gids = cur.alternative_gids
                    idx.a1_records[gid1] = rec

        # Admin2 best gid (only include if the BEST level is actually admin2)
        gid2_best = normalize_gid(r.get("gid_admin2_best") or "")
        lvl_best = int(r.get("admin2_level_best") or 0)

        if gid2_best and lvl_best >= 2:
            gid2_map = normalize_gid(r.get("gid_admin2_mappable") or "")
            lvl_map = int(r.get("admin2_level_mappable") or 0)

            # Rollup admin1 gid for this admin2
            gid1_roll = normalize_gid(r.get("gid_admin1_final") or "") \
                        or normalize_gid(r.get("gid_admin1_from_admin2") or "") \
                        or normalize_gid(r.get("gid_admin1_raw") or "")
            # add GADM parent as alternative rollup if possible
            roll_alts: Set[str] = set()
            if is_gadm_gid(gid2_best) and gadm_dot_depth(gid2_best) >= 2:
                parent = gadm_admin1_from_admin2_gid(gid2_best)
                if parent and parent != gid1_roll:
                    roll_alts.add(parent)

            alt2: Set[str] = set()
            for c in ["gid_admin2_original", "gid_admin2_prev_consistency", "gid_best", "manual_gid2"]:
                if c in r.index:
                    v = normalize_gid(r.get(c) or "")
                    if v and v != gid2_best:
                        alt2.add(v)

            rec2 = Admin2Rec(
                gid_best=gid2_best,
                gid_mappable=gid2_map,
                iso3=iso3,
                admin1=str(r.get("admin1_disp") or "").strip(),
                admin2=str(r.get("admin2_disp") or "").strip(),
                admin2_level_best=lvl_best,
                admin2_level_mappable=lvl_map,
                gid_admin1_rollup=gid1_roll,
                quality=float(as_float(r.get("admin2_quality"), default=0.0) or 0.0),
                method_best=str(r.get("admin2_method_best") or ""),
                pick_column_best=str(r.get("admin2_pickcol_best") or ""),
                source_full_best=str(r.get("admin2_source_full_best") or ""),
                method_mappable=str(r.get("admin2_method_mappable") or ""),
                pick_column_mappable=str(r.get("admin2_pickcol_mappable") or ""),
                source_full_mappable=str(r.get("admin2_source_full_mappable") or ""),
                geojson_path_mappable=str(r.get("admin2_geojson_path_mappable") or ""),
                featureidkey_mappable=str(r.get("admin2_featureidkey_mappable") or ""),
                boundary_meta_best=boundary_meta.get(gid2_best),
                boundary_meta_mappable=boundary_meta.get(gid2_map) if gid2_map else None,
                alternative_gids=alt2,
                gid_admin1_alternative_gids=roll_alts,
            )
            cur2 = idx.a2_records.get(gid2_best)
            if cur2 is None:
                idx.a2_records[gid2_best] = rec2
            else:
                cur2.alternative_gids |= rec2.alternative_gids
                cur2.gid_admin1_alternative_gids |= rec2.gid_admin1_alternative_gids
                if rec2.quality > cur2.quality or (rec2.quality == cur2.quality and len(rec2.admin2) > len(cur2.admin2)):
                    rec2.alternative_gids = cur2.alternative_gids
                    rec2.gid_admin1_alternative_gids = cur2.gid_admin1_alternative_gids
                    idx.a2_records[gid2_best] = rec2

    # 2) name variants -> sets (map names to primary gids)
    def _name_vars(row: pd.Series) -> Tuple[Set[str], Set[str]]:
        a1_vars, a2_vars = set(), set()
        for c in ["Admin1", "admin1_disp", "admin1_matched_name", "admin1_matched_name_original", "admin1_from_admin2"]:
            if c in row.index:
                v = str(row.get(c) or "").strip()
                if v and v.lower() not in {"unknown", "nan"}:
                    a1_vars.add(v)
        for c in ["Admin2", "admin2_disp", "admin2_matched_name", "admin2_matched_name_original"]:
            if c in row.index:
                v = str(row.get(c) or "").strip()
                if v and v.lower() not in {"unknown", "nan"}:
                    a2_vars.add(v)
        return a1_vars, a2_vars

    for _, r in df.iterrows():
        gid1 = normalize_gid(r.get("gid_admin1_final") or "")
        gid2_best = normalize_gid(r.get("gid_admin2_best") or "")
        lvl_best = int(r.get("admin2_level_best") or 0)

        a1_vars, a2_vars = _name_vars(r)

        if gid1:
            for nm in a1_vars:
                a1s = norm_name_strict(nm)
                a1l = norm_name_loose(nm)
                if a1s:
                    idx.a1_exact[a1s].add(gid1)
                if a1l:
                    idx.a1_loose[a1l].add(gid1)
                    idx.a1_choice_gids[a1l].add(gid1)

        if gid2_best and lvl_best >= 2:
            # Use the record's rollup admin1 gid when available
            gid1_roll = normalize_gid(r.get("gid_admin1_final") or "") \
                        or normalize_gid(r.get("gid_admin1_from_admin2") or "") \
                        or normalize_gid(r.get("gid_admin1_raw") or "")

            for nm1 in a1_vars:
                a1s = norm_name_strict(nm1)
                a1l = norm_name_loose(nm1)
                for nm2 in a2_vars:
                    a2s = norm_name_strict(nm2)
                    a2l = norm_name_loose(nm2)
                    if a1s and a2s:
                        idx.a2_exact[(a1s, a2s)].add(gid2_best)
                    if a1l and a2l:
                        idx.a2_loose[(a1l, a2l)].add(gid2_best)
                        idx.a2_choice_cands[a2l].add((gid2_best, gid1_roll, a1l))
                        if gid1_roll:
                            idx.a2_choices_by_gid1[gid1_roll].add(a2l)
                    if a2s:
                        idx.a2_only[a2s].add(gid2_best)

    return idx


# ------------------------ Matching (rapidfuzz) -------------------------

try:
    from rapidfuzz import fuzz, process  # type: ignore
    HAVE_RAPIDFUZZ = True
except Exception:
    HAVE_RAPIDFUZZ = False
    import difflib

    def _ratio(a: str, b: str) -> float:
        return 100.0 * difflib.SequenceMatcher(None, a, b).ratio()

    class fuzz:  # type: ignore
        @staticmethod
        def token_set_ratio(a: str, b: str) -> float:
            return _ratio(a, b)

    class process:  # type: ignore
        @staticmethod
        def extract(query: str, choices: Sequence[str], scorer, limit: int = 5):
            scored = [(c, scorer(query, c), i) for i, c in enumerate(choices)]
            scored.sort(key=lambda x: x[1], reverse=True)
            return scored[:limit]


def resolve_admin1_gid(idx: CountryIndex, admin1_raw: str, *, thr: MatchThresholds, fuzzy_limit: int = 8) -> Tuple[str, str]:
    a1 = str(admin1_raw or "").strip()
    if not a1:
        return "", "missing"
    a1s = norm_name_strict(a1)
    a1l = norm_name_loose(a1)
    if not a1l:
        return "", "missing"

    cands = idx.a1_exact.get(a1s, set())
    if cands:
        gid = _choose_best_gid(cands, idx.a1_records)
        return gid, "kb_exact"

    cands = idx.a1_loose.get(a1l, set())
    if cands:
        gid = _choose_best_gid(cands, idx.a1_records)
        return gid, "kb_loose_exact" if len(cands) == 1 else "kb_loose_multi_best"

    choices = list(idx.a1_choice_gids.keys())
    if not choices:
        return "", "no_admin1_choices"

    matches = process.extract(a1l, choices, scorer=fuzz.token_set_ratio, limit=fuzzy_limit)
    if not matches:
        return "", "no_admin1_fuzzy"

    gid_best_score: Dict[str, float] = defaultdict(float)
    for choice, score, _ in matches:
        for gid in idx.a1_choice_gids.get(choice, set()):
            gid_best_score[gid] = max(gid_best_score[gid], float(score))

    sorted_g = sorted(gid_best_score.items(), key=lambda x: x[1], reverse=True)
    best_gid, best_score = sorted_g[0]
    second_score = sorted_g[1][1] if len(sorted_g) > 1 else -1.0
    n_close = sum(1 for _, s in sorted_g if best_score - s <= thr.a1_tie_margin)

    if (best_score >= thr.a1_accept) and (((best_score - second_score) >= thr.a1_gap) or best_score >= 97.0) and n_close == 1:
        return best_gid, f"fuzzy_a1:{best_score:.1f}"

    return "", f"ambiguous_a1:{best_score:.1f}:n{n_close}"


def resolve_admin2_gid(
    idx: CountryIndex,
    admin1_raw: str,
    admin2_raw: str,
    *,
    thr: MatchThresholds,
    fuzzy_limit: int = 12,
) -> Tuple[str, str]:
    a1 = str(admin1_raw or "").strip()
    a2 = str(admin2_raw or "").strip()
    if not a2:
        return "", "missing"

    a1s = norm_name_strict(a1)
    a2s = norm_name_strict(a2)
    a1l = norm_name_loose(a1)
    a2l = norm_name_loose(a2)
    if not a2l:
        return "", "missing"

    cands = idx.a2_exact.get((a1s, a2s), set())
    if cands:
        gid = _choose_best_gid(cands, idx.a2_records)
        return gid, "kb_exact"

    cands = idx.a2_loose.get((a1l, a2l), set())
    if cands:
        gid = _choose_best_gid(cands, idx.a2_records)
        return gid, "kb_loose_exact" if len(cands) == 1 else "kb_loose_multi_best"

    cands = idx.a2_only.get(a2s, set())
    if cands and len(cands) == 1:
        gid = next(iter(cands))
        return gid, "kb_admin2_only_unique"

    # fuzzy: restrict by admin1 if decent
    restrict_gid1: Set[str] = set()
    if a1:
        gid1, m1 = resolve_admin1_gid(
            idx,
            a1,
            thr=MatchThresholds(a1_accept=thr.restrict_a1_gate, a1_gap=0, a1_tie_margin=5),
            fuzzy_limit=6,
        )
        if gid1 and not m1.startswith("ambiguous"):
            # NOTE: we use gid1 as the rollup key in idx.a2_choices_by_gid1
            restrict_gid1.add(gid1)

    if restrict_gid1:
        candidate_choices: Set[str] = set()
        for g1 in restrict_gid1:
            candidate_choices |= idx.a2_choices_by_gid1.get(g1, set())
        if not candidate_choices:
            candidate_choices = set(idx.a2_choice_cands.keys())
    else:
        candidate_choices = set(idx.a2_choice_cands.keys())

    if not candidate_choices:
        return "", "no_admin2_choices"

    matches = process.extract(a2l, list(candidate_choices), scorer=fuzz.token_set_ratio, limit=fuzzy_limit)
    if not matches:
        return "", "no_admin2_fuzzy"

    gid2_best_score: Dict[str, float] = defaultdict(float)
    gid2_best_a2: Dict[str, float] = defaultdict(float)

    for a2_choice, score_a2, _ in matches:
        for (gid2, gid1_roll, a1_choice) in idx.a2_choice_cands.get(a2_choice, set()):
            score_a1 = float(fuzz.token_set_ratio(a1l, a1_choice)) if a1l else 0.0
            combined = float(score_a2) if not a1l else (0.65 * float(score_a2) + 0.35 * score_a1)
            gid2_best_score[gid2] = max(gid2_best_score[gid2], combined)
            gid2_best_a2[gid2] = max(gid2_best_a2[gid2], float(score_a2))

    sorted_g = sorted(gid2_best_score.items(), key=lambda x: x[1], reverse=True)
    best_gid, best_score = sorted_g[0]
    second_score = sorted_g[1][1] if len(sorted_g) > 1 else -1.0
    n_close = sum(1 for _, s in sorted_g if best_score - s <= thr.a2_tie_margin)

    if (best_score >= thr.a2_accept) and (((best_score - second_score) >= thr.a2_gap) or best_score >= 98.0) and n_close == 1:
        return best_gid, f"fuzzy_a2:{best_score:.1f}"

    # if admin1 missing, allow very strict a2-only
    if not a1l:
        best_a2 = gid2_best_a2.get(best_gid, 0.0)
        a2_scores = sorted(gid2_best_a2.values(), reverse=True)
        second_a2 = a2_scores[1] if len(a2_scores) > 1 else -1.0
        if (best_a2 >= thr.a2_only_accept) and ((best_a2 - second_a2) >= thr.a2_only_gap):
            return best_gid, f"fuzzy_a2only:{best_a2:.1f}"

    return "", f"ambiguous_a2:{best_score:.1f}:n{n_close}"


# ------------------------ reconciled_locations parsing -------------------------

def iter_locations_from_reconciled(reconciled_locations: Any) -> Iterator[Dict[str, str]]:
    obj = parse_maybe_json_or_literal(reconciled_locations)
    if obj is None:
        return

    # A) {"ADMIN1": [...], "ADMIN2": [...]}
    if isinstance(obj, dict) and any(str(k).strip().upper() in ("ADMIN1", "ADMIN2", "ADM1", "ADM2") for k in obj.keys()):
        keymap: Dict[str, Any] = {}
        for k in obj.keys():
            ku = str(k).strip().upper()
            if ku in ("ADMIN1", "ADM1"):
                keymap["ADMIN1"] = k
            elif ku in ("ADMIN2", "ADM2"):
                keymap["ADMIN2"] = k

        for lvl in ("ADMIN1", "ADMIN2"):
            raw_vals = obj.get(keymap.get(lvl, lvl), None) if keymap else obj.get(lvl, None)
            if raw_vals is None:
                raw_vals = obj.get(lvl.lower(), None)
            if raw_vals is None:
                continue

            vals = [raw_vals] if isinstance(raw_vals, str) else (raw_vals if isinstance(raw_vals, list) else [])
            for s in vals:
                s = str(s or "").strip()
                if not s:
                    continue
                parts = s.split("|||")
                if len(parts) == 1:
                    # sometimes comma separated
                    parts = [p.strip() for p in re.split(r"\s*,\s*", s)]
                while len(parts) < 3:
                    parts.append("Unknown")
                a0, a1, a2 = parts[0].strip(), parts[1].strip(), parts[2].strip()
                if lvl == "ADMIN1":
                    a2 = "Unknown"
                yield {"location_level": lvl, "admin0": a0, "admin1": a1, "admin2": a2}
        return

    # B) older dict mapping place->info dict
    if isinstance(obj, dict):
        for _, info in obj.items():
            if not isinstance(info, dict):
                continue
            lvl = str(info.get("location_level") or "").strip().upper()
            if lvl not in ("ADMIN1", "ADMIN2"):
                continue
            yield {
                "location_level": lvl,
                "admin0": str(info.get("admin0") or "").strip(),
                "admin1": str(info.get("admin1") or "").strip(),
                "admin2": str(info.get("admin2") or "").strip(),
            }
        return

    # C) list of dicts
    if isinstance(obj, list):
        for info in obj:
            if not isinstance(info, dict):
                continue
            lvl = str(info.get("location_level") or "").strip().upper()
            if lvl not in ("ADMIN1", "ADMIN2"):
                continue
            yield {
                "location_level": lvl,
                "admin0": str(info.get("admin0") or "").strip(),
                "admin1": str(info.get("admin1") or "").strip(),
                "admin2": str(info.get("admin2") or "").strip(),
            }


# ------------------------ ISO3 resolution (general, not target-only) -------------------------

ISO3_ONLY_RE = re.compile(r"^[A-Z]{3}$")


def resolve_iso3_general(admin0_raw: str, admin0_map: Dict[str, str]) -> Tuple[str, str]:
    """
    Resolve ISO3 from the reconciled location's admin0 field.
    """
    a0 = str(admin0_raw or "").strip()
    if not a0:
        return "", "missing_admin0"

    if ISO3_ONLY_RE.match(a0.upper()):
        return a0.upper(), "admin0_iso3_direct"

    iso3 = admin0_map.get(norm_name_strict(a0), "")
    if iso3:
        return iso3, "kb_admin0_map"

    iso3 = iso3_from_country_name(a0)
    if iso3:
        return iso3, "pycountry"

    return "", "no_iso3"


# ------------------------ Per-document resolution -------------------------

@dataclass
class DocResolution:
    admin1_gids: Dict[str, List[str]]
    admin2_gids: Dict[str, List[str]]
    records: List[Dict[str, Any]]
    unmatched: List[Dict[str, Any]]
    stats: Dict[str, Any]


def _meta_for_admin1(rec: Admin1Rec) -> Dict[str, Any]:
    m: Dict[str, Any] = {
        "kb_admin1": rec.admin1,
        "quality": rec.quality,
        "method": rec.method,
        "pick_column": rec.pick_column,
        "source_full": rec.source_full,
    }
    # Prefer boundary_gid_index meta if available
    if rec.boundary_meta is not None:
        m["boundary"] = {
            "iso3": rec.boundary_meta.iso3,
            "adm_level": rec.boundary_meta.adm_level,
            "source": rec.boundary_meta.source,
            "collection": rec.boundary_meta.collection,
            "geojson_relpath": rec.boundary_meta.geojson_relpath,
            "featureidkey": rec.boundary_meta.featureidkey,
        }
    else:
        # hints from KB columns (may not correspond to chosen final gid in rare cases)
        if rec.geojson_path_hint or rec.featureidkey_hint:
            m["kb_hints"] = {
                "geojson_path": rec.geojson_path_hint,
                "featureidkey": rec.featureidkey_hint,
            }
    if rec.alternative_gids:
        m["alternative_gids"] = sorted(rec.alternative_gids)
    return m


def _meta_for_admin2(rec: Admin2Rec) -> Dict[str, Any]:
    m: Dict[str, Any] = {
        "kb_admin1": rec.admin1,
        "kb_admin2": rec.admin2,
        "quality": rec.quality,
        "gid_admin1_rollup": rec.gid_admin1_rollup,
        "gid_admin1_alternative_rollups": sorted(rec.gid_admin1_alternative_gids) if rec.gid_admin1_alternative_gids else [],
        "best": {
            "gid": rec.gid_best,
            "level_used": rec.admin2_level_best,
            "method": rec.method_best,
            "pick_column": rec.pick_column_best,
            "source_full": rec.source_full_best,
        },
        "mappable": {
            "gid": rec.gid_mappable,
            "level_used": rec.admin2_level_mappable,
            "method": rec.method_mappable,
            "pick_column": rec.pick_column_mappable,
            "source_full": rec.source_full_mappable,
            "geojson_path": rec.geojson_path_mappable,
            "featureidkey": rec.featureidkey_mappable,
        },
    }
    if rec.boundary_meta_best is not None:
        m["boundary_best"] = {
            "iso3": rec.boundary_meta_best.iso3,
            "adm_level": rec.boundary_meta_best.adm_level,
            "source": rec.boundary_meta_best.source,
            "collection": rec.boundary_meta_best.collection,
            "geojson_relpath": rec.boundary_meta_best.geojson_relpath,
            "featureidkey": rec.boundary_meta_best.featureidkey,
        }
    if rec.boundary_meta_mappable is not None:
        m["boundary_mappable"] = {
            "iso3": rec.boundary_meta_mappable.iso3,
            "adm_level": rec.boundary_meta_mappable.adm_level,
            "source": rec.boundary_meta_mappable.source,
            "collection": rec.boundary_meta_mappable.collection,
            "geojson_relpath": rec.boundary_meta_mappable.geojson_relpath,
            "featureidkey": rec.boundary_meta_mappable.featureidkey,
        }
    if rec.alternative_gids:
        m["alternative_gids"] = sorted(rec.alternative_gids)
    return m


def resolve_doc_gids(
    doc: Dict[str, Any],
    *,
    admin0_map: Dict[str, str],
    country_indices: Dict[str, CountryIndex],
    thr: MatchThresholds,
    allowed_iso3: Optional[Set[str]] = None,
) -> DocResolution:
    """
    Core: parse reconciled_locations and produce:
      - admin1_gids (dict iso3 -> list)
      - admin2_gids (dict iso3 -> list)
      - records (detailed)
      - unmatched (optional, capped)
    """
    locs = list(iter_locations_from_reconciled(doc.get("reconciled_locations")))
    admin1_sets: Dict[str, Set[str]] = defaultdict(set)
    admin2_sets: Dict[str, Set[str]] = defaultdict(set)
    records: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []

    stats = {
        "loc_total": 0,
        "loc_iso3_resolved": 0,
        "loc_skipped_not_in_kb": 0,
        "loc_skipped_not_allowed": 0,
        "admin1_matched": 0,
        "admin2_matched": 0,
        "admin2_fallback_to_admin1": 0,
        "unmatched": 0,
        "iso3_method": Counter(),
        "iso3_fix": Counter(),
        "admin1_method": Counter(),
        "admin2_method": Counter(),
    }

    # caches (per-doc; you can also make these global if desired)
    a1_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}  # (iso3, a1_loose) -> (gid, method)
    a2_cache: Dict[Tuple[str, str, str], Tuple[str, str]] = {}  # (iso3, a1_loose, a2_loose) -> (gid, method)

    for loc in locs:
        stats["loc_total"] += 1

        lvl = str(loc.get("location_level") or "").strip().upper()
        admin0 = str(loc.get("admin0") or "")
        admin1 = str(loc.get("admin1") or "")
        admin2 = str(loc.get("admin2") or "")

        iso3, iso_method = resolve_iso3_general(admin0, admin0_map)
        stats["iso3_method"][iso_method] += 1
        if not iso3:
            stats["unmatched"] += 1
            if STORE_UNMATCHED and len(unmatched) < MAX_UNMATCHED_PER_DOC:
                unmatched.append({
                    "location_level": lvl,
                    "admin0_raw": admin0,
                    "admin1_raw": admin1,
                    "admin2_raw": admin2,
                    "reason": f"no_iso3::{iso_method}",
                })
            continue

        iso3_fixed, fix_reason = fix_iso3_common_errors(iso3, admin1, admin2)
        if fix_reason:
            stats["iso3_fix"][fix_reason] += 1
        iso3 = iso3_fixed

        if allowed_iso3 is not None and iso3 not in allowed_iso3:
            stats["loc_skipped_not_allowed"] += 1
            continue

        idx = country_indices.get(iso3)
        if idx is None:
            stats["loc_skipped_not_in_kb"] += 1
            continue

        stats["loc_iso3_resolved"] += 1
        iso3, admin1, admin2 = apply_admin_aliases(iso3, admin1, admin2)

        # ------------------ ADMIN1 mention ------------------
        if lvl == "ADMIN1":
            a1_loose = norm_name_loose(admin1)
            cache_key = (iso3, a1_loose)
            if cache_key in a1_cache:
                gid1, m1 = a1_cache[cache_key]
            else:
                gid1, m1 = resolve_admin1_gid(idx, admin1, thr=thr)
                a1_cache[cache_key] = (gid1, m1)

            stats["admin1_method"][m1] += 1
            if not gid1:
                stats["unmatched"] += 1
                if STORE_UNMATCHED and len(unmatched) < MAX_UNMATCHED_PER_DOC:
                    unmatched.append({
                        "location_level": "ADMIN1",
                        "iso3": iso3,
                        "admin0_raw": admin0,
                        "admin1_raw": admin1,
                        "admin2_raw": admin2,
                        "reason": m1,
                    })
                continue

            gid1 = normalize_gid(gid1)
            admin1_sets[iso3].add(gid1)
            stats["admin1_matched"] += 1

            rec = idx.a1_records.get(gid1)
            records.append({
                "location_level": "ADMIN1",
                "iso3": iso3,
                "admin1_raw": admin1,
                "admin1_norm": a1_loose,
                "gid": gid1,
                "match_method": m1,
                "kb": _meta_for_admin1(rec) if rec else {},
            })
            continue

        # ------------------ ADMIN2 mention ------------------
        if lvl == "ADMIN2":
            # If admin2 missing/unknown, treat as admin1
            if not admin2 or admin2.strip().lower() == "unknown":
                a1_loose = norm_name_loose(admin1)
                cache_key = (iso3, a1_loose)
                if cache_key in a1_cache:
                    gid1, m1 = a1_cache[cache_key]
                else:
                    gid1, m1 = resolve_admin1_gid(idx, admin1, thr=thr)
                    a1_cache[cache_key] = (gid1, m1)

                stats["admin1_method"][f"admin2_unknown_to_admin1::{m1}"] += 1
                if not gid1:
                    stats["unmatched"] += 1
                    if STORE_UNMATCHED and len(unmatched) < MAX_UNMATCHED_PER_DOC:
                        unmatched.append({
                            "location_level": "ADMIN2",
                            "iso3": iso3,
                            "admin0_raw": admin0,
                            "admin1_raw": admin1,
                            "admin2_raw": admin2,
                            "reason": f"admin2_unknown_to_admin1::{m1}",
                        })
                    continue

                gid1 = normalize_gid(gid1)
                admin1_sets[iso3].add(gid1)
                stats["admin2_fallback_to_admin1"] += 1

                rec = idx.a1_records.get(gid1)
                records.append({
                    "location_level": "ADMIN2",
                    "iso3": iso3,
                    "admin1_raw": admin1,
                    "admin2_raw": admin2,
                    "admin1_norm": a1_loose,
                    "admin2_norm": "",
                    "gid_admin1_rollup": gid1,
                    "gid_admin2": "",
                    "match_method": f"admin2_unknown_to_admin1::{m1}",
                    "kb_admin1": _meta_for_admin1(rec) if rec else {},
                })
                continue

            a1_loose = norm_name_loose(admin1)
            a2_loose = norm_name_loose(admin2)
            cache_key2 = (iso3, a1_loose, a2_loose)
            if cache_key2 in a2_cache:
                gid2, m2 = a2_cache[cache_key2]
            else:
                gid2, m2 = resolve_admin2_gid(idx, admin1, admin2, thr=thr)
                a2_cache[cache_key2] = (gid2, m2)

            stats["admin2_method"][m2] += 1
            if not gid2:
                # fallback to admin1
                cache_key = (iso3, a1_loose)
                if cache_key in a1_cache:
                    gid1, m1 = a1_cache[cache_key]
                else:
                    gid1, m1 = resolve_admin1_gid(idx, admin1, thr=thr)
                    a1_cache[cache_key] = (gid1, m1)

                stats["admin1_method"][f"fallback_from_admin2::{m1}"] += 1
                if not gid1:
                    stats["unmatched"] += 1
                    if STORE_UNMATCHED and len(unmatched) < MAX_UNMATCHED_PER_DOC:
                        unmatched.append({
                            "location_level": "ADMIN2",
                            "iso3": iso3,
                            "admin0_raw": admin0,
                            "admin1_raw": admin1,
                            "admin2_raw": admin2,
                            "reason": f"admin2_no_match::{m2}::fallback_admin1::{m1}",
                        })
                    continue

                gid1 = normalize_gid(gid1)
                admin1_sets[iso3].add(gid1)
                stats["admin2_fallback_to_admin1"] += 1

                rec = idx.a1_records.get(gid1)
                records.append({
                    "location_level": "ADMIN2",
                    "iso3": iso3,
                    "admin1_raw": admin1,
                    "admin2_raw": admin2,
                    "admin1_norm": a1_loose,
                    "admin2_norm": a2_loose,
                    "gid_admin1_rollup": gid1,
                    "gid_admin2": "",
                    "match_method": f"fallback_from_admin2::{m2}::{m1}",
                    "kb_admin1": _meta_for_admin1(rec) if rec else {},
                })
                continue

            # Admin2 matched
            gid2 = normalize_gid(gid2)
            admin2_sets[iso3].add(gid2)
            stats["admin2_matched"] += 1

            rec2 = idx.a2_records.get(gid2)

            # rollup admin1 gid:
            gid1_roll = ""
            if rec2 and rec2.gid_admin1_rollup:
                gid1_roll = rec2.gid_admin1_rollup
            elif is_gadm_gid(gid2):
                gid1_roll = gadm_admin1_from_admin2_gid(gid2)

            # if still missing, try admin1 matching by name
            if not gid1_roll:
                cache_key = (iso3, a1_loose)
                if cache_key in a1_cache:
                    gid1_roll, m1 = a1_cache[cache_key]
                else:
                    gid1_roll, m1 = resolve_admin1_gid(idx, admin1, thr=thr)
                    a1_cache[cache_key] = (gid1_roll, m1)
                stats["admin1_method"][f"rollup_from_admin2::{m1}"] += 1
            else:
                m1 = "rollup_from_admin2::kb_or_gadm"

            gid1_roll = normalize_gid(gid1_roll)
            if gid1_roll:
                admin1_sets[iso3].add(gid1_roll)

            records.append({
                "location_level": "ADMIN2",
                "iso3": iso3,
                "admin1_raw": admin1,
                "admin2_raw": admin2,
                "admin1_norm": a1_loose,
                "admin2_norm": a2_loose,
                "gid_admin2": gid2,
                "gid_admin1_rollup": gid1_roll,
                "match_method": m2,
                "rollup_method": m1,
                "kb": _meta_for_admin2(rec2) if rec2 else {},
            })
            continue

        # unknown lvl => skip but can store as unmatched
        stats["unmatched"] += 1
        if STORE_UNMATCHED and len(unmatched) < MAX_UNMATCHED_PER_DOC:
            unmatched.append({
                "location_level": lvl,
                "admin0_raw": admin0,
                "admin1_raw": admin1,
                "admin2_raw": admin2,
                "reason": "unsupported_location_level",
            })

    # finalize dicts as sorted lists
    admin1_gids_out = {iso: sorted(list(gset)) for iso, gset in admin1_sets.items() if gset}
    admin2_gids_out = {iso: sorted(list(gset)) for iso, gset in admin2_sets.items() if gset}

    # compact counters to plain dicts
    stats["iso3_method"] = dict(stats["iso3_method"])
    stats["iso3_fix"] = dict(stats["iso3_fix"])
    stats["admin1_method"] = dict(stats["admin1_method"])
    stats["admin2_method"] = dict(stats["admin2_method"])

    return DocResolution(
        admin1_gids=admin1_gids_out,
        admin2_gids=admin2_gids_out,
        records=records,
        unmatched=unmatched,
        stats=stats,
    )


# ------------------------ Mongo runner -------------------------

def _kb_version_tag(kb_path: Path) -> str:
    """
    Stable-ish KB version tag to store in docs.
    Uses filename + mtime + size; also includes a short hash for uniqueness.
    """
    try:
        st = kb_path.stat()
        raw = f"{kb_path.name}|mtime={int(st.st_mtime)}|size={int(st.st_size)}"
    except Exception:
        raw = f"{kb_path.name}|mtime=?|size=?"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    return f"{raw}|sha1={h}"


def build_mongo_query() -> Dict[str, Any]:
    q: Dict[str, Any] = {}
    if REQUIRE_INCLUDE_TRUE:
        q["include"] = True
    if REQUIRE_RECONCILED_LOCATIONS:
        q["reconciled_locations"] = {"$exists": True}
    return q


def main() -> None:
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is empty. Set ML4P_MONGO_URI env var or edit CONFIG.")
    if not KB_PATH.exists():
        raise FileNotFoundError(f"KB not found: {KB_PATH}")

    # Connect Mongo
    try:
        from pymongo import MongoClient, UpdateOne  # type: ignore
    except Exception as e:
        raise RuntimeError("pymongo is required. Install it in your environment.") from e

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Load boundary gid index meta (optional)
    boundary_meta = load_boundary_gid_index(BOUNDARY_GID_INDEX_PARQUET)

    # Load KB + prep
    eprint("[kb] loading...")
    kb = load_kb(KB_PATH, sheet=KB_SHEET)
    kb_p = prepare_kb_final_df(kb)
    admin0_map = build_admin0_map(kb_p)
    eprint(f"[kb] rows={len(kb_p):,} countries={kb_p['Admin0_ISO3'].nunique():,} rapidfuzz={HAVE_RAPIDFUZZ}")

    # Pre-build indices for allowed countries (or lazily build)
    allowed_iso3: Optional[Set[str]] = None
    if COUNTRIES:
        allowed_iso3 = {c.strip().upper() for c in COUNTRIES if c.strip()}
        eprint(f"[cfg] restricting to {len(allowed_iso3)} ISO3s")
    else:
        allowed_iso3 = None
        eprint("[cfg] no ISO3 restriction (process any ISO3 found in docs that exists in KB)")

    # Build indices cache
    country_indices: Dict[str, CountryIndex] = {}
    # If restricted, build all upfront (faster per-doc)
    if allowed_iso3 is not None:
        for iso3 in sorted(allowed_iso3):
            if iso3 not in set(kb_p["Admin0_ISO3"].unique().tolist()):
                eprint(f"[kb] WARNING: ISO3={iso3} not found in KB; it will be skipped.")
                continue
            try:
                country_indices[iso3] = build_country_index(kb_p, iso3=iso3, boundary_meta=boundary_meta)
            except Exception as e:
                eprint(f"[kb] ERROR building index for {iso3}: {e}")
        eprint(f"[kb] built indices for {len(country_indices)} countries")

    kb_version = _kb_version_tag(KB_PATH)
    run_meta = {
        "kb_version": kb_version,
        "kb_path": str(KB_PATH),
        "kb_sheet": KB_SHEET,
        "boundary_gid_index_parquet": str(BOUNDARY_GID_INDEX_PARQUET) if BOUNDARY_GID_INDEX_PARQUET else "",
        "thresholds": THR.__dict__,
        "rapidfuzz": bool(HAVE_RAPIDFUZZ),
        "script": Path(__file__).name if "__file__" in globals() else "record_kb_gids_to_mongo_v3.py",
        "updated_at": _dt.datetime.now().isoformat(),
        "dry_run": bool(DRY_RUN),
    }

    q_base = build_mongo_query()

    total_updates = 0
    total_scanned = 0
    total_skipped_existing = 0
    total_errors = 0

    # Iterate collections by month
    for y, m in iter_months(START_YM, END_YM):
        colname = COLLECTION_TEMPLATE.format(year=y, month=m)
        if colname not in db.list_collection_names():
            eprint(f"[mongo] missing collection: {colname} (skipping)")
            continue

        eprint(f"\n[mongo] collection={colname}")
        q = dict(q_base)

        # If not overwriting, skip docs that already have both fields
        if not OVERWRITE_EXISTING:
            q["$or"] = [
                {OUT_FIELD_ADMIN1_GIDS: {"$exists": False}},
                {OUT_FIELD_ADMIN2_GIDS: {"$exists": False}},
                {OUT_FIELD_RECORDS: {"$exists": False}},
            ]

        projection = {
            "_id": 1,
            "include": 1,
            "reconciled_locations": 1,
            OUT_FIELD_ADMIN1_GIDS: 1,
            OUT_FIELD_ADMIN2_GIDS: 1,
            OUT_FIELD_RECORDS: 1,
            OUT_FIELD_META: 1,
        }

        cur = db[colname].find(q, projection=projection, batch_size=BATCH_SIZE)
        ops = []
        scanned_in_col = 0
        updated_in_col = 0

        for doc in cur:
            scanned_in_col += 1
            total_scanned += 1
            if MAX_DOCS_PER_COLLECTION is not None and scanned_in_col > MAX_DOCS_PER_COLLECTION:
                break

            # If version matches and we want to skip, skip fast
            if SKIP_IF_KB_VERSION_MATCH:
                meta0 = doc.get(OUT_FIELD_META) or {}
                if isinstance(meta0, dict) and meta0.get("kb_version") == kb_version:
                    total_skipped_existing += 1
                    continue

            # Lazily build index if not restricted
            # (only when we encounter a new iso3)
            # NOTE: resolve_doc_gids will check country_indices.get(iso3); so we fill as needed
            if allowed_iso3 is None:
                # We cannot know which iso3 are needed without parsing; resolve_doc_gids will skip unknowns.
                pass

            # If not restricted, we build indices on demand: detect iso3s in reconciled_locations quickly
            if allowed_iso3 is None:
                # light pre-scan for ISO3s in reconciled_locations strings/dicts
                # We'll just build indices as we discover new iso3 during resolution by intercepting inside resolve_doc_gids:
                # simplest approach: build when needed after iso3 resolved; done via a wrapper below.
                pass

            # Wrapper: on-demand index build
            def _ensure_index(iso3: str) -> None:
                iso3u = iso3.strip().upper()
                if iso3u in country_indices:
                    return
                if iso3u not in set(kb_p["Admin0_ISO3"].unique().tolist()):
                    return
                try:
                    country_indices[iso3u] = build_country_index(kb_p, iso3=iso3u, boundary_meta=boundary_meta)
                except Exception as e:
                    eprint(f"[kb] ERROR building index for {iso3u}: {e}")

            # We do resolution and build missing indices opportunistically:
            # - try resolve iso3 for each loc and build idx before matching.
            # To avoid duplicating logic, we do a quick pass to collect iso3s from locs.
            locs = list(iter_locations_from_reconciled(doc.get("reconciled_locations")))
            if locs:
                iso3s_needed: Set[str] = set()
                for loc in locs:
                    iso3_raw, _ = resolve_iso3_general(str(loc.get("admin0") or ""), admin0_map)
                    if iso3_raw:
                        iso3_fixed, _ = fix_iso3_common_errors(iso3_raw, str(loc.get("admin1") or ""), str(loc.get("admin2") or ""))
                        if allowed_iso3 is None or (allowed_iso3 is not None and iso3_fixed in allowed_iso3):
                            iso3s_needed.add(iso3_fixed)
                for iso3_needed in iso3s_needed:
                    _ensure_index(iso3_needed)

            # Resolve gids
            try:
                res = resolve_doc_gids(
                    doc,
                    admin0_map=admin0_map,
                    country_indices=country_indices,
                    thr=THR,
                    allowed_iso3=allowed_iso3,
                )
            except Exception as e:
                total_errors += 1
                eprint(f"[ERROR] doc={doc.get('_id')} resolve failed: {e}")
                continue

            # Build update payload
            update_doc = {
                OUT_FIELD_ADMIN1_GIDS: res.admin1_gids,
                OUT_FIELD_ADMIN2_GIDS: res.admin2_gids,
                OUT_FIELD_RECORDS: res.records,
                OUT_FIELD_META: {**run_meta, "stats": res.stats},
            }
            if STORE_UNMATCHED:
                update_doc[OUT_FIELD_UNMATCHED] = res.unmatched

            # If not overwriting and fields already exist, skip (extra safeguard)
            if not OVERWRITE_EXISTING:
                if doc.get(OUT_FIELD_ADMIN1_GIDS) is not None and doc.get(OUT_FIELD_ADMIN2_GIDS) is not None:
                    total_skipped_existing += 1
                    continue

            ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": update_doc}, upsert=False))

            if len(ops) >= BULK_WRITE_SIZE:
                if not DRY_RUN:
                    try:
                        r = db[colname].bulk_write(ops, ordered=False)
                        updated_in_col += int(r.modified_count)
                        total_updates += int(r.modified_count)
                    except Exception as e:
                        total_errors += 1
                        eprint(f"[ERROR] bulk_write failed in {colname}: {e}")
                else:
                    # dry-run: pretend all would be modified
                    updated_in_col += len(ops)
                ops = []

        # flush remainder
        if ops:
            if not DRY_RUN:
                try:
                    r = db[colname].bulk_write(ops, ordered=False)
                    updated_in_col += int(r.modified_count)
                    total_updates += int(r.modified_count)
                except Exception as e:
                    total_errors += 1
                    eprint(f"[ERROR] bulk_write failed in {colname}: {e}")
            else:
                updated_in_col += len(ops)

        eprint(f"[mongo] {colname}: scanned={scanned_in_col:,} updated={updated_in_col:,}")

    eprint("\n" + "=" * 70)
    eprint("[DONE]")
    eprint(f"scanned_total={total_scanned:,}")
    eprint(f"updated_total={total_updates:,} (dry_run={DRY_RUN})")
    eprint(f"skipped_existing_total={total_skipped_existing:,} (version_match={SKIP_IF_KB_VERSION_MATCH})")
    eprint(f"errors_total={total_errors:,}")
    eprint("=" * 70)


if __name__ == "__main__":
    main()
