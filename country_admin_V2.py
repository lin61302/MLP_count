#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
country_admin_counts_kb_v2_batch.py

Batch (multi-country) subnational ADMIN1/ADMIN2 environmental event counts for MLEED,
with KB-driven, geojson-consistent GID matching (supports mixed boundary sources).

This version is designed for running on a workstation/remote box with ONE command:
    python country_admin_counts_kb_v2_batch.py

You configure everything in the CONFIG block below (countries, mongo uri, date range, paths).

Outputs (Dropbox structure)
---------------------------
Counts go here:
  /home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational/Admin1/{ISO3}/{RUN_DATE}/counts_{ISO3}_admin1_{YYYY-MM}.csv
  /home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational/Admin2/{ISO3}/{RUN_DATE}/counts_{ISO3}_admin2_{YYYY-MM}.csv

All other artifacts (denominators, diagnostics, unmatched, article records, run config) go here:
  /home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational/Other/{ISO3}/{RUN_DATE}/...

Key matching behavior (UPDATED)
-------------------------------
- Uses KB's per-level geojson coding method for *the chosen primary gid*:
    * Admin1 uses gid_admin1_eff + (admin1_source_eff, admin1_geojson_path_eff, admin1_featureidkey_eff)
    * Admin2 uses gid_admin2_eff + (admin2_source_eff, admin2_geojson_path_eff, admin2_featureidkey_eff)
- Manual GADM overrides:
    * If PREFER_MANUAL_GADM=True, manual_gid1/manual_gid2 are treated as *candidates* for the PRIMARY gid
      even if the KB row's original boundary source is geoboundaries/geohumanitarian/etc.
    * We ONLY use a manual gid as primary if it is validated as a real GADM41 gid
      (via lookup parquet if provided; otherwise via gadm41 geojson inside boundary_cache.zip/root).
    * If manual gid is not validated, we fall back to the original KB gid for that level.
- Alternative GIDs are preserved:
    * Even when the PRIMARY gid becomes GADM, we keep the original KB gid(s) (geoboundaries/macro/etc) as
      "alternative_gids" in the output counts/denoms metadata so downstream mapping has multiple options.
- Admin2 matching is preferred when it is confident AND unambiguous; otherwise we fall back to Admin1.
- Loose matching strips common admin designators (district/municipio/rayon/bashkia/obshtina/vald/etc.),
  but avoids the classic "district of ..." pitfall.

Dependencies
------------
- pandas
- pymongo
- rapidfuzz (recommended; fallback to difflib if missing)
- (optional) pyarrow/fastparquet for parquet reading if you set the GADM lookup parquet paths

Notes
-----
- Article collections are assumed to be named: articles-YYYY-M (no leading-zero month).
"""

from __future__ import annotations

import ast
import datetime as _dt
import gzip
import json
import math
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pandas as pd


# ============================================================
# =========================== CONFIG =========================
# ============================================================

# --- Mongo ---
# Recommended: set via env var so you don't commit credentials:
#   export ML4P_MONGO_URI="mongodb://user:pass@host/?authSource=ml4p&tls=true"
MONGO_URI: str = os.environ.get(
    "ML4P_MONGO_URI",
    "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true",
).strip()
MONGO_DB: str = os.environ.get("ML4P_MONGO_DB", "ml4p").strip()

# --- Countries + time range ---
COUNTRIES: List[str] = [
    # "ALB",
    # "BEN",
    # "COL",
    # "ECU",
    # "ETH",
    # "GEO",
    # "KEN",
    # "PRY",
    # "MLI",
    # "MAR",
    # "NGA",
    # "SRB",
    # "SEN",
    # "TZA",
    # "UGA",
    # "UKR",
    # "ZWE",
    # "MRT",
    # "ZMB",
    # "XKX",
    # "NER",
    # "JAM",
    # "HND",
    # "PHL",
    # "GHA",
    # "RWA",
    # "GTM",
    "BLR",
    "KHM",
    "COD",
    "TUR",
    "BGD",
    "SLV",
    "ZAF",
    "TUN",
    "IDN",
    "NIC",
    "AGO",
    "ARM",
    "LKA",
    "MYS",
    "CMR",
    "HUN",
    "MWI",
    "UZB",
    "IND",
    "MOZ",
    "AZE",
    "KGZ",
    "MDA",
    "KAZ",
    "PER",
    "DZA",
    "MKD",
    "SSD",
    "LBR",
    "PAK",
    "NPL",
    "NAM",
    "BFA",
    "DOM",
    "TLS",
    "SLB",
    "CRI",
    "PAN",
    "MEX",
]
START_YM: str = "2012-01"
END_YM: str = "2025-12"

# --- KB ---
KB_PATH: Path = Path(
    "/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/KB_fresh_start_V1.xlsx"
)  # <-- EDIT
KB_SHEET: Optional[str | int] = 0  # 0=first sheet; or name like "kb"

# --- Boundary cache (optional) ---
# NOTE: Even if VALIDATE_GEOJSON=False, BOUNDARY_CACHE_ZIP/ROOT can still be used
#       to validate manual GADM ids (fallback if parquet lookups aren't available).
BOUNDARY_CACHE_ROOT: Optional[Path] = None  # e.g., Path("/home/ml4p/projects/geodata") containing boundary_cache/...
BOUNDARY_CACHE_ZIP: Optional[Path] = None  # e.g., Path("/home/ml4p/boundary_cache.zip")
VALIDATE_GEOJSON: bool = False  # set True if you have cache available and want join validation

# --- Manual GADM overrides (NEW) ---
# If True, manual_gid1/manual_gid2 (when valid GADM41 ids) become the PRIMARY gid used for counting.
PREFER_MANUAL_GADM: bool = True

# If True, a manual gid MUST be validated against a known-gids source (parquet or cache geojson) to be used.
# If False, we will accept "syntactically valid" gadm-style ids even without verification (NOT recommended).
REQUIRE_GADM_VALIDATION: bool = True

# Optional: parquet lookups (fastest). You can set via env vars too:
#   export ML4P_GADM41_ADMIN1_GID_LOOKUP_PARQUET="/path/to/gadm41_admin1_gid_lookup.parquet"
#   export ML4P_GADM41_ADMIN2_GID_LOOKUP_PARQUET="/path/to/gadm41_admin2_gid_lookup.parquet"
_GADM_A1_ENV = os.environ.get("ML4P_GADM41_ADMIN1_GID_LOOKUP_PARQUET", "").strip()
_GADM_A2_ENV = os.environ.get("ML4P_GADM41_ADMIN2_GID_LOOKUP_PARQUET", "").strip()
GADM41_ADMIN1_GID_LOOKUP_PARQUET: Optional[Path] = Path(_GADM_A1_ENV) if _GADM_A1_ENV else None
GADM41_ADMIN2_GID_LOOKUP_PARQUET: Optional[Path] = Path(_GADM_A2_ENV) if _GADM_A2_ENV else None

# --- Output base (Dropbox) ---
OUT_BASE: Path = Path("/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational")

# --- Run date folder name ---
# Default: today's date in YYYY_MM_DD (consistent with many run folders)
RUN_DATE: str = os.environ.get("ML4P_RUN_DATE", "").strip() or _dt.date.today().strftime("%Y_%m_%d")

# --- Performance ---
BATCH_SIZE: int = 700  # Mongo batch size
NORM_MODE: str = "country"  # "country" (mleed_counts-style) or "gid" (per-gid locatable denom)

# --- Fuzzy thresholds (tuned conservative defaults) ---
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

# --- Event types ---
ENV_EVENT_TYPES: List[str] = [
    "sudden-onset environmental disaster",
    "slow-onset environmental disaster",
    "human-induced disaster",
    "displacement",
    "environmental activism",
    "environmental arrest",
    "environmental protests",
    "environmental corruption",
    "environmental security",
    "environmental crime",
    "environmental government initiatives",
    "environmental corporate initiatives",
    "environmental cooperation",
    "environmental legal action",
    "environmental legal change",
    "lethal environmental violence",
    "nonlethal environmental violence",
    "environmental opinion",
]

# ============================================================
# ======================== End CONFIG ========================
# ============================================================


COLLECTION_TEMPLATE = "articles-{year}-{month}"  # no leading zero month
DEFAULT_FIELDS = [
    "_id",
    "date_publish",
    "source_domain",
    "url",
    "language",
    "title",
    "include",
    "env_classifier",
    "reconciled_locations",
]


# ------------------------ utilities -------------------------


def eprint(*a: Any) -> None:
    print(*a, file=sys.stderr)


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


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


def trunc(s: Any, n: int = 240) -> str:
    if s is None:
        return ""
    s2 = str(s)
    return s2 if len(s2) <= n else s2[: n - 1] + "…"


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


# ------------------------ GID normalization -------------------------

GB_LEGACY_PREFIX_RE = re.compile(r"^GB\.(gbOpen|gbHumanitarian|gbAuthoritative)\.", re.IGNORECASE)
MALFORMED_GADM_RE = re.compile(r"^([A-Z]{3})(\d+\..*_\d+)$")
MALFORMED_GADM_DEPTH1_RE = re.compile(r"^([A-Z]{3})(\d+_\d+)$")
ISO3_ONLY_RE = re.compile(r"^[A-Z]{3}$")
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


# ------------------------ Manual GADM validation (NEW) -------------------------


def _infer_gid_column(cols: Sequence[str], *, level: int) -> str:
    """
    Infer a GID column name in a parquet lookup table.
    We try common patterns first; then regex fallbacks.
    """
    cset = {c: c for c in cols}
    # exact candidates
    candidates = [
        f"KB_GID_{level}",
        f"GID_{level}",
        f"gid_{level}",
        f"kb_gid_{level}",
        "gid",
        "GID",
        "KB_GID",
        "kb_gid",
    ]
    for c in candidates:
        if c in cset:
            return c

    # case-insensitive match
    lower_map = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]

    # regex: something like kb_gid_1 / gid_2 / GID2, etc.
    pat = re.compile(rf"^(kb_)?gid[_-]?{level}$", re.IGNORECASE)
    for c in cols:
        if pat.match(c.strip()):
            return c

    # fallback: any column that looks like it contains "gid" and the level digit
    pat2 = re.compile(rf"gid.*{level}", re.IGNORECASE)
    for c in cols:
        if pat2.search(c):
            return c

    return ""


def load_gadm_gid_set_from_parquet(path: Optional[Path], *, level: int) -> Optional[Set[str]]:
    """
    Load a set of valid GADM gids from a parquet lookup file.
    Returns None if path is missing or unreadable.
    """
    if path is None:
        return None
    try:
        p = Path(path)
    except Exception:
        return None
    if not p.exists():
        eprint(f"[gadm_lookup] parquet not found: {p}")
        return None

    try:
        df = pd.read_parquet(p)  # requires pyarrow/fastparquet
    except Exception as e:
        eprint(f"[gadm_lookup] failed to read parquet {p}: {e}")
        return None

    gid_col = _infer_gid_column(df.columns.tolist(), level=level)
    if not gid_col:
        eprint(f"[gadm_lookup] could not infer gid column for level={level} in {p}; columns={list(df.columns)[:25]}")
        return None

    # Normalize + keep syntactically-valid gadm ids only
    out: Set[str] = set()
    try:
        ser = df[gid_col]
    except Exception:
        return None

    for v in ser.tolist():
        g = normalize_gid(v)
        if not g:
            continue
        if not is_gadm_gid(g):
            continue
        if level == 1 and gadm_dot_depth(g) != 1:
            continue
        if level >= 2 and gadm_dot_depth(g) < 2:
            continue
        out.add(g)

    if not out:
        eprint(f"[gadm_lookup] loaded 0 gids from {p} (col={gid_col})")
        return None

    eprint(f"[gadm_lookup] loaded {len(out):,} level-{level} gids from {p} (col={gid_col})")
    return out


@dataclass
class GadmValidator:
    """
    Validates whether a manual_gid1/manual_gid2 is a real GADM41 GID.

    Priority:
      1) Parquet lookups (global sets)
      2) boundary_cache gadm41 geojson inside zip/root (per-ISO3 cached)
      3) if no validation sources:
           - if require_validation=True -> return False
           - else -> accept syntactically-valid gadm gid
    """

    admin1_global: Optional[Set[str]] = None
    admin2_global: Optional[Set[str]] = None
    boundary_cache_root: Optional[Path] = None
    boundary_cache_zip: Optional[Path] = None
    require_validation: bool = True

    _geojson_cache: Dict[Tuple[str, int], Set[str]] = field(default_factory=dict)

    def _syntax_ok(self, iso3: str, gid: str, level: int) -> bool:
        iso3 = str(iso3 or "").strip().upper()
        g = normalize_gid(gid)
        if not g or not iso3:
            return False
        if not is_gadm_gid(g):
            return False
        if not g.startswith(f"{iso3}."):
            return False
        d = gadm_dot_depth(g)
        if level == 1:
            return d == 1
        return d >= 2

    def _load_geojson_gidset(self, iso3: str, level: int) -> Set[str]:
        iso3 = str(iso3 or "").strip().upper()
        key = (iso3, level)
        if key in self._geojson_cache:
            return self._geojson_cache[key]

        # Try KB_GID first; fallback to GID
        path = f"boundary_cache/gadm41/{iso3}/ADM{level}_kb.geojson"
        s = load_geojson_gid_set(
            path,
            f"properties.KB_GID_{level}",
            boundary_cache_root=self.boundary_cache_root,
            boundary_cache_zip=self.boundary_cache_zip,
        )
        if s is None:
            s = load_geojson_gid_set(
                path,
                f"properties.GID_{level}",
                boundary_cache_root=self.boundary_cache_root,
                boundary_cache_zip=self.boundary_cache_zip,
            )

        gids = set(s) if s is not None else set()
        self._geojson_cache[key] = gids
        return gids

    def is_valid(self, iso3: str, gid: str, level: int) -> bool:
        iso3 = str(iso3 or "").strip().upper()
        g = normalize_gid(gid)
        if not self._syntax_ok(iso3, g, level):
            return False

        # 1) parquet/global
        if level == 1 and self.admin1_global is not None:
            return g in self.admin1_global
        if level >= 2 and self.admin2_global is not None:
            return g in self.admin2_global

        # 2) boundary cache geojson
        if self.boundary_cache_zip is not None or self.boundary_cache_root is not None:
            gids = self._load_geojson_gidset(iso3, level)
            return g in gids

        # 3) no validation sources
        return (not self.require_validation)

    def is_valid_admin1(self, iso3: str, gid: str) -> bool:
        return self.is_valid(iso3, gid, level=1)

    def is_valid_admin2(self, iso3: str, gid: str) -> bool:
        return self.is_valid(iso3, gid, level=2)


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


# Token-stripping list: user-provided + common additions
ADMIN_SUFFIX_TOKENS: Set[str] = {
    # English
    "province",
    "state",
    "district",
    "county",
    "region",
    "division",
    "municipality",
    "parish",
    "prefecture",
    "governorate",
    "department",
    # Spanish/Portuguese
    "provincia",
    "estado",
    "distrito",
    "municipio",
    "municipal",
    "municipalidad",
    "departamento",
    "departamento",
    "departamento",  # harmless dup
    "departamento",
    "departamento",
    # French/Italian/German/etc (accents stripped by norm)
    "departement",
    "departemental",
    "regione",
    # Other common admin units / transliterations
    "oblast",
    "krai",
    "okrug",
    "rayon",
    "raion",
    "rajon",
    "bashkia",
    "obshtina",
    "vald",
    "wilaya",
    "canton",
    "commune",
    "comuna",
    "comune",
    "arrondissement",
    "prefectura",
    "governacion",
    "district municipality",
    # Abbrev
    "dept",
    "mun",
    "dist",
    "gov",
}

PREFIX_DESIGNATORS: Set[str] = {
    "municipio",
    "municipality",
    "department",
    "departamento",
    "departement",
    "province",
    "provincia",
    "region",
    "regione",
    "prefecture",
    "prefectura",
    "governorate",
    "wilaya",
    "commune",
    "comuna",
    "comune",
    "canton",
    "rayon",
    "raion",
    "rajon",
    "bashkia",
    "obshtina",
    "vald",
    "state",
    "estado",
    "county",
    "division",
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


# ------------------------ ISO3 fixes -------------------------

_US_STATES = {
    "alabama",
    "alaska",
    "arizona",
    "arkansas",
    "california",
    "colorado",
    "connecticut",
    "delaware",
    "florida",
    "georgia",
    "hawaii",
    "idaho",
    "illinois",
    "indiana",
    "iowa",
    "kansas",
    "kentucky",
    "louisiana",
    "maine",
    "maryland",
    "massachusetts",
    "michigan",
    "minnesota",
    "mississippi",
    "missouri",
    "montana",
    "nebraska",
    "nevada",
    "new hampshire",
    "new jersey",
    "new mexico",
    "new york",
    "north carolina",
    "north dakota",
    "ohio",
    "oklahoma",
    "oregon",
    "pennsylvania",
    "rhode island",
    "south carolina",
    "south dakota",
    "tennessee",
    "texas",
    "utah",
    "vermont",
    "virginia",
    "washington",
    "west virginia",
    "wisconsin",
    "wyoming",
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


# ------------------------ KB loading/prep -------------------------


def load_kb(kb_path: Path, sheet: Optional[str | int] = None) -> pd.DataFrame:
    if not kb_path.exists():
        raise FileNotFoundError(f"KB not found: {kb_path}")
    ext = kb_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(kb_path, sheet_name=sheet if sheet is not None else 0, dtype=str)
    else:
        df = pd.read_csv(kb_path, dtype=str, low_memory=False)
    df = df.fillna("")
    missing = [c for c in ["Admin0_ISO3", "Admin1", "Admin2", "gid_admin1", "gid_admin2"] if c not in df.columns]
    if missing:
        raise ValueError(f"KB missing required columns: {missing}")
    return df


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


def _split_gid_list(s: Any) -> List[str]:
    if s is None:
        return []
    if isinstance(s, list):
        return [normalize_gid(x) for x in s if normalize_gid(x)]
    if isinstance(s, str):
        t = s.strip()
        if not t:
            return []
        # accept pipe-separated or json list
        if t.startswith("[") and t.endswith("]"):
            obj = parse_maybe_json_or_literal(t)
            if isinstance(obj, list):
                return [normalize_gid(x) for x in obj if normalize_gid(x)]
        if "|" in t:
            return [normalize_gid(x) for x in t.split("|") if normalize_gid(x)]
        return [normalize_gid(t)]
    return []


def prepare_kb_df(
    kb: pd.DataFrame,
    *,
    gadm_validator: Optional[GadmValidator] = None,
    prefer_manual_gadm: bool = True,
) -> pd.DataFrame:
    """
    Prepares KB and computes:
      - gid_admin1_eff / gid_admin2_eff (PRIMARY gids)
      - admin1_*_eff / admin2_*_eff (metadata corresponding to the PRIMARY gid system)
      - admin1_alternative_gids / admin2_alternative_gids (pipe-separated)
      - admin1_gid_method / admin2_gid_method (why primary gid was chosen)
    """
    kb = kb.copy().fillna("")
    kb["Admin0_ISO3"] = kb["Admin0_ISO3"].astype(str).str.strip().str.upper()

    # Normalize gid-like columns we might use
    for col in [
        "gid_admin1",
        "gid_admin2",
        "gid_admin1_from_admin2",
        "gid_best",
        "manual_gid1",
        "manual_gid2",
        "gid_admin1_original",
        "gid_admin2_original",
        "gid_admin1_prev_consistency",
        "gid_admin2_prev_consistency",
    ]:
        if col in kb.columns:
            kb[col] = kb[col].apply(normalize_gid)
        else:
            kb[col] = ""

    # levels used
    if "admin2_level_used" in kb.columns:
        kb["admin2_level_used_int"] = kb["admin2_level_used"].apply(_parse_level_int)
    else:
        kb["admin2_level_used_int"] = 0

    if "gid_best_level" in kb.columns:
        kb["gid_best_level_int"] = kb["gid_best_level"].apply(_parse_level_int)
    else:
        kb["gid_best_level_int"] = 0

    # display names
    kb["admin1_disp"] = kb.get("admin1_matched_name", "").astype(str)
    kb.loc[kb["admin1_disp"].astype(str).str.strip() == "", "admin1_disp"] = kb.get("Admin1", "").astype(str)

    kb["admin2_disp"] = kb.get("admin2_matched_name", "").astype(str)
    kb.loc[kb["admin2_disp"].astype(str).str.strip() == "", "admin2_disp"] = kb.get("Admin2", "").astype(str)

    # quality scores
    kb["admin1_quality"] = kb.get("admin1_canonical_score", "").apply(as_float)
    kb.loc[kb["admin1_quality"].isna(), "admin1_quality"] = kb.get("admin1_score", "").apply(as_float)

    kb["admin2_quality"] = kb.get("admin2_match_score", "").apply(as_float)
    kb.loc[kb["admin2_quality"].isna(), "admin2_quality"] = kb.get("admin2_score", "").apply(as_float)

    kb.loc[kb["admin2_level_used_int"] < 2, "admin2_quality"] = 0.0

    # --- primary gid selection + metadata override (NEW) ---
    # GADM templates (path/key/url) per ISO3, when we switch primary to GADM
    # We'll use KB_GID_1/2 keys consistently.
    kb["admin1_source_eff"] = kb.get("admin1_source", "").astype(str)
    kb["admin1_geojson_path_eff"] = kb.get("admin1_geojson_path", "").astype(str)
    kb["admin1_featureidkey_eff"] = kb.get("admin1_featureidkey", "").astype(str)
    kb["admin1_geojson_url_eff"] = kb.get("admin1_geojson_url", "").astype(str)

    kb["admin2_source_eff"] = kb.get("admin2_source", "").astype(str)
    kb["admin2_geojson_path_eff"] = kb.get("admin2_geojson_path", "").astype(str)
    kb["admin2_featureidkey_eff"] = kb.get("admin2_featureidkey", "").astype(str)
    kb["admin2_geojson_url_eff"] = kb.get("admin2_geojson_url", "").astype(str)

    kb["gid_admin1_eff"] = kb["gid_admin1"].astype(str)
    kb["gid_admin2_eff"] = kb["gid_admin2"].astype(str)

    kb["admin1_gid_method"] = ""
    kb["admin2_gid_method"] = ""

    kb["admin1_alternative_gids"] = ""
    kb["admin2_alternative_gids"] = ""

    # precompute gadm geojson_url by iso3 (best effort)
    gadm_a1_url: Dict[str, str] = {}
    gadm_a2_url: Dict[str, str] = {}
    try:
        tmp1 = kb[kb.get("admin1_source", "").astype(str).str.lower() == "gadm41"]
        if not tmp1.empty and "admin1_geojson_url" in tmp1.columns:
            for iso, g in tmp1.groupby("Admin0_ISO3"):
                u = (g["admin1_geojson_url"].astype(str).str.strip())
                u = u[u != ""]
                if not u.empty:
                    gadm_a1_url[str(iso)] = u.iloc[0]
        tmp2 = kb[kb.get("admin2_source", "").astype(str).str.lower() == "gadm41"]
        if not tmp2.empty and "admin2_geojson_url" in tmp2.columns:
            for iso, g in tmp2.groupby("Admin0_ISO3"):
                u = (g["admin2_geojson_url"].astype(str).str.strip())
                u = u[u != ""]
                if not u.empty:
                    gadm_a2_url[str(iso)] = u.iloc[0]
    except Exception:
        pass

    # row-wise selection
    for i, row in kb.iterrows():
        iso3 = str(row.get("Admin0_ISO3") or "").strip().upper()
        if not iso3:
            continue

        # ---------- ADMIN1 ----------
        orig1 = normalize_gid(row.get("gid_admin1") or "")
        man1 = normalize_gid(row.get("manual_gid1") or "")

        use_man1 = False
        if prefer_manual_gadm and man1:
            if gadm_validator is not None:
                use_man1 = gadm_validator.is_valid_admin1(iso3, man1)
            else:
                # no validator provided => accept only if not requiring validation
                use_man1 = (not REQUIRE_GADM_VALIDATION) and is_gadm_gid(man1) and gadm_dot_depth(man1) == 1 and man1.startswith(
                    f"{iso3}."
                )

        gid1_eff = man1 if use_man1 else orig1
        kb.at[i, "gid_admin1_eff"] = gid1_eff

        if gid1_eff:
            kb.at[i, "admin1_gid_method"] = "manual_gadm_validated" if use_man1 else "kb_assigned"
        else:
            kb.at[i, "admin1_gid_method"] = "missing"

        # If primary is manual gadm, switch metadata fields to gadm41 templates
        if use_man1 and gid1_eff:
            kb.at[i, "admin1_source_eff"] = "gadm41"
            kb.at[i, "admin1_geojson_path_eff"] = f"boundary_cache/gadm41/{iso3}/ADM1_kb.geojson"
            kb.at[i, "admin1_featureidkey_eff"] = "properties.KB_GID_1"
            kb.at[i, "admin1_geojson_url_eff"] = gadm_a1_url.get(iso3, "")

        # Build ADMIN1 alternatives (always include other known ids; exclude primary)
        alt1: List[str] = []
        for cand in [
            orig1,
            normalize_gid(row.get("gid_admin1_original") or ""),
            normalize_gid(row.get("gid_admin1_prev_consistency") or ""),
            normalize_gid(row.get("gid_admin1_from_admin2") or ""),
        ]:
            if cand and cand != gid1_eff:
                alt1.append(cand)

        # include gid_best if it is an admin1-level gid in KB
        best = normalize_gid(row.get("gid_best") or "")
        best_lvl = int(row.get("gid_best_level_int") or 0)
        if best and best_lvl == 1 and best != gid1_eff:
            alt1.append(best)

        # If admin2 manual/eff is gadm, include derived parent gadm as alternative admin1
        # (helps when admin1 primary is non-gadm but admin2 is gadm)
        gid2_probe = normalize_gid(row.get("manual_gid2") or "") or normalize_gid(row.get("gid_admin2") or "")
        if is_gadm_gid(gid2_probe) and gadm_dot_depth(gid2_probe) >= 2:
            derived_a1 = gadm_admin1_from_admin2_gid(gid2_probe)
            if derived_a1 and derived_a1 != gid1_eff:
                alt1.append(derived_a1)

        # de-dupe keep order
        seen1: Set[str] = set()
        alt1_out: List[str] = []
        for g in alt1:
            g2 = normalize_gid(g)
            if g2 and g2 not in seen1 and g2 != gid1_eff:
                seen1.add(g2)
                alt1_out.append(g2)
        kb.at[i, "admin1_alternative_gids"] = "|".join(alt1_out)

        # ---------- ADMIN2 ----------
        lvl_used = int(row.get("admin2_level_used_int") or 0)
        orig2 = normalize_gid(row.get("gid_admin2") or "")
        man2 = normalize_gid(row.get("manual_gid2") or "")

        use_man2 = False
        if prefer_manual_gadm and man2:
            # Optional consistency check: if manual_gid1 is valid, ensure manual_gid2 rolls up to it
            if gadm_validator is not None and gadm_validator.is_valid_admin2(iso3, man2):
                if use_man1 and gid1_eff and is_gadm_gid(gid1_eff):
                    parent = gadm_admin1_from_admin2_gid(man2)
                    if parent and parent != gid1_eff:
                        use_man2 = False
                    else:
                        use_man2 = True
                else:
                    use_man2 = True
            else:
                if gadm_validator is None:
                    use_man2 = (not REQUIRE_GADM_VALIDATION) and is_gadm_gid(man2) and gadm_dot_depth(man2) >= 2 and man2.startswith(
                        f"{iso3}."
                    )
                else:
                    use_man2 = False

        gid2_eff = man2 if use_man2 else orig2
        kb.at[i, "gid_admin2_eff"] = gid2_eff

        if gid2_eff:
            kb.at[i, "admin2_gid_method"] = "manual_gadm_validated" if use_man2 else "kb_assigned"
        else:
            kb.at[i, "admin2_gid_method"] = "missing"

        # Only override metadata if this row is actually an admin2 row (lvl_used>=2)
        if use_man2 and gid2_eff and lvl_used >= 2:
            kb.at[i, "admin2_source_eff"] = "gadm41"
            kb.at[i, "admin2_geojson_path_eff"] = f"boundary_cache/gadm41/{iso3}/ADM2_kb.geojson"
            kb.at[i, "admin2_featureidkey_eff"] = "properties.KB_GID_2"
            kb.at[i, "admin2_geojson_url_eff"] = gadm_a2_url.get(iso3, "")

        # Build ADMIN2 alternatives (exclude primary)
        alt2: List[str] = []
        for cand in [
            orig2,
            normalize_gid(row.get("gid_admin2_original") or ""),
            normalize_gid(row.get("gid_admin2_prev_consistency") or ""),
        ]:
            if cand and cand != gid2_eff:
                alt2.append(cand)

        # include gid_best if it is an admin2-level gid in KB
        if best and best_lvl >= 2 and best != gid2_eff:
            alt2.append(best)

        # de-dupe keep order
        seen2: Set[str] = set()
        alt2_out: List[str] = []
        for g in alt2:
            g2 = normalize_gid(g)
            if g2 and g2 not in seen2 and g2 != gid2_eff:
                seen2.add(g2)
                alt2_out.append(g2)
        kb.at[i, "admin2_alternative_gids"] = "|".join(alt2_out)

    return kb


def build_admin0_map(kb: pd.DataFrame) -> Dict[str, str]:
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


# ------------------------ geojson validation (optional) -------------------------


def _parse_featureidkey(featureidkey: str) -> str:
    s = str(featureidkey or "").strip()
    if not s:
        return ""
    if s.startswith("properties."):
        return s.split(".", 1)[1]
    return s


def _resolve_geojson_path(path_str: str, boundary_cache_root: Optional[Path]) -> Optional[Path]:
    p = Path(path_str) if path_str else None
    if not p:
        return None
    if p.is_absolute() and p.exists():
        return p
    if boundary_cache_root is not None:
        cand = boundary_cache_root / p
        if cand.exists():
            return cand

        s = str(p).replace("\\", "/")
        m = re.match(
            r"^boundary_cache/geoboundaries/(gbOpen|gbHumanitarian|gbAuthoritative)/([A-Z]{3})/(ADM\d+_kb\.geojson)$",
            s,
        )
        if m:
            method, iso3, fname = m.group(1), m.group(2), m.group(3)
            flat = boundary_cache_root / "boundary_cache" / f"geoboundaries_{method}_{iso3}_{fname}"
            if flat.exists():
                return flat

        m2 = re.match(r"^boundary_cache/gadm41/([A-Z]{3})/(ADM\d+_kb\.geojson)$", s)
        if m2:
            iso3, fname = m2.group(1), m2.group(2)
            flat = boundary_cache_root / "boundary_cache" / f"gadm41_{iso3}_{fname}"
            if flat.exists():
                return flat

    if p.exists():
        return p
    return None


def load_geojson_gid_set(
    geojson_path: str,
    featureidkey: str,
    *,
    boundary_cache_root: Optional[Path] = None,
    boundary_cache_zip: Optional[Path] = None,
) -> Optional[Set[str]]:
    prop_key = _parse_featureidkey(featureidkey)
    if not prop_key:
        return None

    if boundary_cache_zip is not None and boundary_cache_zip.exists():
        try:
            zf = zipfile.ZipFile(boundary_cache_zip)
            target = geojson_path.replace("\\", "/")
            if target in zf.namelist():
                data = json.loads(zf.read(target))
                gids: Set[str] = set()
                for feat in data.get("features", []):
                    v = (feat.get("properties", {}) or {}).get(prop_key)
                    if v:
                        gids.add(normalize_gid(v))
                return gids

            m = re.match(
                r"^boundary_cache/geoboundaries/(gbOpen|gbHumanitarian|gbAuthoritative)/([A-Z]{3})/(ADM\d+_kb\.geojson)$",
                target,
            )
            if m:
                method, iso3, fname = m.group(1), m.group(2), m.group(3)
                flat = f"boundary_cache/geoboundaries_{method}_{iso3}_{fname}"
                if flat in zf.namelist():
                    data = json.loads(zf.read(flat))
                    gids = set()
                    for feat in data.get("features", []):
                        v = (feat.get("properties", {}) or {}).get(prop_key)
                        if v:
                            gids.add(normalize_gid(v))
                    return gids
        except Exception as e:
            eprint(f"[geojson] zip read failed ({geojson_path}): {e}")

    p = _resolve_geojson_path(geojson_path, boundary_cache_root)
    if p is None or not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        gids: Set[str] = set()
        for feat in data.get("features", []):
            v = (feat.get("properties", {}) or {}).get(prop_key)
            if v:
                gids.add(normalize_gid(v))
        return gids
    except Exception as e:
        eprint(f"[geojson] file read failed ({p}): {e}")
        return None


# ------------------------ CountryIndex -------------------------


@dataclass
class Admin1Rec:
    gid: str
    iso3: str
    admin1: str
    source: str
    geojson_path: str
    featureidkey: str
    geojson_url: str
    quality: float
    gid_method: str = ""
    alternative_gids: Set[str] = field(default_factory=set)


@dataclass
class Admin2Rec:
    gid: str
    iso3: str
    admin1: str
    admin2: str
    gid_admin1: str
    source: str
    geojson_path: str
    featureidkey: str
    geojson_url: str
    level_used: int
    quality: float
    gid_method: str = ""
    alternative_gids: Set[str] = field(default_factory=set)
    gid_admin1_alternative_gids: Set[str] = field(default_factory=set)


@dataclass
class CountryIndex:
    iso3: str
    admin0_name: str
    a1_records: Dict[str, Admin1Rec] = field(default_factory=dict)
    a2_records: Dict[str, Admin2Rec] = field(default_factory=dict)

    # matching maps
    a1_exact: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    a1_loose: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    a2_exact: Dict[Tuple[str, str], Set[str]] = field(default_factory=lambda: defaultdict(set))
    a2_loose: Dict[Tuple[str, str], Set[str]] = field(default_factory=lambda: defaultdict(set))
    a2_only: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    a1_choice_gids: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    a2_choice_cands: Dict[str, Set[Tuple[str, str, str]]] = field(default_factory=lambda: defaultdict(set))
    a2_choices_by_gid1: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    a1_geojson_gidsets: Dict[Tuple[str, str], Set[str]] = field(default_factory=dict)
    a2_geojson_gidsets: Dict[Tuple[str, str], Set[str]] = field(default_factory=dict)


def _choose_best_gid(gids: Set[str], records: Dict[str, Any]) -> str:
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


def build_country_index(
    kb_p: pd.DataFrame,
    *,
    iso3: str,
    boundary_cache_root: Optional[Path] = None,
    boundary_cache_zip: Optional[Path] = None,
    validate_geojson: bool = False,
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

    # 1) records (one per PRIMARY gid; keep best-quality; merge alternative gids)
    for _, r in df.iterrows():
        gid1 = normalize_gid(r.get("gid_admin1_eff") or "")
        if gid1:
            alt = set(_split_gid_list(r.get("admin1_alternative_gids") or ""))
            rec = Admin1Rec(
                gid=gid1,
                iso3=iso3,
                admin1=str(r.get("admin1_disp") or "").strip(),
                source=str(r.get("admin1_source_eff") or ""),
                geojson_path=str(r.get("admin1_geojson_path_eff") or ""),
                featureidkey=str(r.get("admin1_featureidkey_eff") or ""),
                geojson_url=str(r.get("admin1_geojson_url_eff") or ""),
                quality=float(as_float(r.get("admin1_quality"), default=0.0) or 0.0),
                gid_method=str(r.get("admin1_gid_method") or ""),
                alternative_gids=alt,
            )
            cur = idx.a1_records.get(gid1)
            if cur is None:
                idx.a1_records[gid1] = rec
            else:
                # merge alts always
                cur.alternative_gids |= rec.alternative_gids
                # keep best metadata
                if rec.quality > cur.quality or (rec.quality == cur.quality and len(rec.admin1) > len(cur.admin1)):
                    rec.alternative_gids = cur.alternative_gids
                    idx.a1_records[gid1] = rec

        gid2 = normalize_gid(r.get("gid_admin2_eff") or "")
        lvl = int(r.get("admin2_level_used_int") or 0)
        if gid2 and lvl >= 2:
            alt2 = set(_split_gid_list(r.get("admin2_alternative_gids") or ""))
            gid1_parent = normalize_gid(r.get("gid_admin1_eff") or "")

            gid1_alts: Set[str] = set()
            if is_gadm_gid(gid2):
                d = gadm_admin1_from_admin2_gid(gid2)
                if d and d != gid1_parent:
                    gid1_alts.add(d)

            rec2 = Admin2Rec(
                gid=gid2,
                iso3=iso3,
                admin1=str(r.get("admin1_disp") or "").strip(),
                admin2=str(r.get("admin2_disp") or "").strip(),
                gid_admin1=gid1_parent,
                source=str(r.get("admin2_source_eff") or ""),
                geojson_path=str(r.get("admin2_geojson_path_eff") or ""),
                featureidkey=str(r.get("admin2_featureidkey_eff") or ""),
                geojson_url=str(r.get("admin2_geojson_url_eff") or ""),
                level_used=lvl,
                quality=float(as_float(r.get("admin2_quality"), default=0.0) or 0.0),
                gid_method=str(r.get("admin2_gid_method") or ""),
                alternative_gids=alt2,
                gid_admin1_alternative_gids=gid1_alts,
            )
            cur2 = idx.a2_records.get(gid2)
            if cur2 is None:
                idx.a2_records[gid2] = rec2
            else:
                cur2.alternative_gids |= rec2.alternative_gids
                cur2.gid_admin1_alternative_gids |= rec2.gid_admin1_alternative_gids
                if rec2.quality > cur2.quality or (rec2.quality == cur2.quality and len(rec2.admin2) > len(cur2.admin2)):
                    rec2.alternative_gids = cur2.alternative_gids
                    rec2.gid_admin1_alternative_gids = cur2.gid_admin1_alternative_gids
                    idx.a2_records[gid2] = rec2

    # 2) name variants -> sets (map names to PRIMARY gids)
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
        gid1 = normalize_gid(r.get("gid_admin1_eff") or "")
        gid2 = normalize_gid(r.get("gid_admin2_eff") or "")
        lvl = int(r.get("admin2_level_used_int") or 0)
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

        if gid2 and lvl >= 2:
            gid1_parent = gid1
            for nm1 in a1_vars:
                a1s = norm_name_strict(nm1)
                a1l = norm_name_loose(nm1)
                for nm2 in a2_vars:
                    a2s = norm_name_strict(nm2)
                    a2l = norm_name_loose(nm2)
                    if a1s and a2s:
                        idx.a2_exact[(a1s, a2s)].add(gid2)
                    if a1l and a2l:
                        idx.a2_loose[(a1l, a2l)].add(gid2)
                        idx.a2_choice_cands[a2l].add((gid2, gid1_parent, a1l))
                        idx.a2_choices_by_gid1[gid1_parent].add(a2l)
                    if a2s:
                        idx.a2_only[a2s].add(gid2)

    # 3) optional geojson validation
    if validate_geojson:
        for rec in idx.a1_records.values():
            if not rec.geojson_path or not rec.featureidkey:
                continue
            key = (rec.geojson_path, rec.featureidkey)
            if key in idx.a1_geojson_gidsets:
                continue
            s = load_geojson_gid_set(
                rec.geojson_path,
                rec.featureidkey,
                boundary_cache_root=boundary_cache_root,
                boundary_cache_zip=boundary_cache_zip,
            )
            if s is not None:
                idx.a1_geojson_gidsets[key] = set(s)

        for rec in idx.a2_records.values():
            if not rec.geojson_path or not rec.featureidkey:
                continue
            key = (rec.geojson_path, rec.featureidkey)
            if key in idx.a2_geojson_gidsets:
                continue
            s = load_geojson_gid_set(
                rec.geojson_path,
                rec.featureidkey,
                boundary_cache_root=boundary_cache_root,
                boundary_cache_zip=boundary_cache_zip,
            )
            if s is not None:
                idx.a2_geojson_gidsets[key] = set(s)

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


def gid_is_valid_admin1(idx: CountryIndex, gid: str) -> bool:
    gid = normalize_gid(gid)
    rec = idx.a1_records.get(gid)
    if rec is None or not idx.a1_geojson_gidsets:
        return True
    if not rec.geojson_path or not rec.featureidkey:
        return True
    key = (rec.geojson_path, rec.featureidkey)
    if key in idx.a1_geojson_gidsets:
        return gid in idx.a1_geojson_gidsets[key]
    return True


def gid_is_valid_admin2(idx: CountryIndex, gid: str) -> bool:
    gid = normalize_gid(gid)
    rec = idx.a2_records.get(gid)
    if rec is None or not idx.a2_geojson_gidsets:
        return True
    if not rec.geojson_path or not rec.featureidkey:
        return True
    key = (rec.geojson_path, rec.featureidkey)
    if key in idx.a2_geojson_gidsets:
        return gid in idx.a2_geojson_gidsets[key]
    return True


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
        if gid_is_valid_admin1(idx, gid):
            return gid, "kb_exact"

    cands = idx.a1_loose.get(a1l, set())
    if cands:
        gid = _choose_best_gid(cands, idx.a1_records)
        if gid_is_valid_admin1(idx, gid):
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
        if gid_is_valid_admin1(idx, best_gid):
            return best_gid, f"fuzzy_a1:{best_score:.1f}"

    return "", f"ambiguous_a1:{best_score:.1f}:n{n_close}"


def resolve_admin2_gid(
    idx: CountryIndex, admin1_raw: str, admin2_raw: str, *, thr: MatchThresholds, fuzzy_limit: int = 12
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
        if gid_is_valid_admin2(idx, gid):
            return gid, "kb_exact"

    cands = idx.a2_loose.get((a1l, a2l), set())
    if cands:
        gid = _choose_best_gid(cands, idx.a2_records)
        if gid_is_valid_admin2(idx, gid):
            return gid, "kb_loose_exact" if len(cands) == 1 else "kb_loose_multi_best"

    cands = idx.a2_only.get(a2s, set())
    if cands and len(cands) == 1:
        gid = next(iter(cands))
        if gid_is_valid_admin2(idx, gid):
            return gid, "kb_admin2_only_unique"

    # fuzzy: restrict by admin1 if decent
    restrict_gid1: Set[str] = set()
    if a1:
        gid1, m1 = resolve_admin1_gid(idx, a1, thr=MatchThresholds(a1_accept=thr.restrict_a1_gate, a1_gap=0, a1_tie_margin=5))
        if gid1 and not m1.startswith("ambiguous"):
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
        for (gid2, gid1, a1_choice) in idx.a2_choice_cands.get(a2_choice, set()):
            score_a1 = float(fuzz.token_set_ratio(a1l, a1_choice)) if a1l else 0.0
            combined = float(score_a2) if not a1l else (0.65 * float(score_a2) + 0.35 * score_a1)
            gid2_best_score[gid2] = max(gid2_best_score[gid2], combined)
            gid2_best_a2[gid2] = max(gid2_best_a2[gid2], float(score_a2))

    sorted_g = sorted(gid2_best_score.items(), key=lambda x: x[1], reverse=True)
    best_gid, best_score = sorted_g[0]
    second_score = sorted_g[1][1] if len(sorted_g) > 1 else -1.0
    n_close = sum(1 for _, s in sorted_g if best_score - s <= thr.a2_tie_margin)

    if (best_score >= thr.a2_accept) and (((best_score - second_score) >= thr.a2_gap) or best_score >= 98.0) and n_close == 1:
        if gid_is_valid_admin2(idx, best_gid):
            return best_gid, f"fuzzy_a2:{best_score:.1f}"

    if not a1l:
        best_a2 = gid2_best_a2.get(best_gid, 0.0)
        a2_scores = sorted(gid2_best_a2.values(), reverse=True)
        second_a2 = a2_scores[1] if len(a2_scores) > 1 else -1.0
        if (best_a2 >= thr.a2_only_accept) and ((best_a2 - second_a2) >= thr.a2_only_gap):
            if gid_is_valid_admin2(idx, best_gid):
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


# ------------------------ env labels -------------------------


def extract_env_labels(env_classifier: Any) -> List[str]:
    if env_classifier is None:
        return []
    if isinstance(env_classifier, str):
        obj = safe_literal_eval(env_classifier)
        if obj is None:
            try:
                obj = json.loads(env_classifier)
            except Exception:
                obj = None
    else:
        obj = env_classifier
    if not isinstance(obj, dict):
        return []

    out: List[str] = []
    for key in ("env_max", "env_sec"):
        v = obj.get(key)
        if not v:
            continue
        vals = [str(x).strip() for x in v] if isinstance(v, list) else [str(v).strip()]
        for lab in vals:
            lab2 = lab.strip().lower()
            if lab2:
                out.append(lab2)

    # dedupe preserve order
    seen: Set[str] = set()
    dedup: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            dedup.append(x)
    return dedup


# ------------------------ sources grouping (mleed_counts-style) -------------------------


@dataclass
class SourceGroups:
    local: List[str]
    env_local: List[str]
    int_reg: List[str]
    domain_group: Dict[str, str] = field(default_factory=dict)


def build_source_lists(db, iso3: str) -> SourceGroups:
    iso3 = str(iso3).strip().upper()
    env_code = f"ENV_{iso3}"

    local_sources = [
        doc["source_domain"]
        for doc in db["sources"].find(
            {"primary_location": {"$in": [iso3]}, "include": True},
            projection={"_id": 0, "source_domain": 1},
        )
    ]
    env_local_sources = [
        doc["source_domain"]
        for doc in db["sources"].find(
            {"primary_location": {"$in": [env_code]}, "include": True},
            projection={"_id": 0, "source_domain": 1},
        )
    ]
    int_sources = [
        doc["source_domain"]
        for doc in db["sources"].find(
            {"$or": [{"major_international": True}, {"major_regional": True}], "include": True},
            projection={"_id": 0, "source_domain": 1},
        )
    ]

    local_sources = sorted(set(local_sources))
    env_local_sources = sorted(set(env_local_sources))
    int_sources = sorted(set(int_sources) - set(local_sources) - set(env_local_sources))

    domain_group: Dict[str, str] = {}
    for d in local_sources:
        domain_group[d] = "local"
    for d in env_local_sources:
        domain_group[d] = "env_local"
    for d in int_sources:
        domain_group[d] = "int_reg"

    return SourceGroups(local=local_sources, env_local=env_local_sources, int_reg=int_sources, domain_group=domain_group)


# ------------------------ mongo query builder -------------------------


def build_denom_query(
    iso3: str,
    domains: List[str],
    *,
    group_kind: str,  # "local_like" or "int_reg"
    english: bool,
    require_reconciled_locations: bool = True,
) -> Dict[str, Any]:
    iso3 = str(iso3).strip().upper()
    q: Dict[str, Any] = {
        "source_domain": {"$in": domains},
        "include": True,
        "language": "en" if english else {"$ne": "en"},
    }
    loc_field = "en_cliff_locations" if english else "cliff_locations"
    key = f"{loc_field}.{iso3}"

    if group_kind == "local_like":
        q["$or"] = [
            {key: {"$exists": True}},
            {loc_field: {}},
            {loc_field: {"$exists": False}},
        ]
    elif group_kind == "int_reg":
        q[key] = {"$exists": True}
    else:
        raise ValueError(f"Unknown group_kind={group_kind}")

    if require_reconciled_locations:
        q["reconciled_locations"] = {"$exists": True}
    return q


def compute_country_month_denom(
    db,
    *,
    iso3: str,
    year: int,
    month: int,
    local_like_domains: List[str],
    int_reg_domains: List[str],
) -> Dict[str, int]:
    iso3 = str(iso3).strip().upper()
    colname = COLLECTION_TEMPLATE.format(year=year, month=month)

    out: Dict[str, int] = {
        "denom_local_like_non_en": 0,
        "denom_local_like_en": 0,
        "denom_int_reg_non_en": 0,
        "denom_int_reg_en": 0,
    }

    if local_like_domains:
        q = build_denom_query(iso3, local_like_domains, group_kind="local_like", english=False, require_reconciled_locations=False)
        out["denom_local_like_non_en"] = int(db[colname].count_documents(q))
        q = build_denom_query(iso3, local_like_domains, group_kind="local_like", english=True, require_reconciled_locations=False)
        out["denom_local_like_en"] = int(db[colname].count_documents(q))

    if int_reg_domains:
        q = build_denom_query(iso3, int_reg_domains, group_kind="int_reg", english=False, require_reconciled_locations=False)
        out["denom_int_reg_non_en"] = int(db[colname].count_documents(q))
        q = build_denom_query(iso3, int_reg_domains, group_kind="int_reg", english=True, require_reconciled_locations=False)
        out["denom_int_reg_en"] = int(db[colname].count_documents(q))

    out["denom_country_total"] = int(
        out["denom_local_like_non_en"] + out["denom_local_like_en"] + out["denom_int_reg_non_en"] + out["denom_int_reg_en"]
    )
    return out


# ------------------------ ISO3 resolution per location -------------------------


def resolve_iso3_for_location(
    *,
    admin0_raw: str,
    admin1_raw: str,
    admin2_raw: str,
    admin0_map: Dict[str, str],
    target_iso3: str,
    idx_target: CountryIndex,
    thr: MatchThresholds,
) -> Tuple[str, str]:
    a0 = str(admin0_raw or "").strip()
    a1 = str(admin1_raw or "").strip()
    a2 = str(admin2_raw or "").strip()

    if ISO3_ONLY_RE.match(a0.upper()):
        return a0.upper(), "admin0_iso3_direct"

    iso3 = admin0_map.get(norm_name_strict(a0), "")
    if iso3:
        return iso3, "kb_admin0_map"

    iso3 = iso3_from_country_name(a0)
    if iso3:
        return iso3, "pycountry"

    # infer ONLY if admin0 missing/unknown and match is very confident in target
    if not a0 or a0.lower() == "unknown":
        if a2 and a2.lower() != "unknown" and idx_target.a2_choice_cands:
            gid2, m2 = resolve_admin2_gid(idx_target, a1, a2, thr=thr, fuzzy_limit=8)
            if gid2 and (m2.startswith("kb_") or m2.startswith("fuzzy_")):
                return target_iso3, "infer_target_by_admin2_match"

        if a1 and idx_target.a1_choice_gids:
            gid1, m1 = resolve_admin1_gid(
                idx_target, a1, thr=MatchThresholds(a1_accept=97.0, a1_gap=5.0, a1_tie_margin=2.0), fuzzy_limit=6
            )
            if gid1 and (m1.startswith("kb_") or m1.startswith("fuzzy_")):
                return target_iso3, "infer_target_by_admin1_match"

    return "", "no_iso3"


# ------------------------ metadata from index -------------------------


def build_admin_metadata_from_index(idx: CountryIndex) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    a1_rows = []
    for gid, rec in idx.a1_records.items():
        in_geo = ""
        if idx.a1_geojson_gidsets and rec.geojson_path and rec.featureidkey:
            key = (rec.geojson_path, rec.featureidkey)
            if key in idx.a1_geojson_gidsets:
                in_geo = "1" if gid in idx.a1_geojson_gidsets[key] else "0"
        alt_list = sorted(set([normalize_gid(x) for x in rec.alternative_gids if normalize_gid(x)]))
        a1_rows.append(
            {
                "gid": gid,
                "admin0": idx.admin0_name,
                "admin1": rec.admin1,
                "gid_method": rec.gid_method,
                "alternative_gids": "|".join(alt_list),
                "source": rec.source,
                "featureidkey": rec.featureidkey,
                "geojson_path": rec.geojson_path,
                "geojson_url": rec.geojson_url,
                "gid_in_geojson": in_geo,
            }
        )
    admin1_meta = pd.DataFrame(a1_rows)
    if not admin1_meta.empty:
        admin1_meta = admin1_meta.drop_duplicates(subset=["gid"]).sort_values("gid").reset_index(drop=True)

    a2_rows = []
    a2_to_a1: Dict[str, str] = {}
    for gid, rec in idx.a2_records.items():
        in_geo = ""
        if idx.a2_geojson_gidsets and rec.geojson_path and rec.featureidkey:
            key = (rec.geojson_path, rec.featureidkey)
            if key in idx.a2_geojson_gidsets:
                in_geo = "1" if gid in idx.a2_geojson_gidsets[key] else "0"
        alt_list2 = sorted(set([normalize_gid(x) for x in rec.alternative_gids if normalize_gid(x)]))
        gid1_alt_list = sorted(set([normalize_gid(x) for x in rec.gid_admin1_alternative_gids if normalize_gid(x)]))
        a2_rows.append(
            {
                "gid": gid,
                "admin0": idx.admin0_name,
                "admin1": rec.admin1,
                "admin2": rec.admin2,
                "gid_admin1": rec.gid_admin1,
                "gid_method": rec.gid_method,
                "alternative_gids": "|".join(alt_list2),
                "gid_admin1_alternative_gids": "|".join(gid1_alt_list),
                "source": rec.source,
                "featureidkey": rec.featureidkey,
                "geojson_path": rec.geojson_path,
                "geojson_url": rec.geojson_url,
                "gid_in_geojson": in_geo,
            }
        )
        if rec.gid_admin1:
            a2_to_a1[gid] = rec.gid_admin1

    admin2_meta = pd.DataFrame(a2_rows)
    if not admin2_meta.empty:
        admin2_meta = admin2_meta.drop_duplicates(subset=["gid"]).sort_values("gid").reset_index(drop=True)

    return admin1_meta, admin2_meta, a2_to_a1


# ------------------------ diagnostics -------------------------


@dataclass
class MonthDiagnostics:
    ym: str
    docs_scanned: int = 0
    docs_eligible: int = 0
    docs_with_any_gid: int = 0
    docs_with_admin1_gid: int = 0
    docs_with_admin2_gid: int = 0
    iso3_method: Counter = field(default_factory=Counter)
    iso3_fix: Counter = field(default_factory=Counter)
    admin1_method: Counter = field(default_factory=Counter)
    admin2_method: Counter = field(default_factory=Counter)
    unmatched_admin1: Counter = field(default_factory=Counter)  # (iso3, admin1_raw) -> n
    unmatched_admin2: Counter = field(default_factory=Counter)  # (iso3, admin1_raw, admin2_raw) -> n


# ------------------------ core month processing -------------------------


def process_month(
    db,
    *,
    iso3: str,
    year: int,
    month: int,
    event_types: List[str],
    idx_target: CountryIndex,
    admin0_map: Dict[str, str],
    thr: MatchThresholds,
    a2_to_a1_rollup: Dict[str, str],
    local_like_domains: List[str],
    int_reg_domains: List[str],
    out_article_admin1: Path,
    out_article_admin2: Path,
    batch_size: int = 500,
    # caches persist across months for speed (per country)
    a1_cache: Optional[Dict[str, Tuple[str, str]]] = None,
    a2_cache: Optional[Dict[Tuple[str, str], Tuple[str, str]]] = None,
) -> Tuple[Dict[Tuple[str, str], int], Dict[Tuple[str, str], int], Dict[str, int], Dict[str, int], MonthDiagnostics]:
    iso3 = str(iso3).strip().upper()
    ym = f"{year:04d}-{month:02d}"
    diag = MonthDiagnostics(ym=ym)

    raw_a1: Dict[Tuple[str, str], int] = defaultdict(int)
    raw_a2: Dict[Tuple[str, str], int] = defaultdict(int)
    denom_a1: Dict[str, int] = defaultdict(int)
    denom_a2: Dict[str, int] = defaultdict(int)

    colname = COLLECTION_TEMPLATE.format(year=year, month=month)
    evset = set([e.lower() for e in event_types])

    def _yield_docs(domains: List[str], group_kind: str) -> Iterator[Dict[str, Any]]:
        if not domains:
            return
        for english in (False, True):
            q = build_denom_query(iso3, domains, group_kind=group_kind, english=english, require_reconciled_locations=True)
            cur = db[colname].find(q, projection={k: 1 for k in DEFAULT_FIELDS}, batch_size=batch_size)
            for d in cur:
                yield d

    ensure_dir(out_article_admin1.parent)
    ensure_dir(out_article_admin2.parent)
    f1 = gzip.open(out_article_admin1, "wt", encoding="utf-8")
    f2 = gzip.open(out_article_admin2, "wt", encoding="utf-8")

    if a1_cache is None:
        a1_cache = {}
    if a2_cache is None:
        a2_cache = {}

    try:
        for d in _yield_docs(local_like_domains, "local_like"):
            diag.docs_scanned += 1
            _process_doc(
                d,
                iso3=iso3,
                ym=ym,
                colname=colname,
                event_types=evset,
                idx_target=idx_target,
                admin0_map=admin0_map,
                thr=thr,
                a2_to_a1_rollup=a2_to_a1_rollup,
                raw_a1=raw_a1,
                raw_a2=raw_a2,
                denom_a1=denom_a1,
                denom_a2=denom_a2,
                diag=diag,
                out_f_admin1=f1,
                out_f_admin2=f2,
                a1_cache=a1_cache,
                a2_cache=a2_cache,
            )
        for d in _yield_docs(int_reg_domains, "int_reg"):
            diag.docs_scanned += 1
            _process_doc(
                d,
                iso3=iso3,
                ym=ym,
                colname=colname,
                event_types=evset,
                idx_target=idx_target,
                admin0_map=admin0_map,
                thr=thr,
                a2_to_a1_rollup=a2_to_a1_rollup,
                raw_a1=raw_a1,
                raw_a2=raw_a2,
                denom_a1=denom_a1,
                denom_a2=denom_a2,
                diag=diag,
                out_f_admin1=f1,
                out_f_admin2=f2,
                a1_cache=a1_cache,
                a2_cache=a2_cache,
            )
    finally:
        f1.close()
        f2.close()

    return raw_a1, raw_a2, denom_a1, denom_a2, diag


def _process_doc(
    d: Dict[str, Any],
    *,
    iso3: str,
    ym: str,
    colname: str,
    event_types: Set[str],
    idx_target: CountryIndex,
    admin0_map: Dict[str, str],
    thr: MatchThresholds,
    a2_to_a1_rollup: Dict[str, str],
    raw_a1: Dict[Tuple[str, str], int],
    raw_a2: Dict[Tuple[str, str], int],
    denom_a1: Dict[str, int],
    denom_a2: Dict[str, int],
    diag: MonthDiagnostics,
    out_f_admin1,
    out_f_admin2,
    a1_cache: Dict[str, Tuple[str, str]],
    a2_cache: Dict[Tuple[str, str], Tuple[str, str]],
) -> None:
    if not d or not d.get("_id"):
        return
    if d.get("include") is False:
        return

    diag.docs_eligible += 1

    article_id = str(d.get("_id"))
    src = str(d.get("source_domain") or "")
    lang = str(d.get("language") or "")
    url = str(d.get("url") or "")
    title = trunc(d.get("title") or "", 240)
    date_publish = d.get("date_publish")

    labels = extract_env_labels(d.get("env_classifier"))
    labels = [lab for lab in labels if lab in event_types]

    locs = list(iter_locations_from_reconciled(d.get("reconciled_locations")))
    if not locs:
        return

    gids_a1: Set[str] = set()
    gids_a2: Set[str] = set()
    resolved_locs_admin1: List[Dict[str, Any]] = []
    resolved_locs_admin2: List[Dict[str, Any]] = []

    for loc in locs:
        lvl = str(loc.get("location_level") or "").strip().upper()
        admin0 = str(loc.get("admin0") or "")
        admin1 = str(loc.get("admin1") or "")
        admin2 = str(loc.get("admin2") or "")

        iso3_res, iso_method = resolve_iso3_for_location(
            admin0_raw=admin0,
            admin1_raw=admin1,
            admin2_raw=admin2,
            admin0_map=admin0_map,
            target_iso3=iso3,
            idx_target=idx_target,
            thr=thr,
        )
        diag.iso3_method[iso_method] += 1
        if not iso3_res:
            continue

        iso3_fixed, fix_reason = fix_iso3_common_errors(iso3_res, admin1, admin2)
        if fix_reason:
            diag.iso3_fix[fix_reason] += 1
        iso3_res = iso3_fixed
        if iso3_res != iso3:
            continue

        iso3_res, admin1, admin2 = apply_admin_aliases(iso3_res, admin1, admin2)

        if lvl == "ADMIN1":
            a1_key = norm_name_loose(admin1)
            if a1_key in a1_cache:
                gid1, m1 = a1_cache[a1_key]
            else:
                gid1, m1 = resolve_admin1_gid(idx_target, admin1, thr=thr)
                a1_cache[a1_key] = (gid1, m1)

            diag.admin1_method[m1] += 1
            if not gid1:
                diag.unmatched_admin1[(iso3_res, admin1)] += 1
                continue
            gid1 = normalize_gid(gid1)
            if gid1:
                gids_a1.add(gid1)
                resolved_locs_admin1.append({"level": "ADMIN1", "iso3": iso3_res, "admin1": admin1, "gid": gid1, "method": m1})

        elif lvl == "ADMIN2":
            if not admin2 or admin2.strip().lower() == "unknown":
                a1_key = norm_name_loose(admin1)
                if a1_key in a1_cache:
                    gid1, m1 = a1_cache[a1_key]
                else:
                    gid1, m1 = resolve_admin1_gid(idx_target, admin1, thr=thr)
                    a1_cache[a1_key] = (gid1, m1)

                diag.admin1_method[f"admin2_unknown_to_admin1::{m1}"] += 1
                if gid1:
                    gid1 = normalize_gid(gid1)
                    gids_a1.add(gid1)
                    resolved_locs_admin1.append(
                        {"level": "ADMIN1", "iso3": iso3_res, "admin1": admin1, "gid": gid1, "method": f"admin2_unknown_to_admin1::{m1}"}
                    )
                else:
                    diag.unmatched_admin1[(iso3_res, admin1)] += 1
                continue

            a2_key = (norm_name_loose(admin1), norm_name_loose(admin2))
            if a2_key in a2_cache:
                gid2, m2 = a2_cache[a2_key]
            else:
                gid2, m2 = resolve_admin2_gid(idx_target, admin1, admin2, thr=thr)
                a2_cache[a2_key] = (gid2, m2)

            diag.admin2_method[m2] += 1

            if gid2:
                gid2 = normalize_gid(gid2)
                gids_a2.add(gid2)
                resolved_locs_admin2.append({"level": "ADMIN2", "iso3": iso3_res, "admin1": admin1, "admin2": admin2, "gid": gid2, "method": m2})

                # roll up using KB mapping first (supports mixed/non-GADM)
                gid1 = a2_to_a1_rollup.get(gid2, "")
                if not gid1 and is_gadm_gid(gid2):
                    gid1 = gadm_admin1_from_admin2_gid(gid2)

                if not gid1:
                    a1_key = norm_name_loose(admin1)
                    gid1, m1 = a1_cache.get(a1_key, ("", ""))
                    if not gid1:
                        gid1, m1 = resolve_admin1_gid(idx_target, admin1, thr=thr)
                        a1_cache[a1_key] = (gid1, m1)
                    diag.admin1_method[f"rollup_from_admin2::{m1}"] += 1
                else:
                    m1 = "rollup_from_admin2::kb_gid2_to_gid1"

                gid1 = normalize_gid(gid1)
                if gid1:
                    gids_a1.add(gid1)
                    resolved_locs_admin1.append({"level": "ADMIN1", "iso3": iso3_res, "admin1": admin1, "gid": gid1, "method": m1})
            else:
                # ambiguous admin2 -> fallback admin1
                a1_key = norm_name_loose(admin1)
                if a1_key in a1_cache:
                    gid1, m1 = a1_cache[a1_key]
                else:
                    gid1, m1 = resolve_admin1_gid(idx_target, admin1, thr=thr)
                    a1_cache[a1_key] = (gid1, m1)

                diag.admin1_method[f"fallback_from_admin2::{m1}"] += 1
                if gid1:
                    gid1 = normalize_gid(gid1)
                    gids_a1.add(gid1)
                    resolved_locs_admin1.append(
                        {"level": "ADMIN1", "iso3": iso3_res, "admin1": admin1, "gid": gid1, "method": f"fallback_from_admin2::{m1}"}
                    )
                else:
                    diag.unmatched_admin2[(iso3_res, admin1, admin2)] += 1

    if not gids_a1 and not gids_a2:
        return

    diag.docs_with_any_gid += 1
    if gids_a1:
        diag.docs_with_admin1_gid += 1
    if gids_a2:
        diag.docs_with_admin2_gid += 1

    for g in gids_a1:
        denom_a1[g] += 1
    for g in gids_a2:
        denom_a2[g] += 1

    if labels:
        for g in gids_a1:
            for lab in labels:
                raw_a1[(g, lab)] += 1
        for g in gids_a2:
            for lab in labels:
                raw_a2[(g, lab)] += 1

    base_info = {
        "ym": ym,
        "collection": colname,
        "source_domain": src,
        "language": lang,
        "url": url,
        "date_publish": str(date_publish) if date_publish is not None else "",
        "title": title,
        "env_labels": labels,
        "resolved_locations_admin1": resolved_locs_admin1,
        "resolved_locations_admin2": resolved_locs_admin2,
    }

    if gids_a1:
        out_f_admin1.write(json.dumps({article_id: {**base_info, "gids": sorted(gids_a1), "concept_level": 1}}, ensure_ascii=False) + "\n")
    if gids_a2:
        out_f_admin2.write(json.dumps({article_id: {**base_info, "gids": sorted(gids_a2), "concept_level": 2}}, ensure_ascii=False) + "\n")


# ------------------------ output writers -------------------------


def make_counts_df(
    meta: pd.DataFrame,
    raw: Dict[Tuple[str, str], int],
    denom_map: Dict[str, int],
    event_types: List[str],
    *,
    denom_country_total: Optional[int] = None,
    norm_mode: str = "country",
    level: int = 1,
) -> pd.DataFrame:
    # NEW columns included in outputs:
    #   - gid_method
    #   - alternative_gids
    #   - (admin2 only) gid_admin1_alternative_gids
    if meta.empty:
        if level == 1:
            base_cols = ["gid", "admin0", "admin1", "gid_method", "alternative_gids"]
        else:
            base_cols = ["gid", "admin0", "admin1", "admin2", "gid_admin1", "gid_method", "alternative_gids", "gid_admin1_alternative_gids"]
        extra_cols = ["source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson"]
        cols = base_cols + extra_cols + [f"{e}_raw" for e in event_types] + [f"{e}_norm" for e in event_types]
        return pd.DataFrame(columns=cols)

    base = meta.copy()
    base["gid"] = base["gid"].astype(str).map(normalize_gid)

    for e in event_types:
        el = e.lower()
        base[f"{e}_raw"] = base["gid"].map(lambda g: int(raw.get((g, el), 0))).astype(int)

    nm = (norm_mode or "country").strip().lower()
    if nm == "country":
        denom_val = float(int(denom_country_total or 0))
        if denom_val == 0.0:
            denom_val = float("nan")
        for e in event_types:
            base[f"{e}_norm"] = (base[f"{e}_raw"].astype(float) / denom_val).fillna(0.0)
    else:
        denom_series = base["gid"].map(lambda g: int(denom_map.get(g, 0)))
        denom_vals = denom_series.astype(float).replace({0.0: float("nan")})
        for e in event_types:
            base[f"{e}_norm"] = (base[f"{e}_raw"].astype(float) / denom_vals).fillna(0.0)

    if level == 1:
        base_cols = ["gid", "admin0", "admin1", "gid_method", "alternative_gids"]
    else:
        base_cols = ["gid", "admin0", "admin1", "admin2", "gid_admin1", "gid_method", "alternative_gids", "gid_admin1_alternative_gids"]
    extra_cols = ["source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson"]
    out_cols = base_cols + extra_cols + [f"{e}_raw" for e in event_types] + [f"{e}_norm" for e in event_types]
    return base[out_cols]


def write_country_denom(ym: str, denom: Dict[str, int], out_path: Path) -> None:
    row = {"ym": ym}
    for k, v in (denom or {}).items():
        row[k] = int(v)
    df = pd.DataFrame([row])
    ensure_dir(out_path.parent)
    df.to_csv(out_path, index=False)


def write_denoms(meta_df: pd.DataFrame, denom_map: Dict[str, int], *, out_path: Path, level: int) -> None:
    if meta_df.empty:
        df = pd.DataFrame(columns=["gid", "denom"])
    else:
        df = meta_df.copy()
        df["gid"] = df["gid"].astype(str).map(normalize_gid)
        df["denom"] = df["gid"].map(lambda g: int(denom_map.get(g, 0))).astype(int)
        if level == 1:
            keep = [
                "gid",
                "admin0",
                "admin1",
                "gid_method",
                "alternative_gids",
                "source",
                "featureidkey",
                "geojson_path",
                "geojson_url",
                "gid_in_geojson",
                "denom",
            ]
        else:
            keep = [
                "gid",
                "admin0",
                "admin1",
                "admin2",
                "gid_admin1",
                "gid_method",
                "alternative_gids",
                "gid_admin1_alternative_gids",
                "source",
                "featureidkey",
                "geojson_path",
                "geojson_url",
                "gid_in_geojson",
                "denom",
            ]
        df = df[keep]
    ensure_dir(out_path.parent)
    df.to_csv(out_path, index=False)


def write_monthly_unmatched(diag: MonthDiagnostics, out_dir: Path, iso3: str) -> None:
    ensure_dir(out_dir)

    rows1 = [{"iso3": k[0], "admin1_raw": k[1], "n": int(v)} for k, v in diag.unmatched_admin1.items()]
    df1 = pd.DataFrame(rows1, columns=["iso3", "admin1_raw", "n"])
    if not df1.empty:
        df1 = df1.sort_values("n", ascending=False)
    df1.to_csv(out_dir / f"{iso3}_{diag.ym}_unmatched_admin1.csv", index=False)

    rows2 = [{"iso3": k[0], "admin1_raw": k[1], "admin2_raw": k[2], "n": int(v)} for k, v in diag.unmatched_admin2.items()]
    df2 = pd.DataFrame(rows2, columns=["iso3", "admin1_raw", "admin2_raw", "n"])
    if not df2.empty:
        df2 = df2.sort_values("n", ascending=False)
    df2.to_csv(out_dir / f"{iso3}_{diag.ym}_unmatched_admin2.csv", index=False)


# ------------------------ paths -------------------------


@dataclass
class RunPaths:
    admin1_counts_dir: Path
    admin2_counts_dir: Path
    other_dir: Path
    denom_country_dir: Path
    denom_a1_dir: Path
    denom_a2_dir: Path
    rec_a1_dir: Path
    rec_a2_dir: Path
    diag_monthly_dir: Path
    summary_dir: Path


def get_run_paths(base: Path, iso3: str, run_date: str) -> RunPaths:
    iso3 = iso3.upper()
    admin1_counts_dir = base / "Admin1" / iso3 / run_date
    admin2_counts_dir = base / "Admin2" / iso3 / run_date

    other_dir = base / "Other" / iso3 / run_date
    denom_country_dir = other_dir / "denominators" / "country"
    denom_a1_dir = other_dir / "denominators" / "admin1"
    denom_a2_dir = other_dir / "denominators" / "admin2"
    rec_a1_dir = other_dir / "article_records" / "admin1"
    rec_a2_dir = other_dir / "article_records" / "admin2"
    diag_monthly_dir = other_dir / "diagnostics" / "monthly"
    summary_dir = other_dir / "diagnostics" / "summaries"

    for p in [
        admin1_counts_dir,
        admin2_counts_dir,
        denom_country_dir,
        denom_a1_dir,
        denom_a2_dir,
        rec_a1_dir,
        rec_a2_dir,
        diag_monthly_dir,
        summary_dir,
    ]:
        ensure_dir(p)

    return RunPaths(
        admin1_counts_dir=admin1_counts_dir,
        admin2_counts_dir=admin2_counts_dir,
        other_dir=other_dir,
        denom_country_dir=denom_country_dir,
        denom_a1_dir=denom_a1_dir,
        denom_a2_dir=denom_a2_dir,
        rec_a1_dir=rec_a1_dir,
        rec_a2_dir=rec_a2_dir,
        diag_monthly_dir=diag_monthly_dir,
        summary_dir=summary_dir,
    )


# ------------------------ runner -------------------------


def run_country(db, *, iso3: str, kb_p: pd.DataFrame, admin0_map: Dict[str, str], event_types: List[str]) -> None:
    iso3 = iso3.upper()
    paths = get_run_paths(OUT_BASE, iso3, RUN_DATE)

    # sources
    src = build_source_lists(db, iso3)
    local_like_domains = src.local + src.env_local
    int_reg_domains = src.int_reg
    print(f"[sources] {iso3}: local={len(src.local)} env_local={len(src.env_local)} int_reg={len(src.int_reg)}")

    # country index + metadata
    idx_target = build_country_index(
        kb_p,
        iso3=iso3,
        boundary_cache_root=BOUNDARY_CACHE_ROOT,
        boundary_cache_zip=BOUNDARY_CACHE_ZIP,
        validate_geojson=VALIDATE_GEOJSON,
    )
    admin1_meta, admin2_meta, a2_to_a1 = build_admin_metadata_from_index(idx_target)
    print(f"[meta] {iso3}: ADMIN1 rows={len(admin1_meta)} ADMIN2 rows={len(admin2_meta)} validate_geojson={VALIDATE_GEOJSON}")

    if admin1_meta.empty:
        raise RuntimeError(f"No ADMIN1 metadata rows for {iso3}. Check KB.")

    # save run config
    run_config = {
        "iso3": iso3,
        "run_date": RUN_DATE,
        "start_ym": START_YM,
        "end_ym": END_YM,
        "mongo_db": MONGO_DB,
        "kb_path": str(KB_PATH),
        "kb_sheet": KB_SHEET,
        "boundary_cache_root": str(BOUNDARY_CACHE_ROOT) if BOUNDARY_CACHE_ROOT else "",
        "boundary_cache_zip": str(BOUNDARY_CACHE_ZIP) if BOUNDARY_CACHE_ZIP else "",
        "validate_geojson": bool(VALIDATE_GEOJSON),
        "manual_gadm_overrides": {
            "prefer_manual_gadm": bool(PREFER_MANUAL_GADM),
            "require_gadm_validation": bool(REQUIRE_GADM_VALIDATION),
            "admin1_lookup_parquet": str(GADM41_ADMIN1_GID_LOOKUP_PARQUET) if GADM41_ADMIN1_GID_LOOKUP_PARQUET else "",
            "admin2_lookup_parquet": str(GADM41_ADMIN2_GID_LOOKUP_PARQUET) if GADM41_ADMIN2_GID_LOOKUP_PARQUET else "",
        },
        "sources": {"local_n": len(src.local), "env_local_n": len(src.env_local), "int_reg_n": len(src.int_reg)},
        "event_types": event_types,
        "thresholds": THR.__dict__,
        "rapidfuzz": bool(HAVE_RAPIDFUZZ),
        "created_at": _dt.datetime.now().isoformat(),
    }
    (paths.other_dir / "run_config.json").write_text(json.dumps(run_config, indent=2), encoding="utf-8")

    # persistent caches across months for speed
    a1_cache: Dict[str, Tuple[str, str]] = {}
    a2_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}

    # month loop
    summary_rows = []
    for y, m in iter_months(START_YM, END_YM):
        ym = f"{y:04d}-{m:02d}"
        print(f"[{iso3}] Processing {ym} ...")

        out_rec_a1 = paths.rec_a1_dir / f"{iso3}_admin1_{ym}.jsonl.gz"
        out_rec_a2 = paths.rec_a2_dir / f"{iso3}_admin2_{ym}.jsonl.gz"

        raw_a1, raw_a2, denom_a1, denom_a2, diag = process_month(
            db,
            iso3=iso3,
            year=y,
            month=m,
            event_types=event_types,
            idx_target=idx_target,
            admin0_map=admin0_map,
            thr=THR,
            a2_to_a1_rollup=a2_to_a1,
            local_like_domains=local_like_domains,
            int_reg_domains=int_reg_domains,
            out_article_admin1=out_rec_a1,
            out_article_admin2=out_rec_a2,
            batch_size=BATCH_SIZE,
            a1_cache=a1_cache,
            a2_cache=a2_cache,
        )

        denom_country = compute_country_month_denom(
            db,
            iso3=iso3,
            year=y,
            month=m,
            local_like_domains=local_like_domains,
            int_reg_domains=int_reg_domains,
        )
        denom_country_total = int(denom_country.get("denom_country_total", 0))
        write_country_denom(ym, denom_country, paths.denom_country_dir / f"{iso3}_{ym}_denom_country.csv")

        # wide dfs
        a1_df = make_counts_df(
            admin1_meta,
            raw_a1,
            denom_a1,
            event_types,
            denom_country_total=denom_country_total,
            norm_mode=NORM_MODE,
            level=1,
        )
        a2_df = make_counts_df(
            admin2_meta,
            raw_a2,
            denom_a2,
            event_types,
            denom_country_total=denom_country_total,
            norm_mode=NORM_MODE,
            level=2,
        )

        # write counts into Admin1/Admin2 dropbox trees
        a1_df.to_csv(paths.admin1_counts_dir / f"counts_{iso3}_admin1_{ym}.csv", index=False)
        a2_df.to_csv(paths.admin2_counts_dir / f"counts_{iso3}_admin2_{ym}.csv", index=False)

        # denominators + unmatched into Other tree
        write_denoms(admin1_meta, denom_a1, out_path=paths.denom_a1_dir / f"{iso3}_{ym}_admin1_denom.csv", level=1)
        write_denoms(admin2_meta, denom_a2, out_path=paths.denom_a2_dir / f"{iso3}_{ym}_admin2_denom.csv", level=2)
        write_monthly_unmatched(diag, paths.diag_monthly_dir, iso3)

        summary_rows.append(
            {
                "iso3": iso3,
                "ym": ym,
                "docs_scanned": diag.docs_scanned,
                "docs_eligible": diag.docs_eligible,
                "denom_country_total": denom_country_total,
                "docs_with_any_gid": diag.docs_with_any_gid,
                "docs_with_admin1_gid": diag.docs_with_admin1_gid,
                "docs_with_admin2_gid": diag.docs_with_admin2_gid,
                "unmatched_admin1_nunique": len(diag.unmatched_admin1),
                "unmatched_admin2_nunique": len(diag.unmatched_admin2),
            }
        )

        print(
            f"[{iso3} {ym}] scanned={diag.docs_scanned} eligible={diag.docs_eligible} denom_country={denom_country_total} "
            f"any_gid={diag.docs_with_any_gid} a1_docs={diag.docs_with_admin1_gid} a2_docs={diag.docs_with_admin2_gid}"
        )

    # write summary for the country
    if summary_rows:
        pd.DataFrame(summary_rows).to_csv(paths.summary_dir / f"{iso3}_summary_{START_YM}_to_{END_YM}.csv", index=False)


def main() -> None:
    if not MONGO_URI:
        raise RuntimeError(
            "MONGO_URI is empty. Set env var ML4P_MONGO_URI before running, e.g.\n"
            "  export ML4P_MONGO_URI='mongodb://user:pass@host/?authSource=ml4p&tls=true'"
        )
    if not COUNTRIES:
        raise RuntimeError("COUNTRIES is empty in CONFIG. Add ISO3 codes to run.")

    # Connect Mongo
    try:
        from pymongo import MongoClient  # type: ignore
    except Exception as e:
        raise RuntimeError("pymongo is required. Install it in your environment.") from e

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Build GADM validator (NEW)
    gadm_a1_set = load_gadm_gid_set_from_parquet(GADM41_ADMIN1_GID_LOOKUP_PARQUET, level=1)
    gadm_a2_set = load_gadm_gid_set_from_parquet(GADM41_ADMIN2_GID_LOOKUP_PARQUET, level=2)
    gadm_validator = GadmValidator(
        admin1_global=gadm_a1_set,
        admin2_global=gadm_a2_set,
        boundary_cache_root=BOUNDARY_CACHE_ROOT,
        boundary_cache_zip=BOUNDARY_CACHE_ZIP,
        require_validation=REQUIRE_GADM_VALIDATION,
    )
    print(
        f"[gadm_validation] prefer_manual={PREFER_MANUAL_GADM} require_validation={REQUIRE_GADM_VALIDATION} "
        f"parquet_a1={'yes' if gadm_a1_set is not None else 'no'} parquet_a2={'yes' if gadm_a2_set is not None else 'no'} "
        f"cache_zip={'yes' if (BOUNDARY_CACHE_ZIP and BOUNDARY_CACHE_ZIP.exists()) else 'no'} cache_root={'yes' if (BOUNDARY_CACHE_ROOT and BOUNDARY_CACHE_ROOT.exists()) else 'no'}"
    )

    # Load + prepare KB once
    print("[kb] Loading KB...")
    kb = load_kb(KB_PATH, sheet=KB_SHEET)
    kb_p = prepare_kb_df(kb, gadm_validator=gadm_validator, prefer_manual_gadm=PREFER_MANUAL_GADM)
    admin0_map = build_admin0_map(kb_p)
    print(f"[kb] Loaded rows={len(kb_p):,} countries={kb_p['Admin0_ISO3'].nunique():,}")

    # Run each country
    event_types = ENV_EVENT_TYPES
    print(f"[event_types] {len(event_types)} labels; norm_mode={NORM_MODE}; run_date={RUN_DATE}; rapidfuzz={HAVE_RAPIDFUZZ}")
    print(f"[out] base={OUT_BASE}")

    for iso3 in COUNTRIES:
        iso3 = str(iso3).strip().upper()
        if not iso3:
            continue
        print("\n" + "=" * 70)
        print(f"[run] ISO3={iso3}")
        print("=" * 70)
        try:
            run_country(db, iso3=iso3, kb_p=kb_p, admin0_map=admin0_map, event_types=event_types)
        except Exception as e:
            eprint(f"[ERROR] {iso3}: {e}")
            # keep going for other countries
            continue

    print("\n[done] All requested countries processed.")


if __name__ == "__main__":
    main()
