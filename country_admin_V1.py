#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
country_admin_counts_kb_v3_gidcols_batch.py

Batch (multi-country) subnational ADMIN1/ADMIN2 environmental event counts for MLEED.

OPTIMIZED VERSION (2026-01)
--------------------------
This version is designed to be *much faster* than the KB/fuzzy-matching approach because it
uses precomputed GID columns stored directly on each Mongo article document.

Instead of parsing `reconciled_locations` and matching names to GIDs each run, this script expects
that a prior "gid matching" step has already written per-article Admin1/Admin2 GIDs into Mongo.

You configure everything in the CONFIG block below (countries, mongo uri, date range, paths).

Expected Mongo fields (configurable)
-----------------------------------
You should have two precomputed fields on each article document (names are configurable):
  - an "admin1 gids" field: list/dict/string containing all matched Admin1 GIDs for that article
  - an "admin2 gids" field: list/dict/string containing all matched Admin2 GIDs for that article

The values may be:
  - list of strings: ["KEN.1_1", "KEN.2_1", ...]
  - dict keyed by ISO3: {"KEN": ["KEN.1_1", ...], "UGA": [...]}
  - stringified JSON / python literal / pipe-separated gids

This script will:
  - Count doc-level occurrences per (gid, env_label) for admin1 and admin2.
  - Build per-gid denominators as "number of docs mentioning the gid".
  - Optionally roll up admin2 -> admin1 for admin1 counts (keeps backward compatibility).

Metadata / sorting (to keep outputs readable)
---------------------------------------------
To keep the monthly CSV outputs tidy (not “gid order chaos”), the script will join in names and
sort by:
  - Admin1 outputs: admin1 name, then gid
  - Admin2 outputs: admin1 name, admin2 name, then gid

Recommended metadata source is `boundary_gid_index.parquet` (produced by your KB finalizer).
If that parquet is unavailable, the script can fall back to the KB spreadsheet (limited metadata).

Outputs (Dropbox structure)
---------------------------
Counts go here:
  {OUT_BASE}/Admin1/{ISO3}/{RUN_DATE}/counts_{ISO3}_admin1_{YYYY-MM}.csv
  {OUT_BASE}/Admin2/{ISO3}/{RUN_DATE}/counts_{ISO3}_admin2_{YYYY-MM}.csv

Other artifacts (denominators, diagnostics, article records, run config) go here:
  {OUT_BASE}/Other/{ISO3}/{RUN_DATE}/...

Dependencies
------------
- pandas
- pymongo
- (recommended) pyarrow or fastparquet (only if you use boundary_gid_index parquet)

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
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pandas as pd


# ============================================================
# =========================== CONFIG =========================
# ============================================================

# --- Mongo ---
# Strongly recommended: set via env var so you don't commit credentials:
#   export ML4P_MONGO_URI="mongodb://user:pass@host/?authSource=ml4p&tls=true"
MONGO_URI: str = os.environ.get("ML4P_MONGO_URI", "mongodb://zungru:balsas.rial.tanoaks.schmoe.coffing@db-wibbels.sas.upenn.edu/?authSource=ml4p&tls=true").strip()
MONGO_DB: str = os.environ.get("ML4P_MONGO_DB", "ml4p").strip()

# --- Countries + time range ---
COUNTRIES: List[str] = [
    "ALB",
    "BEN",
    "COL",
    "ECU",
    "ETH",
    "GEO",
    "KEN",
    "PRY",
    "MLI",
    "MAR",
    "NGA",
    "SRB",
    "SEN",
    "TZA",
    "UGA",
    "UKR",
    "ZWE",
    "MRT",
    "ZMB",
    "XKX",
    "NER",
    "JAM",
    "HND",
    "PHL",
    "GHA",
    "RWA",
    "GTM",
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

# --- Precomputed GID fields in Mongo documents (NEW) ---
# If you know the exact field names, set them here (or via env vars below).
# If you aren't sure, leave as-is; the script will try several common candidates.
_DOC_A1_ENV = os.environ.get("ML4P_DOC_GIDS_ADMIN1_FIELD", "").strip()
_DOC_A2_ENV = os.environ.get("ML4P_DOC_GIDS_ADMIN2_FIELD", "").strip()

DOC_ADMIN1_GID_FIELDS: List[str] = [
    x for x in [
        _DOC_A1_ENV,
        "matched_gids_admin1",
        "matched_admin1_gids",
        "admin1_gids",
        "gids_admin1",
        "kb_admin1_gids",
        "final_admin1_gids",
        "admin1_gid_list",
    ] if x
]
DOC_ADMIN2_GID_FIELDS: List[str] = [
    x for x in [
        _DOC_A2_ENV,
        "matched_gids_admin2",
        "matched_admin2_gids",
        "admin2_gids",
        "gids_admin2",
        "kb_admin2_gids",
        "final_admin2_gids",
        "admin2_gid_list",
    ] if x
]

# de-dupe preserve order (env var may duplicate a default candidate)
DOC_ADMIN1_GID_FIELDS = list(dict.fromkeys(DOC_ADMIN1_GID_FIELDS))
DOC_ADMIN2_GID_FIELDS = list(dict.fromkeys(DOC_ADMIN2_GID_FIELDS))


# --- Boundary GID index parquet (recommended for names + sorting) ---
# You can set via env var:
#   export ML4P_BOUNDARY_GID_INDEX_PARQUET="/path/to/boundary_gid_index.parquet"
_BOUNDARY_ENV = os.environ.get("ML4P_BOUNDARY_GID_INDEX_PARQUET", "").strip()
_BOUNDARY_DEFAULT = "/home/ml4p/Downloads/peace-pipeline/peace-machine/peacemachine/scripts/boundary_gid_index.parquet"
if _BOUNDARY_ENV:
    BOUNDARY_GID_INDEX_PARQUET: Optional[Path] = Path(_BOUNDARY_ENV)
else:
    _p = Path(_BOUNDARY_DEFAULT)
    BOUNDARY_GID_INDEX_PARQUET = _p if _p.exists() else None

# --- KB fallback (optional) ---
# Only used if boundary_gid_index parquet is unavailable.
KB_PATH: Optional[Path] = None  # e.g., Path("/home/ml4p/.../KB_fresh_start_V3.xlsx")
KB_SHEET: Optional[str | int] = 0

# --- Output base (Dropbox) ---
OUT_BASE: Path = Path("/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational")

# --- Run date folder name ---
RUN_DATE: str = os.environ.get("ML4P_RUN_DATE", "").strip() or _dt.date.today().strftime("%Y_%m_%d")

# --- Performance ---
BATCH_SIZE: int = 800  # Mongo batch size (network/driver)
NORM_MODE: str = "country"  # "country" or "gid"

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

# Base fields we always need from Mongo
BASE_FIELDS = [
    "_id",
    "date_publish",
    "source_domain",
    "url",
    "language",
    "title",
    "include",
    "env_classifier",
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



_ISO3_SECOND_TOKEN_PAT_CACHE: Dict[str, re.Pattern] = {}

def _iso3_accepts_gid(gid: str, iso3: str) -> bool:
    """
    True if `gid` appears to belong to `iso3`.
    Accepts either:
      - ISO3-prefixed gids:   ISO3....   (e.g., "KEN.1_1")
      - prefixed systems:    XX.ISO3.... (e.g., "GB.KEN.1_1")

    This keeps geoboundaries-style KB gids (often normalized to "GB.<ISO3>...")
    while still preventing cross-country contamination for multi-country articles.
    """
    iso3 = str(iso3 or "").strip().upper()
    if not iso3:
        return True
    g = normalize_gid(gid)
    if not g:
        return False
    if g.startswith(f"{iso3}."):
        return True
    pat = _ISO3_SECOND_TOKEN_PAT_CACHE.get(iso3)
    if pat is None:
        # allow prefixes like "GB.<ISO3>." or other short codes "XX.<ISO3>."
        pat = re.compile(rf"^[A-Z]{{2,6}}\.{iso3}\.")
        _ISO3_SECOND_TOKEN_PAT_CACHE[iso3] = pat
    return bool(pat.match(g))

def _looks_like_iso3_keys(d: Dict[str, Any]) -> bool:
    if not d:
        return False
    # heuristic: most keys are 3-letter-ish codes
    keys = list(d.keys())
    if len(keys) == 0:
        return False
    score = 0
    for k in keys[:10]:
        ku = str(k).strip()
        if re.match(r"^[A-Za-z]{3}$", ku):
            score += 1
    return score >= max(1, min(3, len(keys)))


def _iter_gids_any(obj: Any) -> Iterator[str]:
    """Yield (possibly unnormalized) gid strings from arbitrarily nested structures."""
    if obj is None:
        return

    if isinstance(obj, str):
        s = obj.strip()
        if not s:
            return
        parsed = None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            parsed = parse_maybe_json_or_literal(s)
        if parsed is not None and parsed is not obj:
            yield from _iter_gids_any(parsed)
            return
        # delimiter fallback
        if "|" in s:
            for t in s.split("|"):
                t = t.strip()
                if t:
                    yield t
            return
        # comma/semicolon fallback
        if "," in s or ";" in s:
            for t in re.split(r"[;,]\s*", s):
                t = t.strip()
                if t:
                    yield t
            return
        yield s
        return

    if isinstance(obj, (list, tuple, set)):
        for v in obj:
            yield from _iter_gids_any(v)
        return

    if isinstance(obj, dict):
        # common patterns: {"gid": "..."} or {"kb_gid": "..."}
        for key in ("gid", "kb_gid", "KB_GID", "KB_GID_1", "KB_GID_2"):
            if key in obj:
                yield from _iter_gids_any(obj.get(key))
                return
        # otherwise traverse values
        for v in obj.values():
            yield from _iter_gids_any(v)
        return

    # fallback: stringify
    try:
        s = str(obj).strip()
        if s:
            yield s
    except Exception:
        return


def extract_gid_set(value: Any, *, iso3: str) -> Set[str]:
    """
    Extract a *set* of normalized GIDs for a target ISO3 from a stored Mongo field value.
    Handles list/dict/stringified structures.

    Important: not all KB gid systems are ISO3-prefixed (e.g., geoboundaries often normalize to
    "GB.<ISO3>..."), so we use `_iso3_accepts_gid` rather than a simple startswith(ISO3).
    """
    iso3 = str(iso3 or "").strip().upper()
    if not value:
        return set()

    # If it's a dict keyed by ISO3, select the relevant sub-value (faster + avoids cross-country noise)
    if isinstance(value, dict) and _looks_like_iso3_keys(value):
        sub = value.get(iso3) or value.get(iso3.lower()) or value.get(iso3.title())
        if sub is not None:
            value = sub

    out: Set[str] = set()
    for raw in _iter_gids_any(value):
        g = normalize_gid(raw)
        if not g:
            continue
        if iso3 and (not _iso3_accepts_gid(g, iso3)):
            continue
        out.add(g)
    return out

def extract_first_nonempty_gid_field(
    doc: Dict[str, Any],
    *,
    iso3: str,
    candidate_fields: Sequence[str],
) -> Tuple[Set[str], str]:
    """
    Try fields in order and return (gid_set, field_name_used).
    If a field exists but is empty for this ISO3, we keep searching (so you can pass multiple candidates).
    """
    for f in candidate_fields:
        if not f:
            continue
        if f not in doc:
            continue
        gids = extract_gid_set(doc.get(f), iso3=iso3)
        if gids:
            return gids, f
    return set(), ""


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
) -> Dict[str, Any]:
    """
    Denominator query: just selects the doc universe, not requiring precomputed gid fields.
    """
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

    return q


def gid_presence_query(fields: Sequence[str]) -> Dict[str, Any]:
    """
    Query fragment that tries to exclude docs without any usable gid field.
    Uses $nin to exclude common 'empty' sentinels.
    """
    ors: List[Dict[str, Any]] = []
    for f in fields:
        if not f:
            continue
        ors.append({f: {"$exists": True, "$nin": [None, "", [], {}]}})
    if not ors:
        return {}
    return {"$or": ors}


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
        q = build_denom_query(iso3, local_like_domains, group_kind="local_like", english=False)
        out["denom_local_like_non_en"] = int(db[colname].count_documents(q))
        q = build_denom_query(iso3, local_like_domains, group_kind="local_like", english=True)
        out["denom_local_like_en"] = int(db[colname].count_documents(q))

    if int_reg_domains:
        q = build_denom_query(iso3, int_reg_domains, group_kind="int_reg", english=False)
        out["denom_int_reg_non_en"] = int(db[colname].count_documents(q))
        q = build_denom_query(iso3, int_reg_domains, group_kind="int_reg", english=True)
        out["denom_int_reg_en"] = int(db[colname].count_documents(q))

    out["denom_country_total"] = int(
        out["denom_local_like_non_en"] + out["denom_local_like_en"] + out["denom_int_reg_non_en"] + out["denom_int_reg_en"]
    )
    return out


# ------------------------ metadata loading (parquet / KB fallback) -------------------------

try:
    import pycountry  # type: ignore
except Exception:
    pycountry = None


def country_name_from_iso3(iso3: str) -> str:
    iso3 = str(iso3 or "").strip().upper()
    if not iso3:
        return ""
    if not pycountry:
        return iso3
    try:
        c = pycountry.countries.get(alpha_3=iso3)
        return str(c.name) if c else iso3
    except Exception:
        return iso3


def load_boundary_gid_index(path: Path) -> pd.DataFrame:
    """
    Loads the boundary_gid_index parquet.
    Expected (minimum) columns:
      - kb_gid, iso3, adm_level, source, geojson_relpath, featureidkey, name, properties_json
    """
    if not path.exists():
        raise FileNotFoundError(f"boundary_gid_index parquet not found: {path}")
    try:
        df = pd.read_parquet(path)  # requires pyarrow or fastparquet
    except Exception as e:
        raise RuntimeError(
            f"Failed to read parquet {path}. Install pyarrow or fastparquet in your env.\n"
            f"Original error: {e}"
        ) from e

    df = df.copy()
    df.columns = [str(c) for c in df.columns]
    for c in ["kb_gid", "iso3", "adm_level", "source", "geojson_relpath", "featureidkey", "name", "properties_json", "gid_field", "name_field"]:
        if c not in df.columns:
            df[c] = ""
    df["kb_gid"] = df["kb_gid"].astype(str).map(normalize_gid)
    df["iso3"] = df["iso3"].astype(str).str.strip().str.upper()
    # adm_level should be int-ish
    def _to_int(x: Any) -> int:
        try:
            s = str(x).strip()
            if not s:
                return -1
            if s.isdigit():
                return int(s)
            return int(float(s))
        except Exception:
            return -1
    df["adm_level"] = df["adm_level"].apply(_to_int)
    df = df[df["kb_gid"].astype(str).str.strip() != ""].copy()
    return df


_PARENT_GID_KEYS = [
    "KB_GID_1",
    "GID_1",
    "kb_gid_1",
    "gid_1",
    "gid_admin1",
    "admin1_gid",
    "ADM1_GID",
    "ADMIN1_GID",
]
_PARENT_NAME_KEYS = [
    "NAME_1",
    "NAME1",
    "ADMIN1",
    "Admin1",
    "admin1",
    "ADM1_NAME",
    "adm1_name",
]


def _parse_properties_json(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    if isinstance(s, dict):
        return s
    if not isinstance(s, str):
        return {}
    t = s.strip()
    if not t:
        return {}
    try:
        obj = json.loads(t)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def build_meta_from_boundary_index(df_idx: pd.DataFrame, *, iso3: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    """
    Returns:
      admin1_meta, admin2_meta, a2_to_a1_rollup
    """
    iso3 = str(iso3).strip().upper()
    dfc = df_idx[df_idx["iso3"] == iso3].copy()
    if dfc.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    admin0_name = country_name_from_iso3(iso3)

    # Admin1
    a1 = dfc[dfc["adm_level"] == 1].copy()
    if not a1.empty:
        a1_meta = pd.DataFrame(
            {
                "gid": a1["kb_gid"].astype(str),
                "admin0": admin0_name,
                "admin1": a1["name"].astype(str),
                "gid_method": "",
                "alternative_gids": "",
                "source": a1["source"].astype(str),
                "featureidkey": a1["featureidkey"].astype(str),
                "geojson_path": a1["geojson_relpath"].astype(str),
                "geojson_url": "",
                "gid_in_geojson": "",
                "gid_field": a1.get("gid_field", "").astype(str),
                "name_field": a1.get("name_field", "").astype(str),
            }
        )
        a1_meta = a1_meta.drop_duplicates(subset=["gid"]).reset_index(drop=True)
    else:
        a1_meta = pd.DataFrame(
            columns=[
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
                "gid_field",
                "name_field",
            ]
        )

    gid_to_a1name: Dict[str, str] = {}
    if not a1_meta.empty:
        gid_to_a1name = dict(zip(a1_meta["gid"].astype(str), a1_meta["admin1"].astype(str)))

    # Admin2
    a2 = dfc[dfc["adm_level"] == 2].copy()
    a2_rows = []
    a2_to_a1: Dict[str, str] = {}

    for _, r in a2.iterrows():
        gid2 = normalize_gid(r.get("kb_gid") or "")
        if not gid2:
            continue
        props = _parse_properties_json(r.get("properties_json"))
        gid1 = ""
        for k in _PARENT_GID_KEYS:
            if k in props and props.get(k):
                gid1 = normalize_gid(props.get(k))
                break
        if not gid1 and is_gadm_gid(gid2):
            gid1 = gadm_admin1_from_admin2_gid(gid2)

        a1name = ""
        for k in _PARENT_NAME_KEYS:
            if k in props and props.get(k):
                a1name = str(props.get(k)).strip()
                break
        if not a1name and gid1:
            a1name = gid_to_a1name.get(gid1, "")

        a2name = str(r.get("name") or "").strip()

        a2_rows.append(
            {
                "gid": gid2,
                "admin0": admin0_name,
                "admin1": a1name,
                "admin2": a2name,
                "gid_admin1": gid1,
                "gid_method": "",
                "alternative_gids": "",
                "gid_admin1_alternative_gids": "",
                "source": str(r.get("source") or ""),
                "featureidkey": str(r.get("featureidkey") or ""),
                "geojson_path": str(r.get("geojson_relpath") or ""),
                "geojson_url": "",
                "gid_in_geojson": "",
                "gid_field": str(r.get("gid_field") or ""),
                "name_field": str(r.get("name_field") or ""),
            }
        )
        if gid1:
            a2_to_a1[gid2] = gid1

    a2_meta = pd.DataFrame(a2_rows)
    if not a2_meta.empty:
        a2_meta = a2_meta.drop_duplicates(subset=["gid"]).reset_index(drop=True)

    # Sorting to keep outputs tidy
    if not a1_meta.empty:
        a1_meta = a1_meta.sort_values(["admin1", "gid"], kind="mergesort").reset_index(drop=True)
    if not a2_meta.empty:
        # admin1 name may be empty; still stable
        a2_meta = a2_meta.sort_values(["admin1", "admin2", "gid"], kind="mergesort").reset_index(drop=True)

    return a1_meta, a2_meta, a2_to_a1


def load_kb(kb_path: Path, sheet: Optional[str | int] = None) -> pd.DataFrame:
    if not kb_path.exists():
        raise FileNotFoundError(f"KB not found: {kb_path}")
    ext = kb_path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(kb_path, sheet_name=sheet if sheet is not None else 0, dtype=str)
    else:
        df = pd.read_csv(kb_path, dtype=str, low_memory=False)
    df = df.fillna("")
    return df


def build_meta_from_kb(kb: pd.DataFrame, *, iso3: str) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    """
    Minimal KB fallback: uses KB columns Admin0_ISO3, Admin1, Admin2, gid_admin1, gid_admin2.
    """
    iso3 = str(iso3).strip().upper()
    kb = kb.copy().fillna("")
    if "Admin0_ISO3" not in kb.columns:
        return pd.DataFrame(), pd.DataFrame(), {}
    kbc = kb[kb["Admin0_ISO3"].astype(str).str.strip().str.upper() == iso3].copy()
    if kbc.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    admin0_name = ""
    if "Admin0" in kbc.columns:
        vals = kbc["Admin0"].astype(str).str.strip()
        vals = vals[vals != ""]
        if not vals.empty:
            admin0_name = vals.value_counts().idxmax()
    if not admin0_name:
        admin0_name = country_name_from_iso3(iso3)

    for col in ["gid_admin1", "gid_admin2"]:
        if col not in kbc.columns:
            kbc[col] = ""
        kbc[col] = kbc[col].map(normalize_gid)

    # Admin1 meta
    a1 = kbc[kbc["gid_admin1"].astype(str).str.strip() != ""].copy()
    a1_meta = pd.DataFrame(
        {
            "gid": a1["gid_admin1"].astype(str),
            "admin0": admin0_name,
            "admin1": a1.get("Admin1", "").astype(str),
            "gid_method": "",
            "alternative_gids": "",
            "source": a1.get("admin1_source", "").astype(str),
            "featureidkey": a1.get("admin1_featureidkey", "").astype(str),
            "geojson_path": a1.get("admin1_geojson_path", "").astype(str),
            "geojson_url": a1.get("admin1_geojson_url", "").astype(str),
            "gid_in_geojson": "",
        }
    ).drop_duplicates(subset=["gid"])
    a1_meta = a1_meta.sort_values(["admin1", "gid"], kind="mergesort").reset_index(drop=True)

    # Admin2 meta
    a2 = kbc[kbc["gid_admin2"].astype(str).str.strip() != ""].copy()
    a2_meta = pd.DataFrame(
        {
            "gid": a2["gid_admin2"].astype(str),
            "admin0": admin0_name,
            "admin1": a2.get("Admin1", "").astype(str),
            "admin2": a2.get("Admin2", "").astype(str),
            "gid_admin1": a2["gid_admin1"].astype(str),
            "gid_method": "",
            "alternative_gids": "",
            "gid_admin1_alternative_gids": "",
            "source": a2.get("admin2_source", "").astype(str),
            "featureidkey": a2.get("admin2_featureidkey", "").astype(str),
            "geojson_path": a2.get("admin2_geojson_path", "").astype(str),
            "geojson_url": a2.get("admin2_geojson_url", "").astype(str),
            "gid_in_geojson": "",
        }
    ).drop_duplicates(subset=["gid"])
    a2_meta = a2_meta.sort_values(["admin1", "admin2", "gid"], kind="mergesort").reset_index(drop=True)

    a2_to_a1: Dict[str, str] = {}
    if not a2_meta.empty:
        for g2, g1 in a2_meta[["gid", "gid_admin1"]].itertuples(index=False):
            g2 = normalize_gid(g2)
            g1 = normalize_gid(g1)
            if g2 and g1:
                a2_to_a1[g2] = g1

    return a1_meta, a2_meta, a2_to_a1



def augment_meta_with_observed_gids(
    meta: pd.DataFrame,
    *,
    observed_gids: Set[str],
    level: int,
    admin0_name: str,
    a2_to_a1_rollup: Optional[Dict[str, str]] = None,
    gid_to_admin1_name: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    If the Mongo precomputed-gid columns contain GIDs that are missing from the metadata
    (boundary_gid_index / KB), we append minimal rows so counts are not silently dropped.

    This is usually a no-op (metadata should cover everything), but it is a cheap safety net.
    """
    observed_gids = set([normalize_gid(g) for g in (observed_gids or set()) if normalize_gid(g)])
    if not observed_gids:
        return meta

    if meta is None or meta.empty or "gid" not in meta.columns:
        meta_cols: List[str] = []
        if level == 1:
            meta_cols = [
                "gid", "admin0", "admin1", "gid_method", "alternative_gids",
                "source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson",
            ]
        else:
            meta_cols = [
                "gid", "admin0", "admin1", "admin2", "gid_admin1", "gid_method",
                "alternative_gids", "gid_admin1_alternative_gids",
                "source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson",
            ]
        meta = pd.DataFrame(columns=meta_cols)

    known = set(meta["gid"].astype(str).map(normalize_gid))
    missing = sorted(observed_gids - known)
    if not missing:
        return meta

    # Build a template row with all columns present in meta (so we preserve any extra cols like gid_field/name_field)
    cols = list(meta.columns)
    def _blank_row() -> Dict[str, Any]:
        return {c: "" for c in cols}

    new_rows: List[Dict[str, Any]] = []
    for g in missing:
        r = _blank_row()
        r["gid"] = g
        if "admin0" in r:
            r["admin0"] = admin0_name
        if level == 1:
            # try to reuse an admin1 name if present
            if gid_to_admin1_name and "admin1" in r:
                r["admin1"] = gid_to_admin1_name.get(g, "")
        else:
            gid1 = ""
            if a2_to_a1_rollup:
                gid1 = normalize_gid(a2_to_a1_rollup.get(g, ""))
            if not gid1 and is_gadm_gid(g):
                gid1 = gadm_admin1_from_admin2_gid(g)
            if "gid_admin1" in r:
                r["gid_admin1"] = gid1
            if gid_to_admin1_name and "admin1" in r and gid1:
                r["admin1"] = gid_to_admin1_name.get(gid1, "")
        new_rows.append(r)

    meta2 = pd.concat([meta, pd.DataFrame(new_rows)], ignore_index=True)
    # Basic sorting for tidiness (make_counts_df also sorts, but keep denominators tidy too)
    if level == 1 and "admin1" in meta2.columns:
        meta2 = meta2.sort_values(["admin1", "gid"], kind="mergesort").reset_index(drop=True)
    elif level == 2:
        sort_cols = [c for c in ["admin1", "admin2", "gid"] if c in meta2.columns]
        if sort_cols:
            meta2 = meta2.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return meta2

# ------------------------ diagnostics -------------------------


@dataclass
class MonthDiagnostics:
    ym: str
    docs_scanned: int = 0
    docs_eligible: int = 0
    docs_with_any_gid: int = 0
    docs_with_admin1_gid: int = 0
    docs_with_admin2_gid: int = 0
    docs_with_any_label: int = 0
    gid_field_used_admin1: Counter = field(default_factory=Counter)
    gid_field_used_admin2: Counter = field(default_factory=Counter)
    gid_parse_issues: Counter = field(default_factory=Counter)


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
    """
    Builds a wide counts DataFrame by joining raw counts/denoms onto the metadata frame.
    Ensures stable, readable ordering by sorting on names.
    """
    if meta is None or meta.empty:
        # minimal fallback schema
        if level == 1:
            base_cols = ["gid", "admin0", "admin1", "gid_method", "alternative_gids"]
        else:
            base_cols = ["gid", "admin0", "admin1", "admin2", "gid_admin1", "gid_method", "alternative_gids", "gid_admin1_alternative_gids"]
        extra_cols = ["source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson"]
        cols = base_cols + extra_cols + [f"{e}_raw" for e in event_types] + [f"{e}_norm" for e in event_types]
        return pd.DataFrame(columns=cols)

    base = meta.copy()
    if "gid" not in base.columns:
        raise ValueError("meta is missing required column: gid")
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

    # Keep stable output column order
    if level == 1:
        base_cols = ["gid", "admin0", "admin1", "gid_method", "alternative_gids"]
    else:
        base_cols = ["gid", "admin0", "admin1", "admin2", "gid_admin1", "gid_method", "alternative_gids", "gid_admin1_alternative_gids"]
    extra_cols = ["source", "featureidkey", "geojson_path", "geojson_url", "gid_in_geojson"]

    # Optional helpful metadata columns if present
    for extra in ["gid_field", "name_field"]:
        if extra in base.columns and extra not in extra_cols:
            extra_cols.append(extra)

    out_cols = base_cols + extra_cols + [f"{e}_raw" for e in event_types] + [f"{e}_norm" for e in event_types]
    # Some sources may not have all metadata columns; fill missing with ""
    for c in out_cols:
        if c not in base.columns:
            base[c] = ""

    out = base[out_cols].copy()

    # Sort for readability
    if level == 1:
        if "admin1" in out.columns:
            out = out.sort_values(["admin1", "gid"], kind="mergesort").reset_index(drop=True)
        else:
            out = out.sort_values(["gid"], kind="mergesort").reset_index(drop=True)
    else:
        sort_cols = [c for c in ["admin1", "admin2", "gid"] if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
        else:
            out = out.sort_values(["gid"], kind="mergesort").reset_index(drop=True)

    return out


def write_country_denom(ym: str, denom: Dict[str, int], out_path: Path) -> None:
    row = {"ym": ym}
    for k, v in (denom or {}).items():
        row[k] = int(v)
    df = pd.DataFrame([row])
    ensure_dir(out_path.parent)
    df.to_csv(out_path, index=False)


def write_denoms(meta_df: pd.DataFrame, denom_map: Dict[str, int], *, out_path: Path, level: int) -> None:
    if meta_df is None or meta_df.empty:
        df = pd.DataFrame(columns=["gid", "denom"])
    else:
        df = meta_df.copy()
        df["gid"] = df["gid"].astype(str).map(normalize_gid)
        df["denom"] = df["gid"].map(lambda g: int(denom_map.get(g, 0))).astype(int)

        if level == 1:
            keep = [c for c in [
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
            ] if c in df.columns]
        else:
            keep = [c for c in [
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
            ] if c in df.columns]
        df = df[keep]

    ensure_dir(out_path.parent)
    df.to_csv(out_path, index=False)


def write_monthly_diag(diag: MonthDiagnostics, out_dir: Path, iso3: str) -> None:
    ensure_dir(out_dir)
    rows = [
        {"metric": "docs_scanned", "value": diag.docs_scanned},
        {"metric": "docs_eligible", "value": diag.docs_eligible},
        {"metric": "docs_with_any_gid", "value": diag.docs_with_any_gid},
        {"metric": "docs_with_admin1_gid", "value": diag.docs_with_admin1_gid},
        {"metric": "docs_with_admin2_gid", "value": diag.docs_with_admin2_gid},
        {"metric": "docs_with_any_label", "value": diag.docs_with_any_label},
    ]
    pd.DataFrame(rows).to_csv(out_dir / f"{iso3}_{diag.ym}_diag_summary.csv", index=False)

    if diag.gid_field_used_admin1:
        pd.DataFrame(
            [{"field": k, "n": int(v)} for k, v in diag.gid_field_used_admin1.most_common()]
        ).to_csv(out_dir / f"{iso3}_{diag.ym}_diag_gid_field_admin1.csv", index=False)
    if diag.gid_field_used_admin2:
        pd.DataFrame(
            [{"field": k, "n": int(v)} for k, v in diag.gid_field_used_admin2.most_common()]
        ).to_csv(out_dir / f"{iso3}_{diag.ym}_diag_gid_field_admin2.csv", index=False)
    if diag.gid_parse_issues:
        pd.DataFrame(
            [{"issue": k, "n": int(v)} for k, v in diag.gid_parse_issues.most_common()]
        ).to_csv(out_dir / f"{iso3}_{diag.ym}_diag_gid_parse_issues.csv", index=False)


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


# ------------------------ core month processing -------------------------


def process_month(
    db,
    *,
    iso3: str,
    year: int,
    month: int,
    event_types: List[str],
    a2_to_a1_rollup: Dict[str, str],
    local_like_domains: List[str],
    int_reg_domains: List[str],
    out_article_admin1: Path,
    out_article_admin2: Path,
    batch_size: int = 500,
) -> Tuple[Dict[Tuple[str, str], int], Dict[Tuple[str, str], int], Dict[str, int], Dict[str, int], MonthDiagnostics]:
    """
    Scan docs for one ISO3-month and compute:
      - raw_a1[(gid, label)] and raw_a2[(gid, label)]
      - denom_a1[gid], denom_a2[gid]
    """
    iso3 = str(iso3).strip().upper()
    ym = f"{year:04d}-{month:02d}"
    diag = MonthDiagnostics(ym=ym)

    raw_a1: Dict[Tuple[str, str], int] = defaultdict(int)
    raw_a2: Dict[Tuple[str, str], int] = defaultdict(int)
    denom_a1: Dict[str, int] = defaultdict(int)
    denom_a2: Dict[str, int] = defaultdict(int)

    colname = COLLECTION_TEMPLATE.format(year=year, month=month)
    evset = set([e.lower() for e in event_types])

    # Projection: base fields + all gid candidates
    proj_fields = list(dict.fromkeys(BASE_FIELDS + list(DOC_ADMIN1_GID_FIELDS) + list(DOC_ADMIN2_GID_FIELDS)))
    projection = {k: 1 for k in proj_fields}

    gid_fields_any = list(dict.fromkeys(list(DOC_ADMIN1_GID_FIELDS) + list(DOC_ADMIN2_GID_FIELDS)))
    gid_presence = gid_presence_query(gid_fields_any)

    def _yield_docs(domains: List[str], group_kind: str) -> Iterator[Dict[str, Any]]:
        if not domains:
            return
        for english in (False, True):
            q = build_denom_query(iso3, domains, group_kind=group_kind, english=english)
            # require at least one gid field present
            if gid_presence:
                q.setdefault("$and", []).append(gid_presence)
            cur = db[colname].find(q, projection=projection, batch_size=batch_size)
            for d in cur:
                yield d

    ensure_dir(out_article_admin1.parent)
    ensure_dir(out_article_admin2.parent)
    f1 = gzip.open(out_article_admin1, "wt", encoding="utf-8")
    f2 = gzip.open(out_article_admin2, "wt", encoding="utf-8")

    try:
        for d in _yield_docs(local_like_domains, "local_like"):
            diag.docs_scanned += 1
            _process_doc(
                d,
                iso3=iso3,
                ym=ym,
                colname=colname,
                event_types=evset,
                a2_to_a1_rollup=a2_to_a1_rollup,
                raw_a1=raw_a1,
                raw_a2=raw_a2,
                denom_a1=denom_a1,
                denom_a2=denom_a2,
                diag=diag,
                out_f_admin1=f1,
                out_f_admin2=f2,
            )
        for d in _yield_docs(int_reg_domains, "int_reg"):
            diag.docs_scanned += 1
            _process_doc(
                d,
                iso3=iso3,
                ym=ym,
                colname=colname,
                event_types=evset,
                a2_to_a1_rollup=a2_to_a1_rollup,
                raw_a1=raw_a1,
                raw_a2=raw_a2,
                denom_a1=denom_a1,
                denom_a2=denom_a2,
                diag=diag,
                out_f_admin1=f1,
                out_f_admin2=f2,
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
    a2_to_a1_rollup: Dict[str, str],
    raw_a1: Dict[Tuple[str, str], int],
    raw_a2: Dict[Tuple[str, str], int],
    denom_a1: Dict[str, int],
    denom_a2: Dict[str, int],
    diag: MonthDiagnostics,
    out_f_admin1,
    out_f_admin2,
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
    if labels:
        diag.docs_with_any_label += 1

    gids_a1, f_a1 = extract_first_nonempty_gid_field(d, iso3=iso3, candidate_fields=DOC_ADMIN1_GID_FIELDS)
    gids_a2, f_a2 = extract_first_nonempty_gid_field(d, iso3=iso3, candidate_fields=DOC_ADMIN2_GID_FIELDS)

    if f_a1:
        diag.gid_field_used_admin1[f_a1] += 1
    if f_a2:
        diag.gid_field_used_admin2[f_a2] += 1

    # Backward-compatible rollup: admin2 -> admin1
    if gids_a2:
        for g2 in list(gids_a2):
            g1 = a2_to_a1_rollup.get(g2, "")
            if not g1 and is_gadm_gid(g2):
                g1 = gadm_admin1_from_admin2_gid(g2)
            if g1:
                gids_a1.add(normalize_gid(g1))

    if not gids_a1 and not gids_a2:
        diag.gid_parse_issues["no_gids_after_parse"] += 1
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
        "gid_fields_used": {"admin1": f_a1, "admin2": f_a2},
    }

    if gids_a1:
        out_f_admin1.write(json.dumps({article_id: {**base_info, "gids": sorted(gids_a1), "concept_level": 1}}, ensure_ascii=False) + "\n")
    if gids_a2:
        out_f_admin2.write(json.dumps({article_id: {**base_info, "gids": sorted(gids_a2), "concept_level": 2}}, ensure_ascii=False) + "\n")


# ------------------------ runner -------------------------


def run_country(
    db,
    *,
    iso3: str,
    boundary_idx: Optional[pd.DataFrame],
    kb_fallback: Optional[pd.DataFrame],
    event_types: List[str],
) -> None:
    iso3 = iso3.upper()
    paths = get_run_paths(OUT_BASE, iso3, RUN_DATE)

    # sources
    src = build_source_lists(db, iso3)
    local_like_domains = src.local + src.env_local
    int_reg_domains = src.int_reg
    print(f"[sources] {iso3}: local={len(src.local)} env_local={len(src.env_local)} int_reg={len(src.int_reg)}")

    # metadata
    admin1_meta: pd.DataFrame
    admin2_meta: pd.DataFrame
    a2_to_a1: Dict[str, str]
    meta_mode = "none"

    if boundary_idx is not None and not boundary_idx.empty:
        admin1_meta, admin2_meta, a2_to_a1 = build_meta_from_boundary_index(boundary_idx, iso3=iso3)
        meta_mode = "boundary_gid_index"
    else:
        admin1_meta, admin2_meta, a2_to_a1 = pd.DataFrame(), pd.DataFrame(), {}

    if (admin1_meta is None or admin1_meta.empty) and kb_fallback is not None and not kb_fallback.empty:
        a1b, a2b, roll = build_meta_from_kb(kb_fallback, iso3=iso3)
        if not a1b.empty:
            admin1_meta, admin2_meta, a2_to_a1 = a1b, a2b, roll
            meta_mode = "kb_fallback"

    if admin1_meta is None or admin1_meta.empty:
        raise RuntimeError(
            f"No ADMIN1 metadata for {iso3}. Provide boundary_gid_index.parquet or a KB spreadsheet."
        )

    print(
        f"[meta] {iso3}: mode={meta_mode} ADMIN1 rows={len(admin1_meta):,} ADMIN2 rows={len(admin2_meta):,}"
    )

    # save run config
    run_config = {
        "iso3": iso3,
        "run_date": RUN_DATE,
        "start_ym": START_YM,
        "end_ym": END_YM,
        "mongo_db": MONGO_DB,
        "gid_fields": {
            "admin1_candidates": DOC_ADMIN1_GID_FIELDS,
            "admin2_candidates": DOC_ADMIN2_GID_FIELDS,
        },
        "metadata": {
            "mode": meta_mode,
            "boundary_gid_index_parquet": str(BOUNDARY_GID_INDEX_PARQUET) if BOUNDARY_GID_INDEX_PARQUET else "",
            "kb_path": str(KB_PATH) if KB_PATH else "",
        },
        "sources": {"local_n": len(src.local), "env_local_n": len(src.env_local), "int_reg_n": len(src.int_reg)},
        "event_types": event_types,
        "norm_mode": NORM_MODE,
        "created_at": _dt.datetime.now().isoformat(),
    }
    (paths.other_dir / "run_config.json").write_text(json.dumps(run_config, indent=2), encoding="utf-8")

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
            a2_to_a1_rollup=a2_to_a1,
            local_like_domains=local_like_domains,
            int_reg_domains=int_reg_domains,
            out_article_admin1=out_rec_a1,
            out_article_admin2=out_rec_a2,
            batch_size=BATCH_SIZE,
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


        # Ensure metadata covers any observed gids (cheap safety net; usually a no-op)
        admin0_name = (
            str(admin1_meta["admin0"].iloc[0])
            if (admin1_meta is not None and not admin1_meta.empty and "admin0" in admin1_meta.columns)
            else country_name_from_iso3(iso3)
        )
        gid_to_admin1_name: Dict[str, str] = {}
        if admin1_meta is not None and ("gid" in admin1_meta.columns) and ("admin1" in admin1_meta.columns):
            gid_to_admin1_name = dict(
                zip(
                    admin1_meta["gid"].astype(str).map(normalize_gid),
                    admin1_meta["admin1"].astype(str),
                )
            )
        obs_a1 = set(denom_a1.keys()) | {g for (g, _lab) in raw_a1.keys()}
        obs_a2 = set(denom_a2.keys()) | {g for (g, _lab) in raw_a2.keys()}
        admin1_meta_m = augment_meta_with_observed_gids(
            admin1_meta,
            observed_gids=obs_a1,
            level=1,
            admin0_name=admin0_name,
            gid_to_admin1_name=gid_to_admin1_name,
        )
        admin2_meta_m = augment_meta_with_observed_gids(
            admin2_meta,
            observed_gids=obs_a2,
            level=2,
            admin0_name=admin0_name,
            a2_to_a1_rollup=a2_to_a1,
            gid_to_admin1_name=gid_to_admin1_name,
        )
        # wide dfs
        a1_df = make_counts_df(
            admin1_meta_m,
            raw_a1,
            denom_a1,
            event_types,
            denom_country_total=denom_country_total,
            norm_mode=NORM_MODE,
            level=1,
        )
        a2_df = make_counts_df(
            admin2_meta_m,
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

        # denominators + monthly diagnostics into Other tree
        write_denoms(admin1_meta_m, denom_a1, out_path=paths.denom_a1_dir / f"{iso3}_{ym}_admin1_denom.csv", level=1)
        write_denoms(admin2_meta_m, denom_a2, out_path=paths.denom_a2_dir / f"{iso3}_{ym}_admin2_denom.csv", level=2)
        write_monthly_diag(diag, paths.diag_monthly_dir, iso3)

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
                "docs_with_any_label": diag.docs_with_any_label,
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
    if not DOC_ADMIN1_GID_FIELDS and not DOC_ADMIN2_GID_FIELDS:
        raise RuntimeError(
            "No candidate gid fields configured. Set ML4P_DOC_GIDS_ADMIN1_FIELD / ML4P_DOC_GIDS_ADMIN2_FIELD "
            "or edit DOC_ADMIN1_GID_FIELDS / DOC_ADMIN2_GID_FIELDS in CONFIG."
        )

    # Connect Mongo
    try:
        from pymongo import MongoClient  # type: ignore
    except Exception as e:
        raise RuntimeError("pymongo is required. Install it in your environment.") from e

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Load boundary index once (recommended)
    boundary_idx: Optional[pd.DataFrame] = None
    if BOUNDARY_GID_INDEX_PARQUET is not None and BOUNDARY_GID_INDEX_PARQUET.exists():
        print(f"[boundary_gid_index] loading from {BOUNDARY_GID_INDEX_PARQUET} ...")
        try:
            boundary_idx = load_boundary_gid_index(BOUNDARY_GID_INDEX_PARQUET)
            print(f"[boundary_gid_index] loaded rows={len(boundary_idx):,} gids={boundary_idx['kb_gid'].nunique():,}")
        except Exception as e:
            eprint(f"[boundary_gid_index] failed to load: {e}")
            boundary_idx = None
    else:
        print("[boundary_gid_index] not provided (or not found). Will use KB fallback if configured.")

    # Optional KB fallback
    kb_fallback: Optional[pd.DataFrame] = None
    if KB_PATH is not None:
        print(f"[kb] loading fallback KB from {KB_PATH} ...")
        kb_fallback = load_kb(KB_PATH, sheet=KB_SHEET)

    # Run each country
    event_types = ENV_EVENT_TYPES
    print(f"[event_types] {len(event_types)} labels; norm_mode={NORM_MODE}; run_date={RUN_DATE}")
    print(f"[out] base={OUT_BASE}")
    print(f"[gid_fields] admin1_candidates={DOC_ADMIN1_GID_FIELDS}")
    print(f"[gid_fields] admin2_candidates={DOC_ADMIN2_GID_FIELDS}")

    for iso3 in COUNTRIES:
        iso3 = str(iso3).strip().upper()
        if not iso3:
            continue
        print("\n" + "=" * 70)
        print(f"[run] ISO3={iso3}")
        print("=" * 70)
        try:
            run_country(db, iso3=iso3, boundary_idx=boundary_idx, kb_fallback=kb_fallback, event_types=event_types)
        except Exception as e:
            eprint(f"[ERROR] {iso3}: {e}")
            # keep going for other countries
            continue

    print("\n[done] All requested countries processed.")


if __name__ == "__main__":
    main()