#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

# Use the same excluded labels policy as dashboard data-build.
EXCLUDED_EVENT_LABELS = {
    "",
    "none",
    "null",
    "nan",
    "-999",
    "-1",
    "n/a",
    "environmental opinion",
    "environmental -999",
    "undefined",
}


def normalize_gid(v: object) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"^GBOPEN:", "GB.", s, flags=re.IGNORECASE)
    s = re.sub(r"^GBHUMANITARIAN:", "GB.", s, flags=re.IGNORECASE)
    return s




def _sql_str(s: str) -> str:
    return s.replace("'", "''")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def iter_months(start_ym: str, end_ym: str) -> Iterable[Tuple[int, int]]:
    ys, ms = [int(x) for x in start_ym.split("-")]
    ye, me = [int(x) for x in end_ym.split("-")]
    y, m = ys, ms
    while (y < ye) or (y == ye and m <= me):
        yield y, m
        m += 1
        if m > 12:
            y += 1
            m = 1


def parse_available_months(dataset_dir: Path, level: str) -> List[str]:
    patt = dataset_dir / "events_by_gid" / "year=*" / "month=*" / f"level={level}" / "*.parquet"
    months = set()
    for fp in glob.glob(str(patt)):
        m = re.search(r"year=(\d{4})/month=(\d{2})/level=ADM[12]/", fp)
        if m:
            months.add(f"{m.group(1)}-{m.group(2)}")
    return sorted(months)


def _read_alias_df(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path)


def _read_gid_set(path: Path) -> set:
    if not path.exists():
        return set()
    try:
        df = pd.read_parquet(path, columns=["gid"])
    except Exception:
        return set()
    if df is None or len(df) == 0 or "gid" not in df.columns:
        return set()
    return {normalize_gid(x) for x in df["gid"].astype(str).tolist() if normalize_gid(x)}


def _geometry_gid_set(dataset_dir: Path, level_num: int) -> set:
    out: set = set()
    base = dataset_dir / "geometries"
    candidates = [
        base / f"adm{level_num}_geometries_map.geoparquet",
        base / f"adm{level_num}_geometries.geoparquet",
        base / f"adm{level_num}_geometries.parquet",
    ]
    for p in candidates:
        s = _read_gid_set(p)
        if s:
            out |= s
            break

    patch_dirs = [
        dataset_dir / "geometries",
        dataset_dir / "geometries_patches",
        dataset_dir / "geometry_patches",
        dataset_dir / "patches" / "geometries",
    ]
    patch_patterns = [
        f"adm{level_num}_geometries_patch*.geoparquet",
        f"adm{level_num}_geometries_addon*.geoparquet",
        f"adm{level_num}_geometries_extra*.geoparquet",
        f"adm{level_num}_geometries_suppl*.geoparquet",
    ]
    for d in patch_dirs:
        if not d.exists():
            continue
        for patt in patch_patterns:
            for p in sorted(d.glob(patt)):
                out |= _read_gid_set(p)
    return out


def load_alias_map(dataset_dir: Path, level_num: int) -> Dict[str, str]:
    """Mirror dashboard alias behavior (geometry-aware) from app/utils/data_access.py."""
    base_names = [
        f"adm{level_num}_gid_alias_map.parquet",
        f"adm{level_num}_gid_alias_map.csv",
    ]
    patch_patterns = [
        f"adm{level_num}_gid_alias_patch*.parquet",
        f"adm{level_num}_gid_alias_map_patch*.parquet",
        f"adm{level_num}_gid_alias_patch*.csv",
        f"adm{level_num}_gid_alias_map_patch*.csv",
    ]
    search_dirs = [
        dataset_dir / "geometries",
        dataset_dir / "qc",
        dataset_dir / "geometries_patches",
        dataset_dir / "patches",
        dataset_dir / "patches" / "geometries",
    ]

    files: List[Path] = []
    for d in search_dirs:
        if not d.exists():
            continue
        for bn in base_names:
            p = d / bn
            if p.exists():
                files.append(p)
        for patt in patch_patterns:
            files.extend(sorted(d.glob(patt)))

    # Deduplicate
    uniq: List[Path] = []
    seen = set()
    for p in files:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)

    if not uniq:
        return {}

    geom_gids = _geometry_gid_set(dataset_dir, level_num)
    alias: Dict[str, str] = {}
    for p in uniq:
        try:
            df = _read_alias_df(p)
        except Exception:
            continue
        if df is None or len(df) == 0:
            continue

        if "gid_norm" in df.columns:
            gids = df["gid_norm"].astype(str)
        elif "gid" in df.columns:
            gids = df["gid"].astype(str).map(normalize_gid)
        else:
            continue

        if "canonical_gid_norm" in df.columns:
            cids = df["canonical_gid_norm"].astype(str)
        elif "canonical_gid" in df.columns:
            cids = df["canonical_gid"].astype(str).map(normalize_gid)
        else:
            continue

        for g, c in zip(gids.tolist(), cids.tolist()):
            gg = normalize_gid(g)
            cc = normalize_gid(c)
            if not gg or not cc:
                continue
            # Safety: keep geometry-present source gids stable.
            if geom_gids and gg in geom_gids:
                continue
            old = alias.get(gg, "")
            # If an existing mapping points to geometry, don't override it with non-geometry target.
            if geom_gids and old and old in geom_gids and cc not in geom_gids:
                continue
            alias[gg] = cc

    # Resolve short chains to terminal canonical gid
    out: Dict[str, str] = {}
    for g in list(alias.keys()):
        seen_chain = {g}
        chain: List[str] = []
        cur = g
        for _ in range(16):
            nxt = normalize_gid(alias.get(cur, ""))
            if not nxt or nxt == cur or nxt in seen_chain:
                break
            seen_chain.add(nxt)
            chain.append(nxt)
            cur = nxt
        target = cur if cur else g
        if geom_gids:
            if target in geom_gids:
                pass
            elif g in geom_gids:
                continue
            else:
                geom_hit = ""
                for node in chain:
                    if node in geom_gids:
                        geom_hit = node
                        break
                if geom_hit:
                    target = geom_hit
        if target and target != g:
            # never remap geometry-present source gid
            if geom_gids and g in geom_gids:
                continue
            out[g] = target
    return out


def load_lookup_meta(dataset_dir: Path, level_num: int, alias_map: Dict[str, str]) -> pd.DataFrame:
    """Build canonical metadata rows for export."""
    p_lookup = dataset_dir / "gid_lookup" / f"adm{level_num}.parquet"
    p_meta = dataset_dir / "gid_meta" / f"adm{level_num}.parquet"

    if p_lookup.exists():
        df = pd.read_parquet(p_lookup)
    elif p_meta.exists():
        df = pd.read_parquet(p_meta)
        if "display_name" not in df.columns and "name" in df.columns:
            df["display_name"] = df["name"].astype(str)
    else:
        # Return empty shell; script still runs and records missing rows.
        cols = ["gid", "iso3", "display_name", "sysid"]
        return pd.DataFrame({c: pd.Series(dtype="string") for c in cols})

    if "gid" not in df.columns:
        raise RuntimeError(f"gid column missing in {p_lookup if p_lookup.exists() else p_meta}")

    d = df.copy()
    d["gid"] = d["gid"].astype(str).map(normalize_gid)
    if "iso3" not in d.columns:
        d["iso3"] = ""
    d["iso3"] = d["iso3"].astype(str).str.upper().str.strip()

    # Canonicalize gids using alias map (same as dashboard query path).
    d["canonical_gid"] = d["gid"].map(lambda g: alias_map.get(g, g))

    # Candidate name + score.
    if "display_name" in d.columns:
        d["name_candidate"] = d["display_name"].astype(str)
    elif "boundary_name" in d.columns:
        d["name_candidate"] = d["boundary_name"].astype(str)
    elif "name" in d.columns:
        d["name_candidate"] = d["name"].astype(str)
    else:
        d["name_candidate"] = d["canonical_gid"].astype(str)

    if "display_name_score" in d.columns:
        d["name_score"] = pd.to_numeric(d["display_name_score"], errors="coerce").fillna(0)
    else:
        d["name_score"] = 0

    # Keep best row per canonical gid.
    d = d.sort_values(["canonical_gid", "name_score", "name_candidate"], ascending=[True, False, True], kind="mergesort")
    best = d.drop_duplicates(subset=["canonical_gid"], keep="first").copy()

    # Build alias gids per canonical from lookup rows + alias map.
    aliases_by_canon: Dict[str, set] = defaultdict(set)
    for r in d[["gid", "canonical_gid"]].itertuples(index=False):
        if r.gid and r.gid != r.canonical_gid:
            aliases_by_canon[str(r.canonical_gid)].add(str(r.gid))
    for g, c in alias_map.items():
        if g and c and g != c:
            aliases_by_canon[c].add(g)

    best["alias_gids"] = best["canonical_gid"].map(lambda c: "|".join(sorted(aliases_by_canon.get(str(c), set()))))
    best["n_alias_gids"] = best["canonical_gid"].map(lambda c: len(aliases_by_canon.get(str(c), set()))).astype(int)

    keep_cols = ["canonical_gid", "iso3", "name_candidate", "sysid", "alias_gids", "n_alias_gids"]
    if "geojson_relpath" in best.columns:
        keep_cols.append("geojson_relpath")
    if "featureidkey" in best.columns:
        keep_cols.append("featureidkey")

    out = best[keep_cols].copy().rename(columns={"name_candidate": "name"})
    out = out.drop_duplicates(subset=["canonical_gid"], keep="first").reset_index(drop=True)
    return out


def load_country_names(kb_path: Optional[Path]) -> Dict[str, str]:
    if kb_path is None or not kb_path.exists():
        return {}
    try:
        if kb_path.suffix.lower() in {".xlsx", ".xls"}:
            kb = pd.read_excel(kb_path)
        else:
            kb = pd.read_csv(kb_path)
    except Exception:
        return {}
    if "Admin0_ISO3" not in kb.columns:
        return {}
    c_name_col = None
    for c in ["Admin0", "admin0", "country", "Country"]:
        if c in kb.columns:
            c_name_col = c
            break
    if c_name_col is None:
        return {}
    t = kb[["Admin0_ISO3", c_name_col]].copy()
    t["Admin0_ISO3"] = t["Admin0_ISO3"].astype(str).str.upper().str.strip()
    t[c_name_col] = t[c_name_col].astype(str).str.strip()
    t = t[t["Admin0_ISO3"].str.len() == 3]
    t = t[t[c_name_col] != ""]
    t = t.drop_duplicates(subset=["Admin0_ISO3"], keep="first")
    return dict(zip(t["Admin0_ISO3"], t[c_name_col]))


def load_month_events(dataset_dir: Path, level: str, year: int, month: int) -> pd.DataFrame:
    patt = dataset_dir / "events_by_gid" / f"year={year:04d}" / f"month={month:02d}" / f"level={level}" / "*.parquet"
    files = sorted(glob.glob(str(patt)))
    if not files:
        return pd.DataFrame(columns=["gid", "iso3", "env_label", "count"])
    parts: List[pd.DataFrame] = []
    for f in files:
        try:
            p = pd.read_parquet(f, columns=["gid", "iso3", "env_label", "count"])
        except Exception:
            continue
        if p is not None and len(p):
            parts.append(p)
    if not parts:
        return pd.DataFrame(columns=["gid", "iso3", "env_label", "count"])
    df = pd.concat(parts, ignore_index=True)
    df["gid"] = df["gid"].astype(str)
    df["iso3"] = df["iso3"].astype(str).str.upper().str.strip()
    df["env_label"] = df["env_label"].astype(str).str.lower().str.strip()
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
    df = df.groupby(["gid", "iso3", "env_label"], as_index=False)["count"].sum()
    df = df[df["env_label"].notna()].copy()
    df["env_label"] = df["env_label"].astype(str)
    df = df[~df["env_label"].isin(EXCLUDED_EVENT_LABELS)].copy()
    return df


def apply_alias(df: pd.DataFrame, alias_map: Dict[str, str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (aliased_df, unresolved_df)."""
    if df is None or len(df) == 0:
        empty = pd.DataFrame(columns=["iso3", "gid", "env_label", "count"])
        return empty, empty

    d = df.copy()
    d["gid"] = d["gid"].astype(str).map(normalize_gid)
    d["canonical_gid"] = d["gid"].map(lambda g: alias_map.get(g, g))

    # Track rows where gid changed.
    aliased_rows = d[d["gid"] != d["canonical_gid"]][["iso3", "gid", "canonical_gid", "env_label", "count"]].copy()

    out = (
        d.groupby(["iso3", "canonical_gid", "env_label"], as_index=False)["count"]
        .sum()
        .rename(columns={"canonical_gid": "gid"})
    )
    return out, aliased_rows


def to_wide_counts(
    month_df: pd.DataFrame,
    *,
    meta: pd.DataFrame,
    iso3: str,
    event_labels: Sequence[str],
    country_name: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    d = month_df[month_df["iso3"] == iso3].copy()
    if d.empty:
        # still emit all canonical rows for country with zeros (dashboard-compatible shape)
        m = meta[meta["iso3"] == iso3].copy()
        if m.empty:
            base = pd.DataFrame(columns=["gid", "iso3", "admin0", "name", "sysid", "alias_gids", "n_alias_gids"]) 
        else:
            base = m.rename(columns={"canonical_gid": "gid"}).copy()
            base["admin0"] = country_name
            base = base[["gid", "iso3", "admin0", "name", "sysid", "alias_gids", "n_alias_gids"] + [c for c in ["geojson_relpath", "featureidkey"] if c in base.columns]]
        for ev in event_labels:
            base[f"{ev}_raw"] = 0
        base["total_events_raw"] = 0
        return base, pd.DataFrame(columns=["gid", "iso3", "admin0", "name"])

    pv = d.pivot_table(index="gid", columns="env_label", values="count", aggfunc="sum", fill_value=0)
    pv.columns = [str(c) for c in pv.columns]
    pv = pv.reset_index()

    # Ensure full event schema.
    for ev in event_labels:
        if ev not in pv.columns:
            pv[ev] = 0

    # Keep only expected labels + gid.
    pv = pv[["gid"] + list(event_labels)].copy()

    m = meta[meta["iso3"] == iso3].copy()
    m = m.rename(columns={"canonical_gid": "gid"})

    out = m.merge(pv, on="gid", how="left")
    for ev in event_labels:
        out[ev] = pd.to_numeric(out[ev], errors="coerce").fillna(0).astype(int)

    out["iso3"] = iso3
    out["admin0"] = country_name
    out["name"] = out["name"].fillna(out["gid"]).astype(str)
    out["total_events_raw"] = out[list(event_labels)].sum(axis=1).astype(int)

    # Rename event cols to *_raw
    ren = {ev: f"{ev}_raw" for ev in event_labels}
    out = out.rename(columns=ren)

    # Missing diagnostics + additive retention:
    # If a canonical gid has counts but is absent from metadata/geometry lookup,
    # keep it in output (name fallback=gid) so no counts are dropped.
    missing = pv[~pv["gid"].isin(set(m["gid"].astype(str)))].copy()
    missing_rows = pd.DataFrame()
    if len(missing):
        missing_rows = missing.rename(columns=ren).copy()
        missing_rows["iso3"] = iso3
        missing_rows["admin0"] = country_name
        missing_rows["name"] = missing_rows["gid"].astype(str)
        missing_rows["sysid"] = ""
        missing_rows["alias_gids"] = ""
        missing_rows["n_alias_gids"] = 0
        missing_rows["total_events_raw"] = missing_rows[[f"{ev}_raw" for ev in event_labels]].sum(axis=1).astype(int)
        if "geojson_relpath" in out.columns:
            missing_rows["geojson_relpath"] = ""
        if "featureidkey" in out.columns:
            missing_rows["featureidkey"] = ""

    keep = [
        "gid",
        "iso3",
        "admin0",
        "name",
        "sysid",
        "alias_gids",
        "n_alias_gids",
    ]
    keep += [c for c in ["geojson_relpath", "featureidkey"] if c in out.columns]
    keep += [f"{ev}_raw" for ev in event_labels]
    keep += ["total_events_raw"]

    out = out[keep]
    if len(missing_rows):
        out = pd.concat([out, missing_rows[keep]], ignore_index=True)
    out = out.sort_values("gid", kind="mergesort").reset_index(drop=True)

    missing_diag = missing_rows.copy()
    return out, missing_diag


def detect_event_labels(dataset_dir: Path, start_ym: str, end_ym: str) -> List[str]:
    labels = set()
    for y, m in iter_months(start_ym, end_ym):
        for lvl in ("ADM1", "ADM2"):
            p = dataset_dir / "events_by_gid" / f"year={y:04d}" / f"month={m:02d}" / f"level={lvl}"
            if not p.exists():
                continue
            files = sorted(glob.glob(str(p / "*.parquet")))
            if not files:
                continue
            for f in files:
                try:
                    df = pd.read_parquet(f, columns=["env_label"])
                except Exception:
                    continue
                if df is None or len(df) == 0:
                    continue
                vals = df["env_label"].dropna().astype(str).str.lower().str.strip().tolist()
                for v in vals:
                    if v and v not in EXCLUDED_EVENT_LABELS:
                        labels.add(v)
    return sorted(labels)


def write_month_outputs(
    *,
    dataset_dir: Path,
    out_base: Path,
    run_date: str,
    countries: List[str],
    start_ym: str,
    end_ym: str,
    kb_path: Optional[Path],
) -> None:
    alias1 = load_alias_map(dataset_dir, 1)
    alias2 = load_alias_map(dataset_dir, 2)

    meta1 = load_lookup_meta(dataset_dir, 1, alias1)
    meta2 = load_lookup_meta(dataset_dir, 2, alias2)

    country_name_map = load_country_names(kb_path)

    event_labels = detect_event_labels(dataset_dir, start_ym, end_ym)
    if not event_labels:
        raise RuntimeError("No event labels found in selected dataset/range.")

    print(f"[event_labels] n={len(event_labels)}")
    print("  " + ", ".join(event_labels))

    # Prepare per-country dirs.
    for iso3 in countries:
        iso = iso3.upper()
        ensure_dir(out_base / "Admin1" / iso / run_date)
        ensure_dir(out_base / "Admin2" / iso / run_date)
        ensure_dir(out_base / "Other" / iso / run_date / "diagnostics" / "monthly")

    summary_rows = []

    for y, m in iter_months(start_ym, end_ym):
        ym = f"{y:04d}-{m:02d}"
        print(f"[month] {ym}")

        m1_raw = load_month_events(dataset_dir, "ADM1", y, m)
        m2_raw = load_month_events(dataset_dir, "ADM2", y, m)

        m1, m1_alias_rows = apply_alias(m1_raw, alias1)
        m2, m2_alias_rows = apply_alias(m2_raw, alias2)

        for iso3 in countries:
            iso = iso3.upper()
            cname = country_name_map.get(iso, "")

            out1, miss1 = to_wide_counts(m1, meta=meta1, iso3=iso, event_labels=event_labels, country_name=cname)
            out2, miss2 = to_wide_counts(m2, meta=meta2, iso3=iso, event_labels=event_labels, country_name=cname)

            fp1 = out_base / "Admin1" / iso / run_date / f"counts_{iso}_admin1_{ym}.csv"
            fp2 = out_base / "Admin2" / iso / run_date / f"counts_{iso}_admin2_{ym}.csv"
            out1.to_csv(fp1, index=False)
            out2.to_csv(fp2, index=False)

            diag_dir = out_base / "Other" / iso / run_date / "diagnostics" / "monthly"

            miss1_path = diag_dir / f"{iso}_{ym}_adm1_missing_geometries_after_alias.csv"
            miss2_path = diag_dir / f"{iso}_{ym}_adm2_missing_geometries_after_alias.csv"
            miss1.to_csv(miss1_path, index=False)
            miss2.to_csv(miss2_path, index=False)

            # Track alias applications for transparency.
            a1i = m1_alias_rows[m1_alias_rows["iso3"] == iso].copy()
            a2i = m2_alias_rows[m2_alias_rows["iso3"] == iso].copy()
            a1i.to_csv(diag_dir / f"{iso}_{ym}_adm1_alias_applied_rows.csv", index=False)
            a2i.to_csv(diag_dir / f"{iso}_{ym}_adm2_alias_applied_rows.csv", index=False)

            summary_rows.append(
                {
                    "iso3": iso,
                    "ym": ym,
                    "admin1_nonzero_gids": int((out1["total_events_raw"] > 0).sum()) if "total_events_raw" in out1.columns else 0,
                    "admin2_nonzero_gids": int((out2["total_events_raw"] > 0).sum()) if "total_events_raw" in out2.columns else 0,
                    "admin1_total_events": int(out1["total_events_raw"].sum()) if "total_events_raw" in out1.columns else 0,
                    "admin2_total_events": int(out2["total_events_raw"].sum()) if "total_events_raw" in out2.columns else 0,
                    "admin1_missing_gids_after_alias": int(len(miss1)),
                    "admin2_missing_gids_after_alias": int(len(miss2)),
                    "admin1_alias_rows": int(len(a1i)),
                    "admin2_alias_rows": int(len(a2i)),
                }
            )

    if summary_rows:
        sdf = pd.DataFrame(summary_rows)
        for iso3 in countries:
            iso = iso3.upper()
            p = out_base / "Other" / iso / run_date / "diagnostics" / f"{iso}_summary_{start_ym}_to_{end_ym}.csv"
            ensure_dir(p.parent)
            sdf[sdf["iso3"] == iso].to_csv(p, index=False)

    run_info = {
        "run_date": run_date,
        "dataset_dir": str(dataset_dir),
        "countries": [c.upper() for c in countries],
        "start_ym": start_ym,
        "end_ym": end_ym,
        "out_base": str(out_base),
        "event_labels": event_labels,
    }
    for iso3 in countries:
        iso = iso3.upper()
        p = out_base / "Other" / iso / run_date / "run_config_dashboard_counts.json"
        p.write_text(json.dumps(run_info, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Export dashboard-equivalent monthly Admin1/Admin2 counts to Dropbox country folders."
    )
    ap.add_argument(
        "--dataset-dir",
        default="",
        help="Path to dashboard dataset (contains events_by_gid, gid_lookup, geometries). "
        "If omitted, set --hf-repo to download from HuggingFace.",
    )
    ap.add_argument("--hf-repo", default="", help="Optional HF dataset repo id (e.g., zungru/geoparsing-env-monthly)")
    ap.add_argument("--hf-revision", default="main", help="HF dataset revision (default: main)")
    ap.add_argument("--hf-cache-dir", default="", help="Optional cache dir for HF snapshot_download")
    ap.add_argument(
        "--out-base",
        default="/home/ml4p/Dropbox/Dropbox/ML for Peace/Counts_Subnational",
        help="Base output directory (Dropbox Counts_Subnational)",
    )
    ap.add_argument(
        "--countries",
        default="",
        help="Comma-separated ISO3 list. If blank, derive from dataset gid_lookup/adm1.parquet",
    )
    ap.add_argument(
        "--countries-csv",
        default="",
        help="Optional CSV/TXT path with ISO3 values (preferred for fixed country sets like the 66-country list).",
    )
    ap.add_argument(
        "--countries-col",
        default="",
        help="Optional ISO3 column name in --countries-csv. If omitted, auto-detects common names.",
    )
    ap.add_argument("--start-ym", default="", help="Start month YYYY-MM. Default=min available month")
    ap.add_argument("--end-ym", default="", help="End month YYYY-MM. Default=max available month")
    ap.add_argument("--run-date", default="", help="Run date folder name (default today YYYY_MM_DD)")
    ap.add_argument("--kb-path", default="", help="Optional KB path for country names (Admin0 column)")
    return ap.parse_args()


def main() -> None:
    args = parse_args()

    # Parse requested range early (used to limit HF snapshot patterns).
    start_ym_arg = args.start_ym.strip()
    end_ym_arg = args.end_ym.strip()
    if start_ym_arg and re.fullmatch(r"\d{4}-\d{2}", start_ym_arg) is None:
        raise ValueError("start-ym must be YYYY-MM")
    if end_ym_arg and re.fullmatch(r"\d{4}-\d{2}", end_ym_arg) is None:
        raise ValueError("end-ym must be YYYY-MM")

    dataset_dir_raw = args.dataset_dir.strip()
    dataset_dir = Path(dataset_dir_raw).expanduser().resolve() if dataset_dir_raw else Path("")
    hf_repo = args.hf_repo.strip()
    if (not dataset_dir_raw or not dataset_dir.exists()) and hf_repo:
        from huggingface_hub import snapshot_download

        allow_patterns = ["gid_lookup/**", "gid_meta/**", "geometries/**", "patches/**", "build_log.json"]
        if start_ym_arg and end_ym_arg:
            for y, m in iter_months(start_ym_arg, end_ym_arg):
                allow_patterns.append(f"events_by_gid/year={y:04d}/month={m:02d}/**")
        else:
            allow_patterns.append("events_by_gid/**")

        revision = args.hf_revision.strip() or "main"
        cache_dir = (args.hf_cache_dir.strip() or None)
        last_err: Optional[Exception] = None
        local = None
        for attempt in range(1, 4):
            try:
                local = snapshot_download(
                    repo_id=hf_repo,
                    repo_type="dataset",
                    revision=revision,
                    allow_patterns=allow_patterns,
                    cache_dir=cache_dir,
                    force_download=bool(attempt > 1),
                    resume_download=bool(attempt == 1),
                )
                break
            except Exception as e:
                last_err = e
                print(f"[hf] download attempt {attempt}/3 failed: {type(e).__name__}: {e}")
        if local is None:
            raise RuntimeError(f"HF snapshot download failed for {hf_repo}@{revision}") from last_err
        dataset_dir = Path(local).resolve()
        print(f"[hf] downloaded snapshot: {dataset_dir}")

    if (not dataset_dir_raw and not hf_repo) or (not dataset_dir.exists()):
        raise FileNotFoundError(f"dataset-dir not found: {dataset_dir}")

    out_base = Path(args.out_base).expanduser().resolve()
    ensure_dir(out_base)

    months = sorted(set(parse_available_months(dataset_dir, "ADM1")) | set(parse_available_months(dataset_dir, "ADM2")))
    if not months:
        raise RuntimeError(f"No monthly partitions found under {dataset_dir}/events_by_gid")

    start_ym = start_ym_arg or months[0]
    end_ym = end_ym_arg or months[-1]

    if re.fullmatch(r"\d{4}-\d{2}", start_ym) is None or re.fullmatch(r"\d{4}-\d{2}", end_ym) is None:
        raise ValueError("start-ym/end-ym must be YYYY-MM")

    run_date = args.run_date.strip() or date.today().strftime("%Y_%m_%d")

    def _countries_from_csv(path: Path, preferred_col: str = "") -> List[str]:
        if path.suffix.lower() in {".txt", ".list"}:
            vals = [ln.strip().upper() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
            return sorted({v for v in vals if len(v) == 3})
        df = pd.read_csv(path)
        if preferred_col and preferred_col in df.columns:
            col = preferred_col
        else:
            candidates = [
                "iso3",
                "ISO3",
                "Admin0_ISO3",
                "admin0_iso3",
                "country_iso3",
                "Country_ISO3",
            ]
            col = ""
            for c in candidates:
                if c in df.columns:
                    col = c
                    break
            if not col:
                # fallback: first column with >=3-char strings
                for c in df.columns:
                    s = df[c].dropna().astype(str).str.upper().str.strip()
                    if (s.str.len() == 3).mean() > 0.5:
                        col = c
                        break
            if not col:
                raise RuntimeError(f"Could not infer ISO3 column from {path}")
        vals = df[col].dropna().astype(str).str.upper().str.strip().tolist()
        return sorted({v for v in vals if len(v) == 3})

    countries_csv = args.countries_csv.strip()
    if countries_csv:
        cpath = Path(countries_csv).expanduser().resolve()
        if not cpath.exists():
            raise FileNotFoundError(f"countries-csv not found: {cpath}")
        countries = _countries_from_csv(cpath, args.countries_col.strip())
    elif args.countries.strip():
        countries = [x.strip().upper() for x in args.countries.split(",") if x.strip()]
    else:
        p = dataset_dir / "gid_lookup" / "adm1.parquet"
        if not p.exists():
            raise RuntimeError("countries not provided and gid_lookup/adm1.parquet missing")
        d = pd.read_parquet(p, columns=["iso3"])
        countries = sorted({str(x).upper().strip() for x in d["iso3"].dropna().tolist() if str(x).strip()})

    kb_path = Path(args.kb_path).expanduser().resolve() if args.kb_path.strip() else None
    if kb_path and (not kb_path.exists()):
        raise FileNotFoundError(f"kb-path not found: {kb_path}")

    print("[config]")
    print(f"  dataset_dir={dataset_dir}")
    print(f"  out_base={out_base}")
    print(f"  countries={len(countries)}")
    print(f"  range={start_ym}..{end_ym}")
    print(f"  run_date={run_date}")

    write_month_outputs(
        dataset_dir=dataset_dir,
        out_base=out_base,
        run_date=run_date,
        countries=countries,
        start_ym=start_ym,
        end_ym=end_ym,
        kb_path=kb_path,
    )

    print("[done] Dashboard-equivalent counts exported.")


if __name__ == "__main__":
    main()
