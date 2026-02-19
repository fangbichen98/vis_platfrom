#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import random
import argparse
from pathlib import Path
from typing import Dict, List, Tuple

from flask import Flask, jsonify, request, send_file, send_from_directory, render_template
try:
    from flask_compress import Compress  # type: ignore
except Exception:
    Compress = None  # optional
from werkzeug.utils import secure_filename
import threading

try:
    import duckdb  # type: ignore
except Exception as e:
    duckdb = None


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
APPDATA_DIR = ROOT / "appdata"
LABELS_DIR = ROOT / "labels"
TEMPLATES_DIR = ROOT / "templates"
STATIC_DIR = ROOT / "static"


YEARS = [2018, 2021, 2024]


def _ensure_dirs():
    APPDATA_DIR.mkdir(parents=True, exist_ok=True)
    LABELS_DIR.mkdir(parents=True, exist_ok=True)


def detect_cols(csv_path: Path) -> Dict[str, str]:
    """Detect column names in OD CSV and map to canonical names.
    Canonical: date_dt, time_, o_grid, d_grid, num_total
    """
    with csv_path.open("r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    cols = [c.strip() for c in header]

    def pick(cands: List[str], default: str) -> str:
        for c in cands:
            if c in cols:
                return c
        return default

    mapping = {
        "date_dt": pick(["date_dt", "date"], "date_dt"),
        "time_": pick(["time_", "time", "hour"], "time"),
        "o_grid": pick(["o_grid", "o_grid_500", "o"], "o_grid_500"),
        "d_grid": pick(["d_grid", "d_grid_500", "d"], "d_grid_500"),
        "num_total": pick(["num_total", "flow", "count"], "num_total"),
    }
    return mapping


def load_metadata(meta_csv: Path) -> Tuple[List[Dict], Dict[int, Dict]]:
    items: List[Dict] = []
    by_id: Dict[int, Dict] = {}
    with meta_csv.open("r", newline="") as f:
        reader = csv.DictReader(f)
        # Expected: grid_id, lon, lat; optional: area_name, city_name
        for row in reader:
            try:
                gid = int(row["grid_id"])  # type: ignore
                lon = float(row["lon"])  # type: ignore
                lat = float(row["lat"])  # type: ignore
            except Exception:
                continue
            item = {
                "grid_id": gid,
                "lon": lon,
                "lat": lat,
                "area_name": row.get("area_name", ""),
                "city_name": row.get("city_name", ""),
            }
            items.append(item)
            by_id[gid] = item
    return items, by_id


class DataEngine:
    def __init__(self, data_dir: Path, appdata_dir: Path, meta_path: Path, require_csv: bool = True):
        if duckdb is None:
            raise RuntimeError("duckdb is required. Please pip install duckdb")
        self.data_dir = data_dir
        self.appdata_dir = appdata_dir
        self.meta_items, self.meta_by_id = load_metadata(meta_path)
        self.year_paths = {y: data_dir / f"{y}.csv" for y in YEARS}
        self.year_colmaps: Dict[int, Dict[str, str]] = {}
        if require_csv:
            for y, p in self.year_paths.items():
                if not p.exists():
                    raise FileNotFoundError(f"Missing data file: {p}")
                self.year_colmaps[y] = detect_cols(p)
        else:
            # Parquet-only mode: CSVs are optional; allow building only if files provided later.
            for y, p in self.year_paths.items():
                if p.exists():
                    self.year_colmaps[y] = detect_cols(p)

    def _parquet_path(self, kind: str, year: int) -> Path:
        # kind in {edges_by_o, edges_by_d, hourly}
        return self.appdata_dir / f"{kind}_{year}.parquet"

    def ensure_built(self, years: List[int] = YEARS):
        for y in years:
            need_edges_o = not self._parquet_path("edges_by_o", y).exists()
            need_edges_d = not self._parquet_path("edges_by_d", y).exists()
            need_hourly = not self._parquet_path("hourly", y).exists()
            if need_edges_o or need_edges_d or need_hourly:
                self._build_year(y, build_edges_o=need_edges_o, build_edges_d=need_edges_d, build_hourly=need_hourly)

    def build_totals_for_year(self, y: int):
        """(Re)build totals_{y}.parquet from edges_by_o/d parquet (fast)."""
        p_out = self._parquet_path("edges_by_o", y)
        p_in = self._parquet_path("edges_by_d", y)
        if not p_out.exists() or not p_in.exists():
            # fall back to full build (will read CSV)
            self._build_year(y, build_edges_o=not p_out.exists(), build_edges_d=not p_in.exists(), build_hourly=False)
        totals_p = self._parquet_path("totals", y)
        with duckdb.connect() as con:
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW gout AS
                SELECT o_grid AS grid_id, SUM(num_total) AS out_total
                FROM read_parquet('{str(p_out)}') GROUP BY o_grid
                """
            )
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW gin AS
                SELECT d_grid AS grid_id, SUM(num_total) AS in_total
                FROM read_parquet('{str(p_in)}') GROUP BY d_grid
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW gt AS
                SELECT COALESCE(gout.grid_id, gin.grid_id) AS grid_id,
                       COALESCE(gout.out_total, 0) AS out_total,
                       COALESCE(gin.in_total, 0) AS in_total,
                       COALESCE(gout.out_total, 0) + COALESCE(gin.in_total, 0) AS total
                FROM gout FULL OUTER JOIN gin ON gout.grid_id = gin.grid_id
                """
            )
            try:
                totals_p.unlink(missing_ok=True)
            except Exception:
                pass
            con.execute(
                f"COPY (SELECT * FROM gt ORDER BY grid_id) TO '{str(totals_p)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )

    def _read_csv_sql(self, y: int) -> Tuple[str, Dict[str, str]]:
        p = str(self.year_paths[y])
        m = self.year_colmaps[y]
        # Map to canonical names in SQL SELECT
        sel = f"""
            SELECT 
              {m['date_dt']}::VARCHAR as date_dt,
              {m['time_']}::INTEGER as hour,
              {m['o_grid']}::BIGINT as o_grid,
              {m['d_grid']}::BIGINT as d_grid,
              {m['num_total']}::DOUBLE as num_total
            FROM read_csv_auto('{p}', header=true, sample_size=-1)
        """
        return sel, m

    def _build_year(self, y: int, build_edges_o: bool = True, build_edges_d: bool = True, build_hourly: bool = True):
        print(f"[build] year {y} starting ...", flush=True)
        con = duckdb.connect(str(self.appdata_dir / f"build_{y}.duckdb"))
        try:
            con.execute("PRAGMA threads=8;")
        except Exception:
            pass
        sel, _ = self._read_csv_sql(y)

        if build_edges_o or build_edges_d:
            print(f"[build] year {y}: aggregating edges ...", flush=True)
            con.execute(f"CREATE OR REPLACE TEMP VIEW src AS {sel}")
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW edges AS
                SELECT o_grid, d_grid, SUM(num_total) AS num_total
                FROM src
                GROUP BY o_grid, d_grid
                """
            )
            if build_edges_o:
                path_o_p = self._parquet_path("edges_by_o", y)
                # overwrite any existing file to avoid stale sample indexes
                try:
                    path_o_p.unlink(missing_ok=True)
                except Exception:
                    pass
                path_o = str(path_o_p)
                print(f"[build] year {y}: writing {path_o} ...", flush=True)
                con.execute(
                    f"COPY (SELECT * FROM edges ORDER BY o_grid) TO '{path_o}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )
            if build_edges_d:
                path_d_p = self._parquet_path("edges_by_d", y)
                try:
                    path_d_p.unlink(missing_ok=True)
                except Exception:
                    pass
                path_d = str(path_d_p)
                print(f"[build] year {y}: writing {path_d} ...", flush=True)
                con.execute(
                    f"COPY (SELECT * FROM edges ORDER BY d_grid) TO '{path_d}' (FORMAT PARQUET, COMPRESSION ZSTD)"
                )

        if build_hourly:
            print(f"[build] year {y}: aggregating hourly per grid/week ...", flush=True)
            con.execute(f"CREATE OR REPLACE TEMP VIEW src AS {sel}")
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW t AS
                SELECT 
                  TRY_STRPTIME(CASE WHEN LENGTH(date_dt)=8 THEN date_dt ELSE NULL END, '%Y%m%d')::DATE AS dt,
                  hour,
                  o_grid,
                  d_grid,
                  num_total
                FROM src
                WHERE dt IS NOT NULL
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW out_by AS
                SELECT o_grid AS grid_id, dt, hour, SUM(num_total) AS out_total
                FROM t
                GROUP BY grid_id, dt, hour
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW in_by AS
                SELECT d_grid AS grid_id, dt, hour, SUM(num_total) AS in_total
                FROM t
                GROUP BY grid_id, dt, hour
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW merged AS
                SELECT 
                  COALESCE(o.grid_id, i.grid_id) AS grid_id,
                  COALESCE(o.dt, i.dt) AS dt,
                  COALESCE(o.hour, i.hour) AS hour,
                  COALESCE(o.out_total, 0) AS out_total,
                  COALESCE(i.in_total, 0) AS in_total
                FROM out_by o
                FULL OUTER JOIN in_by i
                  ON o.grid_id = i.grid_id AND o.dt = i.dt AND o.hour = i.hour
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW hourly AS
                SELECT 
                  grid_id,
                  DATE_PART('week', dt)::INTEGER AS week,
                  hour::INTEGER AS hour,
                  SUM(out_total) AS out_total,
                  SUM(in_total) AS in_total,
                  SUM(out_total + in_total) AS total
                FROM merged
                GROUP BY grid_id, week, hour
                """
            )
            path_h_p = self._parquet_path("hourly", y)
            try:
                path_h_p.unlink(missing_ok=True)
            except Exception:
                pass
            path_h = str(path_h_p)
            print(f"[build] year {y}: writing {path_h} ...", flush=True)
            con.execute(
                f"COPY (SELECT * FROM hourly ORDER BY grid_id, week, hour) TO '{path_h}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        # Always (re)build per-grid totals (in,out,total) for heatmap
        try:
            print(f"[build] year {y}: aggregating per-grid totals ...", flush=True)
            con.execute("CREATE OR REPLACE TEMP VIEW e AS SELECT * FROM edges")
        except Exception:
            # edges view may not exist if only hourly requested; create from raw
            con.execute(f"CREATE OR REPLACE TEMP VIEW src AS {sel}")
            con.execute(
                """
                CREATE OR REPLACE TEMP VIEW e AS
                SELECT o_grid, d_grid, SUM(num_total) AS num_total
                FROM src GROUP BY o_grid, d_grid
                """
            )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW gout AS
            SELECT o_grid AS grid_id, SUM(num_total) AS out_total
            FROM e GROUP BY o_grid
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW gin AS
            SELECT d_grid AS grid_id, SUM(num_total) AS in_total
            FROM e GROUP BY d_grid
            """
        )
        con.execute(
            """
            CREATE OR REPLACE TEMP VIEW gt AS
            SELECT COALESCE(gout.grid_id, gin.grid_id) AS grid_id,
                   COALESCE(gout.out_total, 0) AS out_total,
                   COALESCE(gin.in_total, 0) AS in_total,
                   COALESCE(gout.out_total, 0) + COALESCE(gin.in_total, 0) AS total
            FROM gout FULL OUTER JOIN gin ON gout.grid_id = gin.grid_id
            """
        )
        totals_p = self._parquet_path("totals", y)
        try:
            totals_p.unlink(missing_ok=True)
        except Exception:
            pass
        con.execute(
            f"COPY (SELECT * FROM gt ORDER BY grid_id) TO '{str(totals_p)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        con.close()
        try:
            (self.appdata_dir / f"build_{y}.duckdb").unlink(missing_ok=True)
        except Exception:
            pass
        print(f"[build] year {y} finished", flush=True)

    def flows_for_grid(self, year: int, grid_id: int, direction: str = "both", topk: int = 100, cov: float = 0.0) -> Dict:
        assert direction in ("out", "in", "both")
        result = {"out": [], "in": []}
        cov = float(cov or 0.0)
        if cov < 0.0: cov = 0.0
        if cov > 1.0: cov = 1.0
        if direction in ("out", "both"):
            p = str(self._parquet_path("edges_by_o", year))
            with duckdb.connect() as con:
                if cov > 0.0:
                    sql = f"""
                        WITH e AS (
                          SELECT d_grid, num_total FROM read_parquet('{p}') WHERE o_grid = ?
                        )
                        SELECT d_grid, num_total
                        FROM (
                            SELECT d_grid, num_total,
                                   SUM(num_total) OVER (ORDER BY num_total DESC ROWS UNBOUNDED PRECEDING) AS cs,
                                   SUM(num_total) OVER () AS tot
                            FROM e
                            ORDER BY num_total DESC
                        )
                        WHERE cs <= ? * tot
                        ORDER BY num_total DESC
                        LIMIT ?
                    """
                    rows = con.execute(sql, [grid_id, cov, topk]).fetchall()
                else:
                    sql = f"SELECT d_grid, num_total FROM read_parquet('{p}') WHERE o_grid = ? ORDER BY num_total DESC LIMIT ?"
                    rows = con.execute(sql, [grid_id, topk]).fetchall()
            result["out"] = [{"d_grid": int(d), "num_total": float(v)} for (d, v) in rows]
        if direction in ("in", "both"):
            p = str(self._parquet_path("edges_by_d", year))
            with duckdb.connect() as con:
                if cov > 0.0:
                    sql = f"""
                        WITH e AS (
                          SELECT o_grid, num_total FROM read_parquet('{p}') WHERE d_grid = ?
                        )
                        SELECT o_grid, num_total
                        FROM (
                            SELECT o_grid, num_total,
                                   SUM(num_total) OVER (ORDER BY num_total DESC ROWS UNBOUNDED PRECEDING) AS cs,
                                   SUM(num_total) OVER () AS tot
                            FROM e
                            ORDER BY num_total DESC
                        )
                        WHERE cs <= ? * tot
                        ORDER BY num_total DESC
                        LIMIT ?
                    """
                    rows = con.execute(sql, [grid_id, cov, topk]).fetchall()
                else:
                    sql = f"SELECT o_grid, num_total FROM read_parquet('{p}') WHERE d_grid = ? ORDER BY num_total DESC LIMIT ?"
                    rows = con.execute(sql, [grid_id, topk]).fetchall()
            result["in"] = [{"o_grid": int(o), "num_total": float(v)} for (o, v) in rows]

        # enrich with coords
        center = self.meta_by_id.get(grid_id)
        def coord(gid: int):
            m = self.meta_by_id.get(gid)
            if not m:
                return None
            return {"grid_id": gid, "lon": m["lon"], "lat": m["lat"], "area_name": m.get("area_name", ""), "city_name": m.get("city_name", "")}

        out_edges = []
        for e in result.get("out", []):
            tgt = coord(e["d_grid"]) 
            if tgt:
                out_edges.append({"o_grid": grid_id, "d_grid": e["d_grid"], "num_total": e["num_total"], "o": center, "d": tgt})
        in_edges = []
        for e in result.get("in", []):
            src = coord(e["o_grid"]) 
            if src:
                in_edges.append({"o_grid": e["o_grid"], "d_grid": grid_id, "num_total": e["num_total"], "o": src, "d": center})
        return {"center": center, "out_edges": out_edges, "in_edges": in_edges}

    def hourly_series_for_grid(self, grid_id: int, years: List[int] = None) -> Dict[int, Dict[str, List[List[float]]]]:
        years = years or YEARS
        out: Dict[int, Dict[str, List[List[float]]]] = {}
        for y in years:
            pth = self._parquet_path("hourly", y)
            if not pth.exists():
                continue
            p = str(pth)
            with duckdb.connect() as con:
                rows = con.execute(
                    f"SELECT week, hour, out_total, in_total, total FROM read_parquet('{p}') WHERE grid_id = ? ORDER BY week, hour",
                    [grid_id],
                ).fetchall()
            weeks_all = sorted({int(r[0]) for r in rows})
            # Only keep earliest week (changed from 3 weeks to 1 week)
            weeks = weeks_all[:1]
            week_map = {w: f"W{i+1}" for i, w in enumerate(weeks)}
            tmp = {"out": {"W1": [0.0]*24},
                   "in": {"W1": [0.0]*24},
                   "total": {"W1": [0.0]*24}}
            for w, h, out_t, in_t, tot in rows:
                key = week_map.get(int(w))
                if not key:
                    continue
                h = int(h)
                if 0 <= h < 24:
                    tmp["out"][key][h] = float(out_t)
                    tmp["in"][key][h] = float(in_t)
                    tmp["total"][key][h] = float(tot)
            out[y] = {
                "out": [tmp["out"][f"W{i}"] for i in range(1, 2)],
                "in": [tmp["in"][f"W{i}"] for i in range(1, 2)],
                "total": [tmp["total"][f"W{i}"] for i in range(1, 2)],
            }
        return out


VERSION = "2025-10-14"


def create_app(use_sample: bool = False, parquet_only: bool = False) -> Flask:
    _ensure_dirs()
    app = Flask(__name__, template_folder=str(TEMPLATES_DIR), static_folder=str(STATIC_DIR))
    # enable gzip for JSON/JS/CSS if available
    if Compress is not None:
        try:
            Compress(app)
        except Exception:
            pass
    meta_path = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"
    # Optionally use sample CSVs for quick testing
    engine = DataEngine(DATA_DIR, APPDATA_DIR, meta_path, require_csv=not parquet_only)
    if use_sample and not parquet_only:
        for y in YEARS:
            sp = DATA_DIR / f"{y}.sample_sz.csv"
            if sp.exists():
                engine.year_paths[y] = sp
                engine.year_colmaps[y] = detect_cols(sp)

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/api/years")
    def api_years():
        return jsonify(YEARS)

    @app.route("/api/version")
    def api_version():
        # simple version endpoint for debugging connectivity
        try:
            host = request.host
        except Exception:
            host = ""
        routes = {
            "label_queue_back": True,
            "label_queue_set": True,
            "heat": True,
            "bounds": True,
        }
        return jsonify({"version": VERSION, "server": host, "routes": routes})

    @app.route('/favicon.ico')
    def favicon():
        # serve favicon if present under static
        try:
            return send_from_directory(str(STATIC_DIR), 'favicon.ico')
        except Exception:
            return ('', 204)

    @app.route("/api/meta/cities")
    def api_meta_cities():
        # unique city list from metadata
        cities = sorted({(m.get("city_name") or "").strip() for m in engine.meta_items if m.get("city_name")})
        return jsonify(cities)

    @app.route("/api/metadata")
    def api_metadata():
        # optional filtering to reduce payload
        city = (request.args.get("city_name") or "").strip()
        area = (request.args.get("area_name") or "").strip()
        if not city and not area:
            # for safety: if no filters, still allow but clients should filter client-side
            return jsonify(engine.meta_items)
        items = []
        for m in engine.meta_items:
            if city and (m.get("city_name") or "") != city:
                continue
            if area and (m.get("area_name") or "") != area:
                continue
            items.append(m)
        return jsonify(items)

    @app.route("/api/meta/one")
    def api_meta_one():
        try:
            gid = int(request.args.get('grid_id','0'))
        except Exception:
            gid = 0
        if gid == 0:
            return jsonify({"error": "grid_id required"}), 400
        m = engine.meta_by_id.get(gid)
        if not m:
            return jsonify({}), 404
        return jsonify(m)

    @app.route("/api/build", methods=["POST"])  # optional explicit build trigger
    def api_build():
        years = request.json.get("years") if request.is_json else None
        if parquet_only:
            return jsonify({"error": "parquet-only mode: building is disabled (no CSV). Copy CSVs or run without --parquet-only."}), 400
        ys = [int(y) for y in (years or YEARS)]
        engine.ensure_built(ys)
        return jsonify({"status": "ok", "built": ys})

    @app.route("/api/flows")
    def api_flows():
        year_param = request.args.get("year", "2018")
        grid_id = int(request.args.get("grid_id", "0"))
        if grid_id == 0:
            return jsonify({"error": "grid_id required"}), 400
        direction = request.args.get("direction", "both")
        topk = int(request.args.get("topk", "100"))
        cov = float(request.args.get("cov", "0"))

        if year_param == "all":
            data = {}
            available = []
            for y in YEARS:
                if engine._parquet_path("edges_by_o", y).exists() and engine._parquet_path("edges_by_d", y).exists():
                    data[y] = engine.flows_for_grid(y, grid_id, direction=direction, topk=topk, cov=cov)
                    available.append(y)
            if not available:
                return jsonify({"error": "no indexes available", "hint": "POST /api/build or run: python vis/server.py --build"}), 404
            return jsonify({"years": data})
        else:
            y = int(year_param)
            if not engine._parquet_path("edges_by_o", y).exists() or not engine._parquet_path("edges_by_d", y).exists():
                return jsonify({"error": "index missing", "hint": "POST /api/build or run: python vis/server.py --build", "years": [y]}), 409
            data = engine.flows_for_grid(y, grid_id, direction=direction, topk=topk, cov=cov)
            return jsonify(data)

    @app.route("/api/hourly")
    def api_hourly():
        grid_id = int(request.args.get("grid_id", "0"))
        if grid_id == 0:
            return jsonify({"error": "grid_id required"}), 400
        # return available years only to avoid blank chart
        out = {}
        for y in YEARS:
            if engine._parquet_path("hourly", y).exists():
                out[y] = engine.hourly_series_for_grid(grid_id)[y]
        if not out:
            return jsonify({})
        return jsonify(out)

    @app.route("/api/heat")
    def api_heat():
        year = int(request.args.get("year", YEARS[0]))
        metric = request.args.get("metric", "total")  # total|in|out
        if metric not in ("total", "in", "out"):
            return jsonify({"error": "metric must be total|in|out"}), 400
        p = engine._parquet_path("totals", year)
        if not p.exists():
            # Build on the fly from edges-by parquet
            try:
                engine.build_totals_for_year(year)
            except Exception as e:
                return jsonify({"error": "cannot build totals", "detail": str(e)}), 500
        city = request.args.get("city_name", "")
        area = request.args.get("area_name", "")
        # read all then filter in Python; small (<= grid count) and fast
        with duckdb.connect() as con:
            rows = con.execute(f"SELECT grid_id, out_total, in_total, total FROM read_parquet('{str(p)}')").fetchall()
        items = []
        for gid, out_t, in_t, tot in rows:
            m = engine.meta_by_id.get(int(gid))
            if not m:
                continue
            if city and m.get("city_name", "") != city:
                continue
            if area and m.get("area_name", "") != area:
                continue
            val = float(tot if metric == "total" else (in_t if metric == "in" else out_t))
            items.append({"grid_id": int(gid), "v": val})
        if not items:
            return jsonify({"values": [], "q95": 0, "max": 0, "n": 0})
        # compute p95 and max for scaling
        vs = sorted([it["v"] for it in items])
        import math
        q95 = vs[int(math.floor(0.95 * (len(vs)-1)))] if len(vs) > 1 else vs[0]
        mx = vs[-1]
        return jsonify({"values": items, "q95": float(q95), "max": float(mx), "n": len(items)})

    labels_csv = LABELS_DIR / "labels.csv"
    SHOTS_DIR = LABELS_DIR / "shots"
    SHP_DIR = DATA_DIR / "shp"
    SHP_CACHE_DIR = APPDATA_DIR / "shp_cache"
    SHP_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _bounds_cache = {"city": None, "district": None}  # lazy loaded
    queue_json = APPDATA_DIR / "label_queue.json"

    def _read_labels() -> List[Dict]:
        if not labels_csv.exists():
            return []
        out: List[Dict] = []
        with labels_csv.open("r", newline="") as f:
            reader = csv.DictReader(f)
            headers = [h.strip() for h in (reader.fieldnames or [])]
            for r in reader:
                try:
                    item = {
                        "grid_id": int(r["grid_id"]),
                        "lon": float(r["lon"]),
                        "lat": float(r["lat"]),
                        "label": int(r["label"]),
                    }
                    if "remark" in headers:
                        item["remark"] = r.get("remark", "")
                    out.append(item)
                except Exception:
                    continue
        return out

    def _ensure_labels_header_has_remark():
        if not labels_csv.exists():
            return
        with labels_csv.open('r', newline='') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                header = []
        if header and 'remark' not in header:
            # migrate file to include remark column
            rows = _read_labels()
            with labels_csv.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["grid_id", "lon", "lat", "label", "remark"])
                for r in rows:
                    writer.writerow([r['grid_id'], r['lon'], r['lat'], r['label'], r.get('remark','')])

    def _append_label(grid_id: int, label: int, remark: str = ""):
        m = engine.meta_by_id.get(grid_id)
        if not m:
            raise ValueError("grid_id not in metadata")
        exists = labels_csv.exists()
        with labels_csv.open("a", newline="") as f:
            writer = csv.writer(f)
            if not exists:
                writer.writerow(["grid_id", "lon", "lat", "label", "remark"])  # header
            else:
                _ensure_labels_header_has_remark()
            # append row with remark
            writer.writerow([grid_id, m["lon"], m["lat"], label, remark or ""]) 

    # ensure screenshot output directory
    try:
        SHOTS_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    @app.route("/api/screenshot", methods=["POST"])  # save base64 JPEG to labels/shots
    def api_save_screenshot():
        try:
            data = request.get_json(force=True)
        except Exception:
            return jsonify({"error": "invalid json"}), 400
        # expected fields: filename (e.g., "123-4.jpg"), data (base64 like "data:image/jpeg;base64,...")
        name = str(data.get("filename", "")).strip()
        b64 = str(data.get("data", "")).strip()
        if not name or not b64:
            return jsonify({"error": "filename and data required"}), 400
        # normalize filename: only allow [A-Za-z0-9-_ .]
        safe = ''.join(ch for ch in name if ch.isalnum() or ch in ('-', '_', ' ', '.'))
        if not safe:
            return jsonify({"error": "bad filename"}), 400
        # force .jpg extension
        if not safe.lower().endswith('.jpg') and not safe.lower().endswith('.jpeg'):
            safe += '.jpg'
        # strip data url prefix if present
        if ',' in b64:
            b64 = b64.split(',', 1)[1]
        import base64
        try:
            raw = base64.b64decode(b64, validate=False)
        except Exception as e:
            return jsonify({"error": "decode failed", "detail": str(e)}), 400
        out_path = SHOTS_DIR / safe
        try:
            with out_path.open('wb') as f:
                f.write(raw)
        except Exception as e:
            return jsonify({"error": "write failed", "detail": str(e)}), 500
        return jsonify({"ok": True, "path": str(out_path)})

    def _unlabeled_grid_ids(filters: Dict = None) -> List[int]:
        labeled = {r["grid_id"] for r in _read_labels()}
        pool = []
        city = (filters or {}).get("city_name") or ""
        area = (filters or {}).get("area_name") or ""
        keyword = (filters or {}).get("keyword") or ""
        kw = keyword.strip().lower()
        for m in engine.meta_items:
            if m["grid_id"] in labeled:
                continue
            if city and m.get("city_name", "") != city:
                continue
            if area and m.get("area_name", "") != area:
                continue
            if kw:
                text = f"{m.get('city_name','')} {m.get('area_name','')}".lower()
                if kw not in text:
                    continue
            pool.append(m["grid_id"])
        return pool

    def _read_queue() -> Dict:
        if queue_json.exists():
            try:
                return json.loads(queue_json.read_text())
            except Exception:
                pass
        return {"queue": [], "index": 0, "filters": {}, "seed": None}

    def _write_queue(data: Dict):
        queue_json.write_text(json.dumps(data, ensure_ascii=False))

    @app.route("/api/labels", methods=["GET"])  # list labels
    def api_labels_list():
        return jsonify(_read_labels())

    @app.route("/api/labels/stats", methods=["GET"])  # labeled counts per class
    def api_labels_stats():
        rows = _read_labels()
        # count labels 0..9 (0=其他)
        counts = {i: 0 for i in range(0, 10)}
        for r in rows:
            try:
                l = int(r.get("label", -1))
            except Exception:
                l = -1
            if 0 <= l <= 9:
                counts[l] = counts.get(l, 0) + 1
        total = sum(counts.values())
        # return as by_label with string keys for stability
        by_label = {str(k): int(v) for k, v in counts.items()}
        return jsonify({"total": int(total), "by_label": by_label})

    @app.route("/api/labels/download", methods=["GET"])  # download labels.csv
    def api_labels_download():
        if not labels_csv.exists():
            # create empty
            with labels_csv.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["grid_id", "lon", "lat", "label"])
        return send_file(str(labels_csv), as_attachment=True)

    @app.route("/api/labels/import", methods=["POST"])  # import labels CSV
    def api_labels_import():
        mode = request.args.get("mode", "upsert")  # append|upsert
        if 'file' not in request.files:
            return jsonify({"error": "no file"}), 400
        file = request.files['file']
        if not file or file.filename == '':
            return jsonify({"error": "empty file"}), 400
        # Save temp
        tmp_path = APPDATA_DIR / ("import_" + secure_filename(file.filename))
        file.save(str(tmp_path))
        # Read incoming
        import_rows: List[Dict] = []
        with tmp_path.open('r', newline='') as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    import_rows.append({
                        "grid_id": int(r["grid_id"]),
                        "lon": float(r.get("lon", 0) or 0),
                        "lat": float(r.get("lat", 0) or 0),
                        "label": int(r["label"]),
                    })
                except Exception:
                    continue
        tmp_path.unlink(missing_ok=True)
        if mode == 'append':
            exists = labels_csv.exists()
            with labels_csv.open('a', newline='') as f:
                writer = csv.writer(f)
                if not exists:
                    writer.writerow(["grid_id", "lon", "lat", "label"])  # header
                for r in import_rows:
                    m = engine.meta_by_id.get(r["grid_id"]) or {"lon": r["lon"], "lat": r["lat"]}
                    writer.writerow([r["grid_id"], m["lon"], m["lat"], r["label"]])
        else:  # upsert by grid_id
            # Build map from existing + imported (imported wins)
            latest: Dict[int, Dict] = {r["grid_id"]: r for r in _read_labels()}
            for r in import_rows:
                # enrich coords from metadata if missing
                m = engine.meta_by_id.get(r["grid_id"]) or {"lon": r.get("lon", 0.0), "lat": r.get("lat", 0.0)}
                latest[r["grid_id"]] = {"grid_id": r["grid_id"], "lon": m["lon"], "lat": m["lat"], "label": r["label"]}
            with labels_csv.open('w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["grid_id", "lon", "lat", "label"])  # header
                for gid, r in latest.items():
                    writer.writerow([gid, r["lon"], r["lat"], r["label"]])
        return jsonify({"ok": True, "mode": mode, "imported": len(import_rows)})

    @app.route("/api/label", methods=["POST"])  # save one label
    def api_label_save():
        data = request.get_json(force=True)
        grid_id = int(data.get("grid_id"))
        label = int(data.get("label"))
        remark = str(data.get("remark", ""))
        # 允许 0..9（0=其他；1-9 为九类标签）
        if label < 0 or label > 9:
            return jsonify({"error": "label must be 0..9, where 0=其他"}), 400
        _append_label(grid_id, label, remark)
        return jsonify({"ok": True})

    @app.route("/api/label/undo", methods=["POST"])  # remove last label row
    def api_label_undo():
        if not labels_csv.exists():
            return jsonify({"ok": True, "message": "no labels"})
        with labels_csv.open("r", newline="") as f:
            lines = f.readlines()
        if len(lines) <= 1:
            labels_csv.unlink(missing_ok=True)
            return jsonify({"ok": True, "message": "cleared"})
        # keep header and all but last row
        with labels_csv.open("w", newline="") as f:
            f.writelines(lines[:-1])
        return jsonify({"ok": True})

    @app.route("/api/label_queue", methods=["GET"])  # read current queue
    def api_label_queue_get():
        return jsonify(_read_queue())

    @app.route("/api/label_queue/start", methods=["POST"])  # start a new persistent queue
    def api_label_queue_start():
        data = request.get_json(force=True) if request.is_json else {}
        count = int(data.get("count", 20))
        filters = {
            "city_name": data.get("city_name") or "",
            "area_name": data.get("area_name") or "",
            "keyword": data.get("keyword") or "",
        }
        # Optional low-traffic exclusion based on 24h daily-average low-value share
        # Params:
        # - low_pct: 0..100 (e.g., 30 -> exclude grids with >=30% hours whose daily-average <= low_value)
        # - low_value: non-negative threshold for "low" hour (default 0)
        # - filter_year: year of hourly parquet to use; if missing, pick first available
        try:
            # backward-compat: treat zero_pct as low_pct when provided
            low_pct = data.get("low_pct")
            if low_pct is None and data.get("zero_pct") is not None:
                low_pct = data.get("zero_pct")
            low_pct = float(low_pct) if low_pct is not None else None
            if low_pct is not None:
                low_pct = max(0.0, min(100.0, low_pct))
        except Exception:
            low_pct = None
        try:
            low_value = data.get("low_value")
            low_value = float(low_value) if low_value is not None else 0.0
            if low_value < 0:
                low_value = 0.0
        except Exception:
            low_value = 0.0
        try:
            filter_year = int(data.get("filter_year")) if data.get("filter_year") is not None else None
        except Exception:
            filter_year = None
        seed = data.get("seed")
        pool = _unlabeled_grid_ids(filters)
        debug_info = {"applied": False, "chosen_year": None, "weeks": [], "pool_before": len(pool), "pool_after": None, "removed": None}
        # apply low-share filter if requested
        if pool and low_pct is not None and low_pct > 0.0:
            # choose a year that has hourly parquet
            years_try = []
            if filter_year is not None:
                years_try.append(filter_year)
            years_try.extend([y for y in YEARS if y not in years_try])
            chosen_year = None
            for y in years_try:
                if engine._parquet_path("hourly", y).exists():
                    chosen_year = y
                    break
            if chosen_year is not None:
                try:
                    p = str(engine._parquet_path("hourly", chosen_year))
                    with duckdb.connect() as con:
                        # Ensure pool grid_ids are considered even if they never appear in hourly parquet
                        use_pool = True
                        try:
                            con.execute("DROP TABLE IF EXISTS pool")
                            con.execute("CREATE TEMPORARY TABLE pool(gid BIGINT)")
                            con.executemany("INSERT INTO pool (gid) VALUES (?)", [(int(g),) for g in pool])
                        except Exception:
                            use_pool = False
                        # Use all available weeks for the chosen year (no LIMIT) so the filter reflects the full year data
                        weeks = [r[0] for r in con.execute(f"SELECT DISTINCT week FROM read_parquet('{p}') ORDER BY week").fetchall()]
                        debug_info["applied"] = True
                        debug_info["chosen_year"] = chosen_year
                        debug_info["weeks"] = [int(w) for w in weeks]
                        bad_set = set()
                        if weeks:
                            week_list = ",".join(str(int(w)) for w in weeks)
                            if use_pool:
                                sql = f"""
                                    WITH base_raw AS (
                                      SELECT grid_id, hour, total
                                      FROM read_parquet('{p}')
                                      WHERE week IN ({week_list})
                                    ), base AS (
                                      SELECT br.grid_id, br.hour, AVG(br.total) AS avg_total
                                      FROM base_raw br JOIN pool p ON br.grid_id = p.gid
                                      GROUP BY br.grid_id, br.hour
                                    ), gids AS (
                                      SELECT gid AS grid_id FROM pool
                                    ), hrs AS (
                                      SELECT range AS hour FROM range(0,24)
                                    ), full24 AS (
                                      SELECT g.grid_id, h.hour FROM gids g CROSS JOIN hrs h
                                    ), h AS (
                                      SELECT f.grid_id, f.hour, COALESCE(b.avg_total, 0) AS avg_total
                                      FROM full24 f LEFT JOIN base b USING (grid_id, hour)
                                    ), zr AS (
                                      SELECT grid_id,
                                        SUM(CASE WHEN avg_total <= ? THEN 1 ELSE 0 END)::DOUBLE / 24.0 AS low_ratio
                                      FROM h
                                      GROUP BY grid_id
                                    )
                                    SELECT grid_id FROM zr WHERE low_ratio >= ?
                                """
                                rows = con.execute(sql, [float(low_value), float(low_pct)/100.0]).fetchall()
                            else:
                                # Fallback: derive gids from parquet (older DuckDB without temp table)
                                sql = f"""
                                    WITH base AS (
                                      SELECT grid_id, hour, AVG(total) AS avg_total
                                      FROM read_parquet('{p}')
                                      WHERE week IN ({week_list})
                                      GROUP BY grid_id, hour
                                    ), gids AS (
                                      SELECT DISTINCT grid_id FROM base
                                    ), hrs AS (
                                      SELECT range AS hour FROM range(0,24)
                                    ), full24 AS (
                                      SELECT g.grid_id, h.hour FROM gids g CROSS JOIN hrs h
                                    ), h AS (
                                      SELECT f.grid_id, f.hour, COALESCE(b.avg_total, 0) AS avg_total
                                      FROM full24 f LEFT JOIN base b USING (grid_id, hour)
                                    ), zr AS (
                                      SELECT grid_id,
                                        SUM(CASE WHEN avg_total <= ? THEN 1 ELSE 0 END)::DOUBLE / 24.0 AS low_ratio
                                      FROM h
                                      GROUP BY grid_id
                                    )
                                    SELECT grid_id FROM zr WHERE low_ratio >= ?
                                """
                            rows = con.execute(sql, [float(low_value), float(low_pct)/100.0]).fetchall()
                            bad_set = {int(r[0]) for r in rows}
                        # If no weeks available, keep pool unchanged
                    pool = [gid for gid in pool if gid not in bad_set]
                    debug_info["pool_after"] = len(pool)
                    debug_info["removed"] = int(debug_info["pool_before"]) - int(debug_info["pool_after"])
                except Exception:
                    # ignore filter errors; proceed without zero filter
                    pass
        if not pool:
            if debug_info["pool_after"] is None:
                debug_info["pool_after"] = len(pool)
                debug_info["removed"] = int(debug_info["pool_before"]) - int(debug_info["pool_after"])  # may be 0
            q = {"queue": [], "index": 0, "filters": filters, "seed": seed, "debug": debug_info}
            _write_queue(q)
            return jsonify(q)
        rnd = random.Random(seed)
        rnd.shuffle(pool)
        queue = pool[: max(1, count)]
        if debug_info["pool_after"] is None:
            debug_info["pool_after"] = len(pool)
            debug_info["removed"] = int(debug_info["pool_before"]) - int(debug_info["pool_after"])  # may be 0
        q = {"queue": queue, "index": 0, "filters": filters, "seed": seed, "debug": debug_info}
        _write_queue(q)
        return jsonify(q)

    @app.route("/api/label_queue/advance", methods=["POST"])  # move pointer forward
    def api_label_queue_advance():
        q = _read_queue()
        idx = int(q.get("index", 0))
        queue = q.get("queue", [])
        if idx < len(queue):
            idx += 1
        q["index"] = idx
        _write_queue(q)
        has_more = idx < len(queue)
        cur = queue[idx] if has_more else None
        return jsonify({"index": idx, "has_more": has_more, "current": cur, "total": len(queue)})


    @app.route("/api/low_filter_debug")
    def api_low_filter_debug():
        """Debug endpoint: inspect low-traffic filter inputs/decision for one grid.
        Query params:
          - grid_id (int, required)
          - year (int, optional): chosen year; if missing, pick first available hourly parquet
          - low_value (float, default 0): threshold for "low" hour (avg_total <= low_value)
          - low_pct (float, default 0): percentage threshold; decision = (low_ratio >= low_pct/100)
        Returns per-hour avg_total, le_threshold flags, the ratio and decision.
        """
        try:
            grid_id = int(request.args.get("grid_id"))
        except Exception:
            return jsonify({"error": "grid_id required"}), 400
        try:
            year = request.args.get("year")
            year = int(year) if year is not None else None
        except Exception:
            year = None
        try:
            low_value = float(request.args.get("low_value", 0.0))
        except Exception:
            low_value = 0.0
        try:
            low_pct = float(request.args.get("low_pct", 0.0))
        except Exception:
            low_pct = 0.0
        # choose a year
        years_try = []
        if year is not None:
            years_try.append(year)
        years_try.extend([y for y in YEARS if y not in years_try])
        chosen_year = None
        for y in years_try:
            if engine._parquet_path("hourly", y).exists():
                chosen_year = y
                break
        if chosen_year is None:
            return jsonify({"error": "no hourly parquet available"}), 404
        p = str(engine._parquet_path("hourly", chosen_year))
        # compute over all weeks
        try:
            with duckdb.connect() as con:
                weeks = [r[0] for r in con.execute(f"SELECT DISTINCT week FROM read_parquet('{p}') ORDER BY week").fetchall()]
                if not weeks:
                    return jsonify({"error": "no weeks in parquet", "year": chosen_year}), 404
                week_list = ",".join(str(int(w)) for w in weeks)
                sql = f"""
                    WITH base_raw AS (
                      SELECT grid_id, hour, total
                      FROM read_parquet('{p}')
                      WHERE week IN ({week_list}) AND grid_id = {grid_id}
                    ), base AS (
                      SELECT hour, AVG(total) AS avg_total
                      FROM base_raw
                      GROUP BY hour
                    ), hrs AS (
                      SELECT range AS hour FROM range(0,24)
                    ), h AS (
                      SELECT h.hour, COALESCE(b.avg_total, 0) AS avg_total
                      FROM hrs h LEFT JOIN base b USING (hour)
                    )
                    SELECT hour, avg_total, (avg_total <= ?) AS le
                    FROM h ORDER BY hour
                """
                rows = con.execute(sql, [float(low_value)]).fetchall()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        hours = []
        le_cnt = 0
        for hour, avg_total, le in rows:
            hours.append({"hour": int(hour), "avg_total": float(avg_total), "le": bool(le)})
            if le:
                le_cnt += 1
        ratio = le_cnt / 24.0 if hours else 0.0
        decision = (ratio >= (low_pct/100.0)) if low_pct > 0.0 else False
        return jsonify({
            "grid_id": grid_id,
            "year": chosen_year,
            "low_value": low_value,
            "low_pct": low_pct,
            "ratio": ratio,
            "le_count": le_cnt,
            "decision_exclude": decision,
            "hours": hours,
        })

    @app.route("/api/label_queue/back", methods=["POST"])  # move pointer backward
    def api_label_queue_back():
        q = _read_queue()
        idx = int(q.get("index", 0))
        queue = q.get("queue", [])
        if idx > 0:
            idx -= 1
        q["index"] = idx
        _write_queue(q)
        has_more = idx < len(queue)
        cur = queue[idx] if idx < len(queue) else None
        return jsonify({"index": idx, "has_more": has_more, "current": cur, "total": len(queue)})

    @app.route("/api/label_queue/set", methods=["POST"])  # set pointer to index or grid_id
    def api_label_queue_set():
        q = _read_queue()
        queue = q.get("queue", [])
        data = request.get_json(force=True, silent=True) or {}
        idx = data.get("index")
        gid = data.get("grid_id")
        if gid is not None and queue:
            try:
                gid = int(gid)
                idx = queue.index(gid)
            except Exception:
                idx = q.get("index", 0)
        try:
            idx = int(idx)
        except Exception:
            idx = q.get("index", 0)
        if idx < 0:
            idx = 0
        if idx > len(queue):
            idx = len(queue)
        q["index"] = idx
        _write_queue(q)
        cur = queue[idx] if idx < len(queue) else None
        return jsonify({"index": idx, "current": cur, "total": len(queue)})

    @app.route("/api/labels/clear", methods=["POST"])  # clear labels.csv (backup old)
    def api_labels_clear():
        if labels_csv.exists():
            ts = int(time.time())
            backup = labels_csv.parent / f"labels_backup_{ts}.csv"
            try:
                labels_csv.replace(backup)
            except Exception:
                pass
        # re-create empty with header
        with labels_csv.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["grid_id", "lon", "lat", "label", "remark"])  # header
        return jsonify({"ok": True})

    @app.route("/api/label_queue/reset", methods=["POST"])  # clear queue
    def api_label_queue_reset():
        queue_json.unlink(missing_ok=True)
        return jsonify({"ok": True})

    def _rdp(points, epsilon):
        # Douglas-Peucker simplification on [ [x,y], ... ]
        if len(points) < 3:
            return points
        import math
        def dist(p, a, b):
            # perpendicular distance from p to segment a-b
            (x, y) = p; (x1, y1) = a; (x2, y2) = b
            if x1 == x2 and y1 == y2:
                return math.hypot(x - x1, y - y1)
            t = ((x - x1)*(x2 - x1) + (y - y1)*(y2 - y1)) / ((x2 - x1)**2 + (y2 - y1)**2)
            t = max(0, min(1, t))
            px = x1 + t * (x2 - x1); py = y1 + t * (y2 - y1)
            return math.hypot(x - px, y - py)
        def rdp_rec(pts):
            dmax = 0.0; idx = 0
            for i in range(1, len(pts) - 1):
                d = dist(pts[i], pts[0], pts[-1])
                if d > dmax: dmax = d; idx = i
            if dmax > epsilon:
                res1 = rdp_rec(pts[:idx+1])
                res2 = rdp_rec(pts[idx:])
                return res1[:-1] + res2
            else:
                return [pts[0], pts[-1]]
        return rdp_rec(points)

    def _load_bounds(level: str):
        # level: 'city' -> PRD_CITY, 'district' -> PRD_district
        target = 'PRD_CITY' if level == 'city' else 'PRD_district'
        shp_path = SHP_DIR / f"{target}.shp"
        dbf_path = SHP_DIR / f"{target}.dbf"
        prj_path = SHP_DIR / f"{target}.prj"
        if not shp_path.exists():
            raise FileNotFoundError(f"missing shapefile: {shp_path}")
        # Read shapefile using pyshp; transform via pyproj if projected
        try:
            import shapefile  # pyshp
        except Exception as e:
            raise RuntimeError("Please pip install shapefile")
        transformer = None
        try:
            from pyproj import CRS, Transformer
            if prj_path.exists():
                prj_wkt = prj_path.read_text()
                src = CRS.from_wkt(prj_wkt)
                # If not geographic (latlon degrees), transform to EPSG:4326
                if not src.is_geographic:
                    transformer = Transformer.from_crs(src, CRS.from_epsg(4326), always_xy=True)
        except Exception:
            transformer = None

        r = shapefile.Reader(str(shp_path))
        fields = [f[0] for f in r.fields[1:]]
        recs = r.shapeRecords()
        # choose label field
        label_field = None
        for cand in ['city_name','CityName','NAME','Name','NAME_2','name','NAME_CH','CITY','city']:
            if cand in fields:
                label_field = cand; break
        if not label_field:
            # fallback: first field
            label_field = fields[0] if fields else None
        out = []
        # Simplification tolerance in degrees (~50-100m)
        tol = 0.001
        # Limit points per ring to keep payload light
        max_pts = 2000
        for sr in recs:
            shp = sr.shape
            rec = sr.record
            name = str(rec[fields.index(label_field)]) if label_field else ""
            pts = shp.points
            parts = list(shp.parts) + [len(pts)]
            rings = []
            for i in range(len(parts)-1):
                seg = pts[parts[i]:parts[i+1]]
                # transform
                ring = []
                if transformer:
                    for (x,y) in seg:
                        lon, lat = transformer.transform(x, y)
                        ring.append([float(lon), float(lat)])
                else:
                    for (x,y) in seg:
                        ring.append([float(x), float(y)])
                # simplify and decimate
                if len(ring) > 2:
                    ring = _rdp(ring, tol)
                if len(ring) > max_pts:
                    step = max(1, len(ring)//max_pts)
                    ring = ring[::step]
                rings.append(ring)
            out.append({"name": name, "rings": rings})
        return out

    @app.route('/api/bounds')
    def api_bounds():
        level = request.args.get('level','city')
        cache_key = 'city' if level=='city' else 'district'
        cache_file = SHP_CACHE_DIR / f"{cache_key}_bounds.json"
        if _bounds_cache.get(cache_key) is None:
            # try load cache
            if cache_file.exists():
                try:
                    _bounds_cache[cache_key] = json.loads(cache_file.read_text())
                except Exception:
                    _bounds_cache[cache_key] = None
            if _bounds_cache.get(cache_key) is None:
                try:
                    data = _load_bounds(cache_key)
                except Exception as e:
                    return jsonify({"error":"load shapefile failed", "detail": str(e)}), 500
                # Save cache
                try:
                    cache_file.write_text(json.dumps(data, ensure_ascii=False))
                except Exception:
                    pass
                _bounds_cache[cache_key] = data
        data = _bounds_cache.get(cache_key) or []
        # optional filter by names (comma-separated)
        names = request.args.get('names','').strip()
        if names:
            name_set = set([n.strip() for n in names.split(',') if n.strip()])
            data = [d for d in data if d.get('name') in name_set]
        return jsonify({"level": cache_key, "items": data})

    return app


def main():
    parser = argparse.ArgumentParser(description="Mobility labeling web app")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--build", action="store_true", help="precompute parquet indexes and exit")
    parser.add_argument("--use-sample", action="store_true", help="use *.sample_sz.csv for faster trial")
    parser.add_argument("--parquet-only", action="store_true", help="run without raw CSV; requires prebuilt parquet under vis/appdata")
    args = parser.parse_args()

    _ensure_dirs()
    meta_path = DATA_DIR / "grid_metadata" / "PRD_grid_metadata.csv"
    engine = DataEngine(DATA_DIR, APPDATA_DIR, meta_path, require_csv=not args.parquet_only)
    if args.use_sample and not args.parquet_only:
        for y in YEARS:
            sp = DATA_DIR / f"{y}.sample_sz.csv"
            if sp.exists():
                engine.year_paths[y] = sp
                engine.year_colmaps[y] = detect_cols(sp)
    if args.build:
        engine.ensure_built(YEARS)
        print("build complete")
        return
    app = create_app(use_sample=args.use_sample, parquet_only=args.parquet_only)
    # No auto-build by default; use --build or POST /api/build when ready.
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
