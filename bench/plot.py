#!/usr/bin/env python3
"""
Generate all STRIQ benchmark visualizations from CSV data.
Everything in the README comes from here.
Requirements: pip install plotly kaleido
"""

import csv, math, sys
from pathlib import Path
from collections import defaultdict

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("Install with: pip install plotly kaleido")
    sys.exit(1)

RESULTS = Path(__file__).parent / "results"
PLOTS   = RESULTS / "plots"
CSV_F   = RESULTS / "compression.csv"
EPS_F   = RESULTS / "epsilon.csv"
PLOTS.mkdir(parents=True, exist_ok=True)

BLUE, ORANGE, GREEN, PURPLE = "#2563EB", "#F97316", "#22C55E", "#A855F7"
RED = "#EF4444"

L = dict(template="plotly_white",
         font=dict(family="Inter, system-ui, sans-serif", size=14),
         margin=dict(l=70, r=30, t=55, b=55))

PICKS = {
    "jena_climate_2009_2016": ["p (mbar)", "T (degC)", "rh (%)", "sh (g/kg)", "VPact (mbar)"],
    "household_power_consumption": ["Global_active_power", "Voltage", "Global_intensity", "Sub_metering_1"],
    "metro_traffic": ["temp", "traffic_volume", "clouds_all"],
    "noaa_gsod": ["TEMP", "SLP", "VISIB", "PRCP"],
}

DS_LABELS = {
    "jena_climate_2009_2016": "Jena Climate",
    "household_power_consumption": "Household Power",
    "metro_traffic": "Metro Traffic",
    "noaa_gsod": "NOAA GSOD",
}


def load():
    rows = []
    with open(CSV_F) as f:
        for r in csv.DictReader(f):
            rows.append({k: r[k] for k in r})
    return rows


def tbl(rows):
    d = defaultdict(lambda: defaultdict(dict))
    for r in rows:
        ds = r["dataset"]
        col = r["column"].strip().strip('"')
        c = r["codec"]
        entry = {
            "ratio": float(r["ratio"]),
            "N": int(r["N"]),
            "enc_mbs": float(r["encode_mbs"]),
            "dec_mbs": float(r["decode_mbs"]),
            "q_mean": float(r["query_mean_us"]),
            "q_gor": float(r["query_gorilla_us"]),
            "q_lz4": float(r["query_lz4_us"]),
            "q_zstd": float(r["query_zstd_us"]),
        }
        for key in ("query_min_us", "query_max_us", "query_sum_us",
                     "query_count_us", "query_var_us", "query_where_us",
                     "query_value_at_us", "query_scan_us", "query_ds_us"):
            short = key.replace("query_", "q_").replace("_us", "")
            entry[short] = float(r.get(key, 0))
        d[ds][col][c] = entry
    return d


def ds_label(ds_key, data):
    """Build label like 'Jena Climate (420K rows)' with row count from data."""
    base = DS_LABELS.get(ds_key, ds_key)
    cols = data.get(ds_key, {})
    n = 0
    for col_codecs in cols.values():
        s = col_codecs.get("striq-e0.01", {})
        if s.get("N", 0) > n:
            n = s["N"]
    if n >= 1000:
        label = f"{n/1000:.0f}K" if n < 1_000_000 else f"{n/1_000_000:.1f}M"
    else:
        label = str(n)
    return f"{base} ({label} rows)"


DS_ORDER = ["jena_climate_2009_2016", "household_power_consumption",
            "metro_traffic", "noaa_gsod"]


def plot_summary_table(data):
    header = ["Dataset", "Signal", "STRIQ", "Gorilla", "LZ4", "Zstd-3",
              "STRIQ query", "Gorilla query", "Speedup"]
    cells = [[] for _ in header]

    for ds_key in DS_ORDER:
        cols = PICKS.get(ds_key, [])
        label = ds_label(ds_key, data)
        for col in cols:
            codecs = data.get(ds_key, {}).get(col, {})
            s = codecs.get("striq-e0.01", {})
            g = codecs.get("gorilla", {})
            l = codecs.get("lz4", {})
            z = codecs.get("zstd-3", {})
            sq = s.get("q_mean", 0)
            gq = s.get("q_gor", 0) or g.get("q_gor", 0)
            if gq < 1.0: gq = 0
            speedup = f"{gq/sq:,.0f}x" if sq > 0 and gq > 0 else "—"
            cells[0].append(label)
            cells[1].append(col)
            cells[2].append(f"{s.get('ratio',0):.2f}x")
            cells[3].append(f"{g.get('ratio',0):.2f}x")
            cells[4].append(f"{l.get('ratio',0):.2f}x")
            cells[5].append(f"{z.get('ratio',0):.2f}x")
            cells[6].append(f"{sq:.1f} us")
            cells[7].append(f"{gq:,.0f} us" if gq > 0 else "—")
            cells[8].append(speedup)

    fill_colors = []
    for i in range(len(header)):
        if i in (6, 8):
            fill_colors.append(["#EFF6FF"] * len(cells[0]))
        else:
            fill_colors.append(["white"] * len(cells[0]))

    fig = go.Figure(go.Table(
        header=dict(
            values=[f"<b>{h}</b>" for h in header],
            fill_color="#2563EB", font=dict(color="white", size=12),
            align="center", height=32,
        ),
        cells=dict(
            values=cells,
            fill_color=fill_colors,
            font=dict(size=11),
            align=["left", "left"] + ["center"] * 7,
            height=26,
        ),
    ))
    fig.update_layout(width=1100, height=max(len(cells[0]) * 42 + 150, 500),
                      margin=dict(l=5, r=5, t=30, b=5),
                      title=dict(text="<b>STRIQ vs Gorilla vs LZ4 vs Zstd — Per-Column Comparison</b>",
                                 x=0.5, xanchor="center", font=dict(size=14)))

    out = PLOTS / "summary_table.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_dataset_summary(data):
    header = ["Dataset", "Rows", "Signals", "Avg STRIQ Ratio",
              "Avg Query (us)", "Avg Gorilla Query (us)", "Avg Speedup"]
    cells = [[] for _ in header]

    for ds_key in DS_ORDER:
        label = DS_LABELS.get(ds_key, ds_key)
        cols = PICKS.get(ds_key, [])
        ratios, sq_list, gq_list = [], [], []
        n = 0
        for col in cols:
            codecs = data.get(ds_key, {}).get(col, {})
            s = codecs.get("striq-e0.01", {})
            if s.get("N", 0) > n:
                n = s["N"]
            ratios.append(s.get("ratio", 0))
            sq_list.append(s.get("q_mean", 0))
            gq = s.get("q_gor", 0) or codecs.get("gorilla", {}).get("q_gor", 0)
            if gq < 1.0: gq = 0
            gq_list.append(gq)

        avg_r = sum(ratios) / len(ratios) if ratios else 0
        avg_sq = sum(sq_list) / len(sq_list) if sq_list else 0
        valid_gq = [g for g in gq_list if g > 0]
        avg_gq = sum(valid_gq) / len(valid_gq) if valid_gq else 0
        avg_sp = avg_gq / avg_sq if avg_sq > 0 and avg_gq > 0 else 0

        cells[0].append(f"<b>{label}</b>")
        cells[1].append(f"{n:,}")
        cells[2].append(str(len(cols)))
        cells[3].append(f"{avg_r:.2f}x")
        cells[4].append(f"{avg_sq:.1f}")
        cells[5].append(f"{avg_gq:,.0f}" if avg_gq > 0 else "—")
        cells[6].append(f"<b>{avg_sp:,.0f}x</b>" if avg_sp > 0 else "—")

    fig = go.Figure(go.Table(
        header=dict(
            values=[f"<b>{h}</b>" for h in header],
            fill_color="#2563EB", font=dict(color="white", size=13),
            align="center", height=34,
        ),
        cells=dict(
            values=cells,
            fill_color=[["white"] * len(cells[0])] * 5 + [["#EFF6FF"] * len(cells[0])] * 2,
            font=dict(size=12),
            align=["left"] + ["center"] * 6,
            height=30,
        ),
    ))
    fig.update_layout(width=900, height=260,
                      margin=dict(l=5, r=5, t=30, b=5),
                      title=dict(text="<b>Per-Dataset Summary</b>",
                                 x=0.5, xanchor="center", font=dict(size=14)))

    out = PLOTS / "dataset_summary.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_query_speedup(data):
    labels, sq_list, gq_list, lq_list, zq_list = [], [], [], [], []

    for ds_key in DS_ORDER:
        for col in PICKS.get(ds_key, []):
            codecs = data.get(ds_key, {}).get(col, {})
            s = codecs.get("striq-e0.01", {})
            sq = s.get("q_mean", 0)
            if sq <= 0: continue
            gq = s.get("q_gor", 0) or codecs.get("gorilla", {}).get("q_gor", 0)
            lq = s.get("q_lz4", 0) or codecs.get("lz4", {}).get("q_lz4", 0)
            zq = s.get("q_zstd", 0) or codecs.get("zstd-3", {}).get("q_zstd", 0)
            if gq < 1.0: gq = 0
            if lq < 1.0: lq = 0
            if zq < 1.0: zq = 0
            if gq <= 0 and lq <= 0: continue
            ds_short = DS_LABELS.get(ds_key, ds_key)
            labels.append(f"{ds_short} / <b>{col}</b>")
            sq_list.append(sq)
            gq_list.append(gq if gq > 0 else None)
            lq_list.append(lq if lq > 0 else None)
            zq_list.append(zq if zq > 0 else None)

    fig = go.Figure()
    fig.add_trace(go.Bar(name="STRIQ algebraic", y=labels, x=sq_list,
        orientation="h", marker_color=BLUE,
        text=[f"<b>{v:.0f} us</b>" for v in sq_list],
        textposition="outside", textfont=dict(size=10, color=BLUE)))
    fig.add_trace(go.Bar(name="Gorilla decode+scan", y=labels,
        x=[v or 0 for v in gq_list], orientation="h", marker_color=ORANGE))
    fig.add_trace(go.Bar(name="LZ4 decode+scan", y=labels,
        x=[v or 0 for v in lq_list], orientation="h", marker_color=GREEN))
    fig.add_trace(go.Bar(name="Zstd decode+scan", y=labels,
        x=[v or 0 for v in zq_list], orientation="h", marker_color=PURPLE))

    fig.update_layout(**L, barmode="group",
        title=dict(text="<b>query_mean() Latency</b>",
                   x=0.5, xanchor="center"),
        xaxis=dict(title="Latency (us, log scale)",
                   type="log", gridcolor="#E0E0E0", range=[0, 5]),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", y=1.07, x=0.5, xanchor="center", font=dict(size=12)),
        width=950, height=520, bargap=0.22, bargroupgap=0.06)

    out = PLOTS / "query_speedup.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_compression(data):
    codecs = [("striq-e0.01", "STRIQ", BLUE), ("gorilla", "Gorilla", ORANGE),
              ("lz4", "LZ4", GREEN), ("zstd-3", "Zstd-3", PURPLE)]
    ds_keys = list(PICKS.keys())
    titles = [DS_LABELS[k] for k in ds_keys]
    fig = make_subplots(rows=2, cols=2, horizontal_spacing=0.12, vertical_spacing=0.2,
                        subplot_titles=titles)
    pos = [(1,1),(1,2),(2,1),(2,2)]
    for pi, ds_key in enumerate(ds_keys):
        r, c = pos[pi]
        for codec_key, label, color in codecs:
            ratios = []
            for col in PICKS[ds_key]:
                v = data.get(ds_key,{}).get(col,{}).get(codec_key,{}).get("ratio",0)
                ratios.append(min(v, 20))
            fig.add_trace(go.Bar(x=PICKS[ds_key], y=ratios, name=label,
                marker_color=color, opacity=0.88,
                showlegend=(pi==0), legendgroup=codec_key), row=r, col=c)
    fig.update_layout(**L, barmode="group",
        title=dict(text="<b>Compression Ratio by Dataset</b>",
                   x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.06, x=0.5, xanchor="center", font=dict(size=12)),
        width=1000, height=680)
    for i in range(1, 5):
        fig.update_yaxes(title_text="Ratio (x)" if i in [1,3] else None,
                         range=[0, 15], row=pos[i-1][0], col=pos[i-1][1])
    fig.update_xaxes(tickangle=20, tickfont=dict(size=10))

    out = PLOTS / "compression_ratio.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_encode_throughput(data):
    labels, s_enc, g_enc, l_enc = [], [], [], []

    for ds_key in ["jena_climate_2009_2016", "household_power_consumption", "metro_traffic"]:
        for col in PICKS[ds_key][:3]:
            codecs = data.get(ds_key, {}).get(col, {})
            s = codecs.get("striq-e0.01", {})
            g = codecs.get("gorilla", {})
            lz = codecs.get("lz4", {})
            ds_short = DS_LABELS[ds_key]
            labels.append(f"{ds_short} / {col}")
            s_enc.append(s.get("enc_mbs", 0))
            g_enc.append(g.get("enc_mbs", 0))
            l_enc.append(lz.get("enc_mbs", 0))

    fig = go.Figure()
    fig.add_trace(go.Bar(y=labels, x=s_enc, name="STRIQ", orientation="h", marker_color=BLUE))
    fig.add_trace(go.Bar(y=labels, x=g_enc, name="Gorilla", orientation="h", marker_color=ORANGE))
    fig.add_trace(go.Bar(y=labels, x=l_enc, name="LZ4", orientation="h", marker_color=GREEN))

    fig.update_layout(**L, barmode="group",
        title=dict(text="<b>Encode Throughput</b>", x=0.5, xanchor="center"),
        xaxis=dict(title="MB/s"),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", y=1.07, x=0.5, xanchor="center", font=dict(size=12)),
        width=900, height=450, bargap=0.2, bargroupgap=0.05)

    out = PLOTS / "encode_throughput.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_speedup_factor(data):
    labels, speedups = [], []

    for ds_key in DS_ORDER:
        for col in PICKS.get(ds_key, []):
            codecs = data.get(ds_key, {}).get(col, {})
            s = codecs.get("striq-e0.01", {})
            sq = s.get("q_mean", 0)
            gq = s.get("q_gor", 0) or codecs.get("gorilla", {}).get("q_gor", 0)
            if gq < 1.0: gq = 0
            if sq <= 0 or gq <= 0: continue
            factor = gq / sq
            if factor < 2: continue
            ds_short = DS_LABELS[ds_key]
            labels.append(f"{ds_short} / <b>{col}</b>")
            speedups.append(factor)

    colors = [BLUE if s >= 100 else "#93C5FD" for s in speedups]

    fig = go.Figure(go.Bar(
        y=labels, x=speedups, orientation="h",
        marker_color=colors,
        text=[f"<b>{v:,.0f}x</b>" for v in speedups],
        textposition="outside", textfont=dict(size=11),
    ))
    fig.update_layout(**L,
        title=dict(text="<b>STRIQ query_mean() Speedup vs Gorilla</b>",
                   x=0.5, xanchor="center", font=dict(size=13)),
        xaxis=dict(title="Speedup factor (x, log scale)",
                   type="log", gridcolor="#E0E0E0"),
        yaxis=dict(autorange="reversed"),
        width=900, height=480, bargap=0.3)

    out = PLOTS / "speedup_factor.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_epsilon():
    if not EPS_F.exists():
        print("  No epsilon.csv — skipping")
        return
    rows = []
    with open(EPS_F) as f:
        for r in csv.DictReader(f):
            ep = float(r["epsilon_pct"])
            if ep == 0.0: continue
            rows.append({"col": r["column"].strip().strip('"'),
                         "eps": ep, "ratio": float(r["ratio"]),
                         "query": float(r["query_mean_us"])})
    if not rows: return

    target = ["p (mbar)", "T (degC)", "rh (%)", "H2OC (mmol/mol)"]
    colors = [BLUE, RED, GREEN, ORANGE]
    by_col = defaultdict(list)
    for r in rows:
        by_col[r["col"]].append(r)

    fig = make_subplots(rows=1, cols=2,
        subplot_titles=["Compression Ratio vs Epsilon", "Query Latency vs Epsilon"])
    for i, col in enumerate(target):
        pts = sorted(by_col.get(col, []), key=lambda x: x["eps"])
        if not pts: continue
        xs = [p["eps"] for p in pts]
        fig.add_trace(go.Scatter(x=xs, y=[p["ratio"] for p in pts],
            mode="lines+markers", name=col, line=dict(color=colors[i], width=2.5),
            marker=dict(size=8)), row=1, col=1)
        fig.add_trace(go.Scatter(x=xs, y=[p["query"] for p in pts],
            mode="lines+markers", name=col, line=dict(color=colors[i], width=2.5),
            marker=dict(size=8), showlegend=False), row=1, col=2)

    fig.update_xaxes(title_text="Epsilon (% of signal range)", type="log")
    fig.update_yaxes(title_text="Compression Ratio", row=1, col=1)
    fig.update_yaxes(title_text="query_mean (us)", row=1, col=2)
    fig.update_layout(**L, legend=dict(orientation="h", y=-0.15, x=0.5, xanchor="center",
        font=dict(size=12)), width=1000, height=420)

    out = PLOTS / "epsilon_tradeoff.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


import random

def plot_pla_concept():
    """PLA shrinking-cone: signal + epsilon band + fitted segments."""
    random.seed(42)
    N = 200
    xs = list(range(N))
    signal = [20 + 8*math.sin(x/25) + 3*math.sin(x/7) + random.gauss(0, 0.3) for x in xs]
    eps = 1.5

    segs = _pla_fit(signal, eps)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=xs, y=[s + eps for s in signal],
        mode="lines", line=dict(width=0), showlegend=False))
    fig.add_trace(go.Scatter(x=xs, y=[s - eps for s in signal],
        mode="lines", line=dict(width=0), fill="tonexty",
        fillcolor="rgba(37,99,235,0.1)", showlegend=False))
    fig.add_trace(go.Scatter(x=xs, y=signal, mode="lines",
        line=dict(color="#94A3B8", width=1), name="Raw signal"))

    colors = ["#2563EB", "#F97316", "#22C55E", "#A855F7", "#EF4444",
              "#06B6D4", "#D946EF", "#F59E0B"]
    for idx, (st, ln, off, sl) in enumerate(segs):
        seg_x = list(range(st, st + ln))
        seg_y = [off + sl * (t - st) for t in seg_x]
        fig.add_trace(go.Scatter(x=seg_x, y=seg_y, mode="lines",
            line=dict(color=colors[idx % len(colors)], width=3),
            showlegend=(idx == 0), name="PLA segments"))

    for idx, (st, ln, off, sl) in enumerate(segs[:-1]):
        fig.add_vline(x=st + ln - 0.5, line_dash="dot",
                      line_color="#CBD5E1", line_width=1)

    fig.update_layout(
        template="plotly_white",
        font=dict(family="Inter, system-ui, sans-serif", size=13),
        title=dict(text="<b>PLA Encoding — Shrinking Cone fits linear segments within ε</b>",
                   x=0.5, xanchor="center"),
        xaxis=dict(title="Sample index", gridcolor="#F1F5F9"),
        yaxis=dict(title="Value", gridcolor="#F1F5F9"),
        legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
        width=900, height=400, margin=dict(l=60, r=30, t=60, b=50),
        annotations=[dict(x=100, y=max(signal)+2.5, text=f"ε = ±{eps}  →  {len(segs)} segments for {N} points",
                          showarrow=False, font=dict(size=12, color="#64748B"))]
    )
    out = PLOTS / "pla_concept.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_encoding_pipeline():
    """Architecture: encoding pipeline flow diagram."""
    fig = go.Figure()
    fig.update_xaxes(range=[0, 10], visible=False)
    fig.update_yaxes(range=[0, 6], visible=False)

    boxes = [
        (0.3, 4.2, 1.8, 1.2, "Raw CSV\nrows", "#F1F5F9", "#64748B"),
        (2.5, 4.2, 1.8, 1.2, "Codec\nRouter", "#DBEAFE", "#2563EB"),
        (5.0, 5.0, 1.6, 0.8, "PLA /\nChebyshev", "#D1FAE5", "#059669"),
        (5.0, 3.8, 1.6, 0.8, "Decimal /\nRLE", "#FEF3C7", "#D97706"),
        (5.0, 2.6, 1.6, 0.8, "RAW_STATS", "#FEE2E2", "#DC2626"),
        (7.4, 4.2, 2.2, 1.2, "Block +\nStats header", "#EDE9FE", "#7C3AED"),
    ]
    for x, y, w, h, txt, bg, border in boxes:
        fig.add_shape(type="rect", x0=x, y0=y, x1=x+w, y1=y+h,
                      fillcolor=bg, line=dict(color=border, width=2),
                      layer="below")
        fig.add_annotation(x=x+w/2, y=y+h/2, text=f"<b>{txt}</b>",
                           showarrow=False, font=dict(size=11, color=border))

    arrows = [
        (2.1, 4.8, 2.5, 4.8),
        (4.3, 4.8, 5.0, 5.4),
        (4.3, 4.8, 5.0, 4.2),
        (4.3, 4.8, 5.0, 3.0),
        (6.6, 5.4, 7.4, 5.0),
        (6.6, 4.2, 7.4, 4.8),
        (6.6, 3.0, 7.4, 4.5),
    ]
    for x0, y0, x1, y1 in arrows:
        fig.add_annotation(x=x1, y=y1, ax=x0, ay=y0, xref="x", yref="y",
                           axref="x", ayref="y", showarrow=True,
                           arrowhead=2, arrowsize=1.2, arrowwidth=2,
                           arrowcolor="#94A3B8")

    fig.add_annotation(x=5, y=1.8,
        text="Each block stores <b>min, max, sum, count</b> per column — enabling algebraic queries without decompression",
        showarrow=False, font=dict(size=11, color="#475569"),
        bgcolor="#F8FAFC", bordercolor="#E2E8F0", borderwidth=1, borderpad=6)

    fig.add_annotation(x=3.4, y=3.6,
        text="Per-column trial fit:<br>PLA → Decimal → RLE → RAW",
        showarrow=False, font=dict(size=9, color="#94A3B8"))

    fig.update_layout(template="plotly_white", width=900, height=380,
        title=dict(text="<b>STRIQ Encoding Pipeline</b>", x=0.5, xanchor="center",
                   font=dict(family="Inter, system-ui, sans-serif", size=14)),
        margin=dict(l=10, r=10, t=50, b=10), plot_bgcolor="white")
    out = PLOTS / "encoding_pipeline.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_query_concept():
    """Comparison: traditional scan vs STRIQ algebraic query."""
    fig = make_subplots(rows=1, cols=2, subplot_titles=[
        "<b>Traditional: decompress → scan</b>",
        "<b>STRIQ: read block stats → algebra</b>"],
        horizontal_spacing=0.08)

    n_blocks = 6
    for b in range(n_blocks):
        fig.add_shape(type="rect", x0=b*1.2, y0=0, x1=b*1.2+1, y1=1,
                      fillcolor="#FEE2E2", line=dict(color="#EF4444", width=2),
                      row=1, col=1)
        fig.add_annotation(x=b*1.2+0.5, y=0.5, text=f"Block {b+1}<br><i>decode all</i>",
                           showarrow=False, font=dict(size=8, color="#991B1B"),
                           row=1, col=1)

    for b in range(n_blocks):
        fig.add_shape(type="rect", x0=b*1.2, y0=0, x1=b*1.2+1, y1=0.3,
                      fillcolor="#D1FAE5", line=dict(color="#059669", width=2),
                      row=1, col=2)
        fig.add_annotation(x=b*1.2+0.5, y=0.15, text="stats",
                           showarrow=False, font=dict(size=8, color="#065F46"),
                           row=1, col=2)
        fig.add_shape(type="rect", x0=b*1.2, y0=0.35, x1=b*1.2+1, y1=1,
                      fillcolor="#F1F5F9", line=dict(color="#CBD5E1", width=1),
                      row=1, col=2)
        fig.add_annotation(x=b*1.2+0.5, y=0.67, text="data<br><i>skip</i>",
                           showarrow=False, font=dict(size=8, color="#94A3B8"),
                           row=1, col=2)

    fig.add_annotation(x=3.5, y=-0.3, text="Read every byte → O(N)",
                       showarrow=False, font=dict(size=11, color="#DC2626", family="Inter"),
                       xref="x", yref="y")
    fig.add_annotation(x=3.5, y=-0.3, text="Read only headers → O(blocks)",
                       showarrow=False, font=dict(size=11, color="#059669", family="Inter"),
                       xref="x2", yref="y2")

    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(template="plotly_white", width=950, height=300,
        title=dict(text="<b>Why STRIQ queries are fast: algebraic aggregates skip the data</b>",
                   x=0.5, xanchor="center",
                   font=dict(family="Inter, system-ui, sans-serif", size=14)),
        margin=dict(l=20, r=20, t=70, b=50), showlegend=False)
    out = PLOTS / "query_concept.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def _pla_fit(signal, eps):
    """Run shrinking-cone PLA and return segment list [(start, length, offset, slope)]."""
    N = len(signal)
    segs = []
    i = 0
    while i < N:
        start = i
        offset = signal[i]
        s_lo, s_hi = -1e9, 1e9
        j = i + 1
        while j < N:
            dt = j - start
            s_hi = min(s_hi, (signal[j] + eps - offset) / dt)
            s_lo = max(s_lo, (signal[j] - eps - offset) / dt)
            if s_lo > s_hi:
                break
            j += 1
        slope = (s_lo + s_hi) / 2
        segs.append((start, j - start, offset, slope))
        i = j
    return segs


def plot_epsilon_in_action():
    """Show the same signal compressed at 3 epsilon levels side by side."""
    random.seed(7)
    N = 300
    xs = list(range(N))
    signal = [18 + 10*math.sin(x/30) + 4*math.sin(x/9) + 1.5*math.cos(x/50)
              + random.gauss(0, 0.4) for x in xs]

    epsilons = [0.5, 2.0, 6.0]
    colors = ["#2563EB", "#F97316", "#22C55E"]
    labels = ["Tight (ε = 0.5)", "Moderate (ε = 2.0)", "Loose (ε = 6.0)"]

    fig = make_subplots(rows=1, cols=3, subplot_titles=[
        f"<b>{lbl}</b>" for lbl in labels], horizontal_spacing=0.06)

    for ci, (eps, clr) in enumerate(zip(epsilons, colors), 1):
        segs = _pla_fit(signal, eps)
        recon = [0.0] * N
        for st, ln, off, sl in segs:
            for t in range(st, st + ln):
                recon[t] = off + sl * (t - st)

        fig.add_trace(go.Scatter(x=xs, y=signal, mode="lines",
            line=dict(color="#CBD5E1", width=1), showlegend=(ci==1),
            name="Original"), row=1, col=ci)

        fig.add_trace(go.Scatter(x=xs, y=recon, mode="lines",
            line=dict(color=clr, width=2.5), showlegend=(ci==1),
            name="Reconstructed"), row=1, col=ci)

        raw_bytes = N * 8
        seg_bytes = len(segs) * 18
        ratio = raw_bytes / seg_bytes if seg_bytes > 0 else 0
        max_err = max(abs(signal[i] - recon[i]) for i in range(N))

        fig.add_annotation(x=N/2, y=min(signal)-3.5,
            text=f"<b>{len(segs)}</b> segments · <b>{ratio:.1f}x</b> ratio · max err <b>{max_err:.2f}</b>",
            showarrow=False, font=dict(size=10, color="#475569"),
            row=1, col=ci)

    fig.update_layout(
        template="plotly_white",
        font=dict(family="Inter, system-ui, sans-serif", size=12),
        title=dict(text="<b>Epsilon controls the accuracy–compression tradeoff</b>",
                   x=0.5, xanchor="center"),
        legend=dict(orientation="h", y=1.02, x=0.01, xanchor="left", font=dict(size=11)),
        width=1100, height=370, margin=dict(l=50, r=20, t=80, b=50))
    fig.update_yaxes(title_text="Value", row=1, col=1)
    for c in range(1, 4):
        fig.update_xaxes(title_text="Sample", row=1, col=c)

    out = PLOTS / "epsilon_in_action.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_query_operations(data):
    """Bar chart: latency of every STRIQ query operation across datasets."""
    ops = [
        ("q_mean",     "mean()"),
        ("q_min",      "min()"),
        ("q_max",      "max()"),
        ("q_sum",      "sum()"),
        ("q_count",    "count()"),
        ("q_var",      "variance()"),
        ("q_where",    "mean_where()"),
        ("q_ds",       "downsample()"),
        ("q_value_at", "value_at()"),
        ("q_scan",     "scan()"),
    ]
    colors = [BLUE, "#06B6D4", "#0EA5E9", "#8B5CF6", GREEN,
              ORANGE, RED, PURPLE, "#D946EF", "#F59E0B"]

    ds_keys = [k for k in DS_ORDER if k in data]
    if not ds_keys:
        return

    fig = make_subplots(
        rows=len(ds_keys), cols=1,
        subplot_titles=[ds_label(k, data) for k in ds_keys],
        vertical_spacing=0.08)

    for di, ds_key in enumerate(ds_keys, 1):
        cols_picked = PICKS.get(ds_key, list(data[ds_key].keys())[:4])
        avg_latencies = []
        for op_key, op_label in ops:
            vals = []
            for col in cols_picked:
                s = data[ds_key].get(col, {}).get("striq-e0.01", {})
                v = s.get(op_key, 0)
                if v > 0:
                    vals.append(v)
            avg_latencies.append(sum(vals) / len(vals) if vals else 0)

        op_labels = [o[1] for o in ops]
        bar_colors = [colors[i] for i in range(len(ops))]

        fig.add_trace(go.Bar(
            x=op_labels, y=avg_latencies,
            marker_color=bar_colors,
            text=[f"{v:.1f}" if v > 0 else "" for v in avg_latencies],
            textposition="outside", textfont=dict(size=9),
            showlegend=False,
        ), row=di, col=1)
        fig.update_yaxes(title_text="Latency (µs)", row=di, col=1)

    fig.update_layout(
        **L,
        title=dict(text="<b>STRIQ Query Latency by Operation (all algebraic, no decompression)</b>",
                   x=0.5, xanchor="center"),
        width=1000, height=250 * len(ds_keys) + 100,
        bargap=0.3)
    fig.update_xaxes(tickangle=25, tickfont=dict(size=10))

    out = PLOTS / "query_operations.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


def plot_query_ops_vs_competitors(data):
    """STRIQ algebraic ops vs Gorilla/LZ4/Zstd decode+scan — horizontal bars."""
    ops = [
        ("q_mean",  "mean()"),
        ("q_min",   "min()"),
        ("q_max",   "max()"),
        ("q_sum",   "sum()"),
        ("q_count", "count()"),
        ("q_var",   "variance()"),
        ("q_where", "mean_where()"),
        ("q_ds",    "downsample()"),
        ("q_value_at", "value_at()"),
        ("q_scan",  "scan()"),
    ]

    q_key_map = {"gorilla": "q_gor", "lz4": "q_lz4", "zstd-3": "q_zstd"}
    competitor_avgs = {}
    for codec_key, label in [("gorilla", "Gorilla"), ("lz4", "LZ4"), ("zstd-3", "Zstd-3")]:
        all_vals = []
        q_key = q_key_map[codec_key]
        for ds_key in DS_ORDER:
            if ds_key not in data:
                continue
            for col in PICKS.get(ds_key, []):
                codecs = data[ds_key].get(col, {})
                v = codecs.get(codec_key, {}).get(q_key, 0)
                if v > 1.0:
                    all_vals.append(v)
        competitor_avgs[label] = sum(all_vals) / len(all_vals) if all_vals else 0

    striq_avgs = []
    for op_key, _ in ops:
        all_vals = []
        for ds_key in DS_ORDER:
            if ds_key not in data:
                continue
            for col in PICKS.get(ds_key, []):
                s = data[ds_key].get(col, {}).get("striq-e0.01", {})
                v = s.get(op_key, 0)
                if v > 0:
                    all_vals.append(v)
        striq_avgs.append(sum(all_vals) / len(all_vals) if all_vals else 0)

    op_labels = [o[1] for o in ops]
    op_labels.reverse()
    striq_avgs.reverse()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=op_labels, x=striq_avgs,
        orientation="h", name="STRIQ (algebraic)",
        marker_color=BLUE,
        text=[f"<b>{v:.1f} µs</b>" for v in striq_avgs],
        textposition="outside", textfont=dict(size=10, color=BLUE)))

    comp_colors = {"Gorilla": ORANGE, "LZ4": GREEN, "Zstd-3": PURPLE}
    comp_y_offset = {"Gorilla": 0.98, "LZ4": 0.85, "Zstd-3": 0.72}
    for label, avg in competitor_avgs.items():
        if avg > 0:
            fig.add_vline(x=avg, line_dash="dash", line_color=comp_colors[label],
                          line_width=2.5)
            fig.add_annotation(
                x=math.log10(avg), y=comp_y_offset[label], xref="x", yref="paper",
                text=f"<b>{label}</b> decode+scan: {avg:,.0f} µs",
                showarrow=False, font=dict(size=11, color=comp_colors[label]),
                xanchor="right", xshift=-6,
                bgcolor="rgba(255,255,255,0.85)", borderpad=2)

    fig.update_layout(
        **L,
        title=dict(
            text="<b>STRIQ Algebraic Query Latency vs Traditional decode+scan</b><br>"
                 "<sup>Vertical lines = competitor cost for a single mean() (must decompress all rows)</sup>",
            x=0.5, xanchor="center"),
        xaxis=dict(title="Latency (µs, log scale)", type="log",
                   gridcolor="#E0E0E0", range=[-1.2, 5]),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center", font=dict(size=12)),
        width=1050, height=520, bargap=0.3)

    out = PLOTS / "query_ops_vs_competitors.png"
    fig.write_image(str(out), scale=2)
    print(f"  {out}")


if __name__ == "__main__":
    if not CSV_F.exists():
        print(f"No {CSV_F} — run: make bench"); sys.exit(1)
    rows = load()
    data = tbl(rows)
    print(f"Loaded {len(rows)} rows\nGenerating:")
    plot_summary_table(data)
    plot_dataset_summary(data)
    plot_query_speedup(data)
    plot_speedup_factor(data)
    plot_compression(data)
    plot_encode_throughput(data)
    plot_epsilon()
    plot_pla_concept()
    plot_encoding_pipeline()
    plot_query_concept()
    plot_epsilon_in_action()
    plot_query_operations(data)
    plot_query_ops_vs_competitors(data)
    print(f"\nDone — {PLOTS}/")
