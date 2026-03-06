#!/usr/bin/env bash
# Download all benchmark datasets for striq_codec.
# Each dataset is skipped if its output file already exists.
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

TOTAL=4
OK=0

# ---------------------------------------------------------------------------
# 1. Jena Climate  (420K rows, 14 float columns)
#    Source: Google / TensorFlow public datasets
# ---------------------------------------------------------------------------
echo "=== [1/$TOTAL] Jena Climate ==="
JENA="jena_climate_2009_2016.csv"
if [ -f "$JENA" ]; then
    echo "  Already present: $JENA — skipping."
else
    echo "  Downloading …"
    curl -sL "https://storage.googleapis.com/tensorflow/tf-keras-datasets/jena_climate_2009_2016.csv.zip" \
        -o /tmp/jena.zip
    unzip -oq /tmp/jena.zip -d "$DIR/"
    rm -f /tmp/jena.zip
    echo "  Done: $JENA"
fi
OK=$((OK + 1))

# ---------------------------------------------------------------------------
# 2. Household Power Consumption  (2M rows, 7 float columns)
#    Source: UCI Machine Learning Repository
#    Semicolon-separated; ZIP contains household_power_consumption.txt
# ---------------------------------------------------------------------------
echo ""
echo "=== [2/$TOTAL] Household Power Consumption ==="
HPC="household_power_consumption.txt"
if [ -f "$HPC" ]; then
    echo "  Already present: $HPC — skipping."
else
    echo "  Downloading …"
    curl -sL "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip" \
        -o /tmp/hpc.zip
    unzip -oq /tmp/hpc.zip -d /tmp/hpc_extract
    # The text file may be at the top level or nested; find it.
    SRC=$(find /tmp/hpc_extract -name "household_power_consumption.txt" | head -1)
    if [ -z "$SRC" ]; then
        echo "  ERROR: household_power_consumption.txt not found in archive." >&2
        exit 1
    fi
    mv "$SRC" "$DIR/$HPC"
    rm -rf /tmp/hpc.zip /tmp/hpc_extract
    echo "  Done: $HPC"
fi
OK=$((OK + 1))

# ---------------------------------------------------------------------------
# 3. NOAA Global Summary of the Day (GSOD)
#    5 major US stations, year 2025 only, concatenated and cleaned.
#    URL format: https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/{year}/{station}.csv
#    Columns kept: STATION, DATE, TEMP, DEWP, SLP, VISIB, WDSP, MXSPD,
#                  MAX, MIN, PRCP
# ---------------------------------------------------------------------------
echo ""
echo "=== [3/$TOTAL] NOAA GSOD ==="
NOAA="noaa_gsod.csv"
if [ -f "$NOAA" ]; then
    echo "  Already present: $NOAA — skipping."
else
    STATIONS="72295023174 74486094789 72530094846 72259003927 72565003017"
    YEARS="2020 2021 2022 2023 2024 2025"
    RAW_DIR="/tmp/noaa_raw"
    mkdir -p "$RAW_DIR"

    echo "  Downloading station CSVs …"
    for YEAR in $YEARS; do
        for STN in $STATIONS; do
            URL="https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/${YEAR}/${STN}.csv"
            DEST="$RAW_DIR/${YEAR}_${STN}.csv"
            if [ ! -f "$DEST" ]; then
                echo "    $YEAR / $STN"
                curl -sL "$URL" -o "$DEST" || {
                    echo "    WARNING: failed to download $URL" >&2
                    rm -f "$DEST"
                }
            fi
        done
    done

    echo "  Concatenating and cleaning …"
    HEADER_FILE=$(ls "$RAW_DIR"/*.csv 2>/dev/null | head -1)
    if [ -z "$HEADER_FILE" ]; then
        echo "  ERROR: no NOAA CSV files downloaded." >&2
        exit 1
    fi

    # Use Python's csv module to handle quoted fields with commas (e.g. NAME).
    python3 -c "
import csv, sys, glob, os
keep = ['STATION','DATE','TEMP','DEWP','SLP','VISIB','WDSP','MXSPD','MAX','MIN','PRCP']
missing = {'999.9','9999.9','99.99'}
raw = sorted(glob.glob(os.path.join('$RAW_DIR', '*.csv')))
out = open('$DIR/$NOAA', 'w', newline='')
w = csv.writer(out)
w.writerow(keep)
for f in raw:
    with open(f) as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            vals = []
            for c in keep:
                v = row.get(c, '').strip()
                if v in missing: v = ''
                vals.append(v)
            w.writerow(vals)
out.close()
print(f'  Processed {len(raw)} station files')
"

    rm -rf "$RAW_DIR"
    echo "  Done: $NOAA"
fi
OK=$((OK + 1))

# ---------------------------------------------------------------------------
# 4. Metro Interstate Traffic Volume  (48K rows, weather floats + traffic)
#    Source: UCI Machine Learning Repository
# ---------------------------------------------------------------------------
echo ""
echo "=== [4/$TOTAL] Metro Interstate Traffic Volume ==="
METRO="metro_traffic.csv"
if [ -f "$METRO" ]; then
    echo "  Already present: $METRO — skipping."
else
    echo "  Downloading …"
    curl -sL "https://archive.ics.uci.edu/static/public/492/metro+interstate+traffic+volume.zip" \
        -o /tmp/metro.zip
    unzip -oq /tmp/metro.zip -d /tmp/metro_extract
    # Archive contains .csv.gz — find and decompress it.
    GZ=$(find /tmp/metro_extract -name "*.csv.gz" -o -name "*.csv" | head -1)
    if [ -z "$GZ" ]; then
        echo "  ERROR: no CSV or CSV.GZ found in metro traffic archive." >&2
        exit 1
    fi
    case "$GZ" in
        *.gz) gunzip -c "$GZ" > "$DIR/$METRO" ;;
        *)    mv "$GZ" "$DIR/$METRO" ;;
    esac
    rm -rf /tmp/metro.zip /tmp/metro_extract
    echo "  Done: $METRO"
fi
OK=$((OK + 1))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo "  Summary: $OK / $TOTAL datasets ready"
echo "========================================"
echo ""
ls -lh "$DIR"/*.csv "$DIR"/*.txt 2>/dev/null | awk '{print "  " $5 "\t" $NF}'
echo ""
echo "All done."
