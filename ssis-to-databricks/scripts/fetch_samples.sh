#!/usr/bin/env bash
# Fetch the third-party SSIS sample packages this tool was tested against.
#
# These repos are NOT redistributed in this project (see ../ATTRIBUTION.md — some are
# GPL, others carry no license). This script clones them into ./samples/external/ for
# local testing only. Full credit to each author is in ATTRIBUTION.md.
set -euo pipefail

DEST="$(cd "$(dirname "$0")/.." && pwd)/samples/external"
mkdir -p "$DEST"

repos=(
  "https://github.com/GoodmanNeil/SSIS-Examples.git"
  "https://github.com/RanaGaballah/DataWareHouse_SSIS.git"
  "https://github.com/NirmalAndrews/IntegrationServicesSamples.git"
  "https://github.com/Henokagb/ETL-EBusiness-data_SSIS.git"
  "https://github.com/marcelmotta/IMSports-ETL.git"
  "https://github.com/safizaidi98/Inremental-Load-SCD-Merge-Join-Lookup-Knowledge-Star-Project-SSIS.git"
  "https://github.com/niroshank/sttm-dimenisonal-dw-ssis-scd-tutorial.git"
)

for url in "${repos[@]}"; do
  name="$(basename "$url" .git)"
  if [ -d "$DEST/$name" ]; then
    echo "already present: $name"
  else
    echo "cloning: $name"
    git clone --depth 1 "$url" "$DEST/$name"
  fi
done

echo
echo "Done. Third-party .dtsx are under: $DEST"
echo "They are for local testing only — see ATTRIBUTION.md for licenses."
