#!/usr/bin/env bash
# Upload FHIR export batches to the Unity Catalog volume.
#   scripts/upload_batches.sh 1 30    # first 30 days
#   scripts/upload_batches.sh 31 45   # the rest, later: Auto Loader picks up only these
set -euo pipefail
FIRST=${1:-1}
LAST=${2:-999}
CATALOG=${CATALOG:-clinical_dev}
DEST="dbfs:/Volumes/${CATALOG}/clinical/landing/fhir"

ls -d data/landing/fhir/export_* | sort | sed -n "${FIRST},${LAST}p" | while read -r dir; do
  echo "uploading $(basename "$dir")"
  databricks fs cp -r "$dir" "${DEST}/$(basename "$dir")" --overwrite
done
