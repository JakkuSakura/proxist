#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
data_dir="${root_dir}/data"

mkdir -p "${data_dir}"

curl -L "https://docs.stacresearch.com/system/files/central/STAC-M3_Overview.pdf" \
  -o "${data_dir}/STAC-M3_Overview.pdf"

curl -L "https://stacresearch.com/news/stac-m3-benchmark-results-kx-kdb-4-1-on-supermicro-micron-intel/" \
  -o "${data_dir}/stac-m3-kx-kdb-4.1-supermicro-micron-intel-2025-10-27.html"
