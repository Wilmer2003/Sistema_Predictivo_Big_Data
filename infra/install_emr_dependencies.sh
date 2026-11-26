#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Uso: bash $0 REQUIREMENTS_TXT RUTA_ABSOLUTA_VENV" >&2
    exit 2
fi

requirements=$1
venv=$2
test -r "$requirements"
[[ "$venv" = /* ]]

"${EMR_PYTHON:-python3}" -m venv "$venv"
# EMR proporciona PySpark. Las dependencias nativas se instalan en cada nodo.
"$venv/bin/python" -m pip install -r <(sed -E '/^[[:space:]]*pyspark[=<>!~;]/d' "$requirements")
"$venv/bin/python" -m pip check
"$venv/bin/python" -c 'import psycopg2'
