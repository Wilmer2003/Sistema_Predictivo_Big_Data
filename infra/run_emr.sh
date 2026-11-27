#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "Uso: bash $0 RUTA_ABSOLUTA_VENV PAQUETE_SRC_ZIP FASE [FASE ...]" >&2
    exit 2
fi

venv=$1
archive=$2
shift 2
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

[[ "$venv" = /* ]]
test -x "$venv/bin/python"
test -r "$archive"

: "${S3_BUCKET:?Configura S3_BUCKET}"
: "${POSTGRES_HOST:?Configura POSTGRES_HOST}"
: "${POSTGRES_DB:?Configura POSTGRES_DB}"
: "${POSTGRES_USER:?Configura POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?Inyecta POSTGRES_PASSWORD en el entorno privado del driver}"
export S3_BUCKET POSTGRES_HOST POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
export PIPELINE_ENV=aws SPARK_MASTER=yarn

exec spark-submit --master yarn --deploy-mode client \
    --conf "spark.pyspark.python=$venv/bin/python" \
    --py-files "$archive" \
    "$root/src/orchestration/run_pipeline.py" "$@"
