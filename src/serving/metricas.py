"""Publish and register the actual evaluation report produced by training."""

import argparse
import json
import os
from dataclasses import replace
from datetime import datetime
from pathlib import Path

from src.common.config import AppConfig, get_config
from src.common.logger import get_logger
from src.common.postgres import connect_postgres, postgres_cursor
from src.common.s3_artifacts import put_json, validate_run_id

logger = get_logger(__name__)


def persistir_metricas_aws(config: AppConfig, reporte: dict) -> str:
    if not config.is_aws:
        raise ValueError("La publicación de métricas requiere PIPELINE_ENV=aws.")
    run_id = validate_run_id(config.run_id)
    if reporte.get("run_id", run_id) != run_id:
        raise ValueError("El run_id del reporte no coincide con el de la ejecución.")

    payload = {**reporte, "run_id": run_id}
    split = payload["split_temporal"]
    modelo = payload["metricas_modelo"]
    baseline = payload["metricas_baseline_persistencia"]
    ganancia = payload["ganancia_vs_baseline"]
    fecha = datetime.fromisoformat(payload["timestamp_evaluacion"].replace("Z", "+00:00"))
    if fecha.utcoffset() is None or split["train_max_year"] >= split["test_min_year"]:
        raise ValueError("El reporte debe incluir una fecha con zona horaria y una división temporal válida.")

    valores = (
        run_id, fecha, payload["modelo"], payload["target"],
        split["train_max_year"], split["test_min_year"],
        modelo["rmse"], modelo["mae"], modelo["r2"],
        baseline["rmse"], baseline["mae"], baseline["r2"],
        ganancia["reduccion_rmse_porcentaje"], ganancia["reduccion_mae_porcentaje"],
        ganancia["supera_al_baseline"],
    )
    uri = put_json(
        config, f"results/estadisticas/runs/{run_id}/metricas_evaluacion.json",
        payload, immutable=True,
    )
    logger.info("Reporte de métricas guardado en: %s", uri)

    reporte_json = json.dumps(payload, allow_nan=False)
    with postgres_cursor(connect_postgres(config.postgres)) as cursor:
        cursor.execute("""
            INSERT INTO public.metricas_evaluacion (
                run_id, timestamp_evaluacion, modelo, target, train_max_year, test_min_year,
                rmse, mae, r2, baseline_rmse, baseline_mae, baseline_r2,
                reduccion_rmse_porcentaje, reduccion_mae_porcentaje, supera_al_baseline,
                artifact_uri, reporte
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (run_id) DO NOTHING
            RETURNING run_id
        """, (*valores, uri, reporte_json))
        if cursor.fetchone() is None:
            cursor.execute(
                "SELECT 1 FROM public.metricas_evaluacion "
                "WHERE run_id = %s AND artifact_uri = %s AND reporte = %s::jsonb",
                (run_id, uri, reporte_json),
            )
            if cursor.fetchone() is None:
                raise ValueError(f"Ya existen métricas distintas para run_id={run_id}.")
    logger.info("Métricas registradas en public.metricas_evaluacion para run_id=%s", run_id)
    return uri


def main():
    parser = argparse.ArgumentParser(description="Registra un JSON de evaluación auténtico de AWS.")
    parser.add_argument("reporte", type=Path, help="JSON original recuperado de la ejecución AWS.")
    args = parser.parse_args()
    reporte = json.loads(args.reporte.read_text(encoding="utf-8-sig"))
    run_id = os.getenv("PIPELINE_RUN_ID") or reporte.get("run_id")
    if not run_id:
        parser.error("Define PIPELINE_RUN_ID con un identificador estable de la ejecución original.")
    persistir_metricas_aws(replace(get_config(), run_id=run_id), reporte)


if __name__ == "__main__":
    main()
