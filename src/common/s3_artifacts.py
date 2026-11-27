"""Persist small, driver-generated JSON artifacts in Amazon S3."""

import json
import re

from src.common.config import AppConfig


def validate_run_id(run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
        raise ValueError("PIPELINE_RUN_ID debe tener 1–100 caracteres alfanuméricos, _ o -.")
    return run_id


def put_json(config: AppConfig, key: str, payload: dict, *, immutable: bool = False) -> str:
    import boto3
    from botocore.exceptions import ClientError

    if not config.s3_bucket:
        raise ValueError("S3_BUCKET es obligatorio en modo AWS.")

    body = json.dumps(payload, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2).encode("utf-8")
    s3 = boto3.client("s3", region_name=config.aws_region)
    args = {"Bucket": config.s3_bucket, "Key": key}
    try:
        conditions = {"IfNoneMatch": "*"} if immutable else {}
        s3.put_object(**args, **conditions, Body=body, ContentType="application/json")
    except ClientError as exc:
        if not immutable or exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
            raise
        with s3.get_object(**args)["Body"] as existing:
            if existing.read() != body:
                raise ValueError(f"El artefacto {key} ya existe con contenido distinto.") from exc
    return f"s3://{config.s3_bucket}/{key}"
