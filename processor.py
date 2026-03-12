#!/usr/bin/env python3
import json
import io
import boto3
import pandas as pd
import os
from datetime import datetime

from baseline import BaselineManager
from detector import AnomalyDetector
from logger_config import logger

s3 = boto3.client("s3")

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"] # students configure this


def process_file(bucket: str, key: str):

    logger.info(f"Processing file: {key}")

    # 1. Download raw file
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))

        logger.info(f"Loaded {len(df)} rows")

    except Exception as e:
        logger.error(f"Error loading file {key}: {str(e)}")
        return
    
    # 2. Load current baseline
    try:
        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()

    except Exception as e:
        logger.error(f"Error loading baseline: {str(e)}")
        return

    # 3. Update baseline with values from this batch BEFORE scoring
    #    (use only non-null values for each channel)
    try:
        for col in NUMERIC_COLS:

            if col in df.columns:
                clean_values = df[col].dropna().tolist()

                if clean_values:
                    baseline = baseline_mgr.update(baseline, col, clean_values)

        logger.info("Baseline updated")

    except Exception as e:
        logger.error(f"Error updating baseline: {str(e)}")

    # 4. Run detection
    try:
        detector = AnomalyDetector()
        scored_df = detector.run(df, NUMERIC_COLS, baseline)

    except Exception as e:
        logger.error(f"Error running detector: {str(e)}")
        return
    
    # 5. Write scored file to processed/ prefix
    try:
        output_key = key.replace("raw/", "processed/")

        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )

        logger.info(f"Uploaded processed file {output_key}")

    except Exception as e:
        logger.error(f"Error uploading processed file: {str(e)}")

    # 6. Save updated baseline back to S3
    try:
        baseline_mgr.save(baseline)
        logger.info("Baseline saved")
        upload_logs(bucket)

    except Exception as e:
        logger.error(f"Error saving baseline: {str(e)}")
    
    # 7. Build and return a processing summary
    anomaly_count = int(scored_df["anomaly"].sum()) if "anomaly" in scored_df else 0
    summary = {
        "source_key": key,
        "output_key": output_key,
        "processed_at": datetime.utcnow().isoformat(),
        "total_rows": len(df),
        "anomaly_count": anomaly_count,
        "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
        "baseline_observation_counts": {
            col: baseline.get(col, {}).get("count", 0) for col in NUMERIC_COLS
        }
    }

    # Write summary JSON alongside the processed file
    summary_key = output_key.replace(".csv", "_summary.json")
    s3.put_object(
        Bucket=bucket,
        Key=summary_key,
        Body=json.dumps(summary, indent=2),
        ContentType="application/json"
    )

    print(f"  Done: {anomaly_count}/{len(df)} anomalies flagged")
    return summary

def upload_logs(bucket: str):
    try:
        if os.path.exists("logs/app.log"):
            s3.upload_file(
                "logs/app.log",
                bucket,
                "logs/app.log"
            )
            logger.info("Log file synced to S3")
    except Exception as e:
        logger.error(f"Failed to upload logs: {str(e)}")