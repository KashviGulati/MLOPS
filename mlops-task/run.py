import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import pandas as pd
import yaml


REQUIRED_CONFIG_FIELDS = ["seed", "window", "version"]


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def write_metrics(output_path, metrics):
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2)


def validate_config(config):
    for field in REQUIRED_CONFIG_FIELDS:
        if field not in config:
            raise ValueError(f"Missing required config field: {field}")

    if not isinstance(config["window"], int) or config["window"] <= 0:
        raise ValueError("window must be a positive integer")


def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if not isinstance(config, dict):
        raise ValueError("Invalid config structure")

    validate_config(config)
    return config


def load_dataset(input_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    if os.path.getsize(input_path) == 0:
        raise ValueError("Input CSV file is empty")

    try:
        df = pd.read_csv(input_path)
    except Exception:
        raise ValueError("Invalid CSV format")

    if df.empty:
        raise ValueError("CSV contains no rows")

    if "close" not in df.columns:
        raise ValueError("Missing required column: close")

    return df


def process_data(df, window):
    logging.info("Computing rolling mean")

    df["rolling_mean"] = df["close"].rolling(window=window).mean()

    logging.info("Generating signals")

    df["signal"] = np.where(
        df["rolling_mean"].isna(),
        np.nan,
        np.where(df["close"] > df["rolling_mean"], 1, 0)
    )

    return df


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    setup_logging(args.log_file)

    start_time = time.time()

    logging.info("Job started")

    metrics = {
        "version": "unknown",
        "status": "error",
        "error_message": "Unknown error"
    }

    try:
        config = load_config(args.config)

        version = config["version"]
        seed = config["seed"]
        window = config["window"]

        metrics["version"] = version

        logging.info(
            f"Config loaded | seed={seed}, window={window}, version={version}"
        )

        np.random.seed(seed)

        df = load_dataset(args.input)

        logging.info(f"Rows loaded: {len(df)}")

        df = process_data(df, window)

        valid_signals = df["signal"].dropna()

        signal_rate = float(valid_signals.mean())

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": int(len(df)),
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        logging.info(f"Metrics summary: {metrics}")
        logging.info("Job completed successfully")

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=2))

        sys.exit(0)

    except Exception as e:
        logging.exception("Pipeline failed")

        metrics["status"] = "error"
        metrics["error_message"] = str(e)

        write_metrics(args.output, metrics)

        print(json.dumps(metrics, indent=2))

        sys.exit(1)


if __name__ == "__main__":
    main()