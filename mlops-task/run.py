import argparse
import json
import logging
import os
import sys
import time
import yaml
import numpy as np
import pandas as pd


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



    main()