import sys
import logging
import pandas as pd
import gzip


def unzip_file_get_df(source_file):
    logging.info(f"source_file is...{source_file}")
    with gzip.open(source_file) as f:
        df = pd.read_json(f, lines=True)
        return df
