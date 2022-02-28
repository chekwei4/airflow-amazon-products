import pandas as pd
import numpy as np
import string

import logging


def remove_punctuation(review):
    no_punct = [words.lower() for words in review if words not in string.punctuation]
    review_wo_punct = "".join(no_punct)
    return review_wo_punct


def get_clean_review_df(source_file):
    df = pd.read_csv(source_file)
    logging.info(f"before dropping na for {source_file}...{df.shape}")
    df = df.dropna(subset=["review"])
    logging.info(f"after dropping na for {source_file}...{df.shape}")
    logging.info(f"running clean review for {source_file}...")
    df["review_no_punct"] = df["review"].apply(remove_punctuation)
    return df
