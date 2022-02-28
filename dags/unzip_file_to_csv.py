import sys
import logging
import pandas as pd
import gzip


# def parse(file):
#     g = gzip.open(file, "rb")
#     for l in g:
#         yield eval(l)


# def unzip_file_to_csv(source_file):
#     i = 0
#     df = {}
#     for d in parse(source_file):
#         df[i] = d
#         i += 1
#     df_return = pd.DataFrame.from_dict(df, orient="index")
#     return df_return


def unzip_file_get_df(source_file):
    logging.info(f"source_file is...{source_file}")
    with gzip.open(source_file) as f:
        df = pd.read_json(f, lines=True)
        return df
