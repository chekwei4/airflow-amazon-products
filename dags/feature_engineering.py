from locale import D_FMT
import pandas as pd
import numpy as np

import logging

import datetime


def get_feat_eng_df(source_file):
    df = pd.read_csv(source_file)
    logging.info(f"before preprocessing...{df.shape}")
    df = drop_features(df)
    df = create_review_time(df)
    df = create_review(df)
    df = create_sentiment(df)
    df["reviewText_len"] = df["review"].astype(str).apply(len)
    df = drop_review_len_zero(df)
    return df


def drop_features(df):
    return df.drop(["reviewerID", "reviewerName", "helpful", "unixReviewTime"], axis=1)


def create_review_time(df):
    # Splitting the date
    temp_dt = df["reviewTime"].str.split(",", n=1, expand=True)
    # adding date to the main dataset
    df["date"] = temp_dt[0]
    # adding year to the main dataset
    df["year"] = temp_dt[1]
    temp_dt = df["date"].str.split(" ", n=1, expand=True)
    # adding month to the main dataset
    df["month"] = temp_dt[0]
    # adding day to the main dataset
    df["day"] = temp_dt[1]
    df = df.drop(["date", "reviewTime"], axis=1)
    df["reviewTime"] = pd.to_datetime(df[["year", "month", "day"]])
    df = df.drop(["year", "month", "day"], axis=1)
    return df


def create_review(df):
    df["review"] = df["reviewText"] + " " + df["summary"]
    df = df.drop(["reviewText", "summary"], axis=1)
    return df


def create_bins(rating):
    if rating >= 4.0:
        val = "pos"
    else:
        val = "neg"
    return val


def create_sentiment(df):
    df["sentiment"] = df["overall"].apply(create_bins)
    return df


def drop_review_len_zero(df):
    return df.drop(df[df["reviewText_len"] == 0].index)
