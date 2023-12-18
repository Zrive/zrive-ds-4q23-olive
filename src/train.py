from processing import (
    build_spine,
    build_numeric_features_parcela,
    build_binary_features_parcela,
    build_date_variables_parcelas,
    build_categorical_features_parcela,
    build_binary_features_parcela,
    estados,
)
import numpy as np
import pandas as pd
from sklearn import metrics
import math


def train_test_val_split(
    df: pd.DataFrame, train_size=0.7, test_size=0.2, cooloff_days=14
):
    if train_size + test_size > 1:
        raise ValueError("train_size + test_size must be less than 1")

    # Getting the date indexes to split the data
    df_date_cumsum = df.groupby("fecha")["next_y"].count().cumsum() / len(df)
    df_val_index = df_date_cumsum[df_date_cumsum > train_size].index[0]
    df_test_index = df_date_cumsum[df_date_cumsum > train_size + test_size].index[0]

    # Splitting the data into train, test and validation
    train_df = df.loc[df["fecha"] < df_val_index]
    val_df = df.loc[
        df["fecha"].between(
            df_val_index + pd.DateOffset(days=cooloff_days),
            df_test_index,
            inclusive="left",
        )
    ]
    test_df = df.loc[df["fecha"] >= df_test_index + pd.DateOffset(days=cooloff_days)]

    train_df = drop_id_features(train_df)
    test_df = drop_id_features(test_df)
    val_df = drop_id_features(val_df)

    return train_df, val_df, test_df


def load_climatic_features() -> pd.DataFrame:
    df = pd.read_parquet("meteo_dataset_list_range_40_100.parquet")
    return df


def drop_id_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=["fecha", "codparcela"])

    return df


def build_dataset_with_features(
    numeric_features=True,
    categorical_features=True,
    binary_features=True,
    date_parcela_features=True,
    climate_features=True,
):
    if climate_features:
        climate_features = load_climatic_features()
        climate_features.dropna(inplace=True)
        spine = build_spine()
        df = pd.merge(climate_features, spine, on=["codparcela", "fecha"], how="left")
        df.dropna(subset="next_y", inplace=True)
    else:
        df = build_spine()

    if numeric_features:
        df = build_numeric_features_parcela(df)

    if categorical_features:
        df = build_categorical_features_parcela(df)

    if binary_features:
        df = build_binary_features_parcela(df)

    if date_parcela_features:
        df = build_date_variables_parcelas(df)

    return df


def split_into_x_y(df: pd.DataFrame):
    y_data = df["next_y"]
    x_data = df.drop(columns=["next_y"])

    return x_data, y_data


def mse_score_discrete(y_true, y_pred, threshold=0.5):
    y_pred_rounded = []

    for pred in y_pred:
        if pred - int(pred) < threshold:
            y_pred_rounded.append(max(0, math.floor(pred)))
        else:
            y_pred_rounded.append(max(0, math.ceil(pred)))

    return metrics.mean_squared_error(y_true, y_pred_rounded)


def main():
    df = build_dataset_with_features()
    train, val, test = train_test_val_split(df)
    x_train, y_train = split_into_x_y(train)


if __name__ == "__main__":
    main()
