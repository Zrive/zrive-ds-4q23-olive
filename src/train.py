from processing import (
    build_spine,
    build_numeric_features_parcela,
    build_binary_features_parcela,
    build_date_variables_parcelas,
    build_categorical_features_parcela,
    build_binary_features_parcela,
    estados,
)
import pandas as pd
from sklearn import metrics
import math

climatic_vars = ['stl1_min_1',
 'stl1_max_1',
 'stl1_mean_1',
 'stl1_median_1',
 'stl1_std_1',
 'tp_min_1',
 'tp_max_1',
 'tp_mean_1',
 'tp_median_1',
 'tp_std_1',
 'swvl1_min_1',
 'swvl1_max_1',
 'swvl1_mean_1',
 'swvl1_median_1',
 'swvl1_std_1',
 'u10_min_1',
 'u10_max_1',
 'u10_mean_1',
 'u10_median_1',
 'u10_std_1',
 'v10_min_1',
 'v10_max_1',
 'v10_mean_1',
 'v10_median_1',
 'v10_std_1',
 'ssr_min_1',
 'ssr_max_1',
 'ssr_mean_1',
 'ssr_median_1',
 'ssr_std_1',
 'stl1_min_2',
 'stl1_max_2',
 'stl1_mean_2',
 'stl1_median_2',
 'stl1_std_2',
 'tp_min_2',
 'tp_max_2',
 'tp_mean_2',
 'tp_median_2',
 'tp_std_2',
 'swvl1_min_2',
 'swvl1_max_2',
 'swvl1_mean_2',
 'swvl1_median_2',
 'swvl1_std_2',
 'u10_min_2',
 'u10_max_2',
 'u10_mean_2',
 'u10_median_2',
 'u10_std_2',
 'v10_min_2',
 'v10_max_2',
 'v10_mean_2',
 'v10_median_2',
 'v10_std_2',
 'ssr_min_2',
 'ssr_max_2',
 'ssr_mean_2',
 'ssr_median_2',
 'ssr_std_2',
 'stl1_min_3',
 'stl1_max_3',
 'stl1_mean_3',
 'stl1_median_3',
 'stl1_std_3',
 'tp_min_3',
 'tp_max_3',
 'tp_mean_3',
 'tp_median_3',
 'tp_std_3',
 'swvl1_min_3',
 'swvl1_max_3',
 'swvl1_mean_3',
 'swvl1_median_3',
 'swvl1_std_3',
 'u10_min_3',
 'u10_max_3',
 'u10_mean_3',
 'u10_median_3',
 'u10_std_3',
 'v10_min_3',
 'v10_max_3',
 'v10_mean_3',
 'v10_median_3',
 'v10_std_3',
 'ssr_min_3',
 'ssr_max_3',
 'ssr_mean_3',
 'ssr_median_3',
 'ssr_std_3',
 'stl1_min_4',
 'stl1_max_4',
 'stl1_mean_4',
 'stl1_median_4',
 'stl1_std_4',
 'tp_min_4',
 'tp_max_4',
 'tp_mean_4',
 'tp_median_4',
 'tp_std_4',
 'swvl1_min_4',
 'swvl1_max_4',
 'swvl1_mean_4',
 'swvl1_median_4',
 'swvl1_std_4',
 'u10_min_4',
 'u10_max_4',
 'u10_mean_4',
 'u10_median_4',
 'u10_std_4',
 'v10_min_4',
 'v10_max_4',
 'v10_mean_4',
 'v10_median_4',
 'v10_std_4',
 'ssr_min_4',
 'ssr_max_4',
 'ssr_mean_4',
 'ssr_median_4',
 'ssr_std_4',
 'value_ndvi_min_1',
 'value_ndvi_max_1',
 'value_ndvi_mean_1',
 'value_ndvi_median_1',
 'value_ndvi_std_1',
 'value_fapar_min_1',
 'value_fapar_max_1',
 'value_fapar_mean_1',
 'value_fapar_median_1',
 'value_fapar_std_1']

def train_test_val_split(df: pd.DataFrame, train_size=0.7, test_size=0.2, cooloff_days=14, print_dates=False):
    """
    Splits into Train / Validation / Test sets and removes ID variables.
    """
    if train_size + test_size > 1:
        raise ValueError("train_size + test_size must be less than 1")

    # Getting the date indexes to split the data
    df_date_cumsum = df.groupby("fecha")["next_y"].count().cumsum() / len(df)
    df_val_index = df_date_cumsum[df_date_cumsum > train_size].index[0]
    df_test_index = df_date_cumsum[df_date_cumsum > train_size + test_size].index[0]

    # Splitting the data into train, test, and validation
    train_df = df.loc[df["fecha"] < df_val_index]
    val_df = df.loc[
        df["fecha"].between(
            df_val_index + pd.DateOffset(days=cooloff_days),
            df_test_index,
            inclusive="left",
        )
    ]
    test_df = df.loc[df["fecha"] >= df_test_index + pd.DateOffset(days=cooloff_days)]

    if print_dates:
        print("Training dates:")
        print(train_df["fecha"].min(), "to", train_df["fecha"].max())
        print("\nValidation dates:")
        print(val_df["fecha"].min(), "to", val_df["fecha"].max())
        print("\nTest dates:")
        print(test_df["fecha"].min(), "to", test_df["fecha"].max())

    train_df = drop_id_features(train_df)
    test_df = drop_id_features(test_df)
    val_df = drop_id_features(val_df)

    return train_df, val_df, test_df

def load_climatic_features() -> pd.DataFrame:
    """
    Loads the dataset with the climatic variables.
    """
    df = pd.read_parquet("meteo_dataset_list_range_40_100.parquet")
    return df


def drop_id_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops the ID variables from the dataset.
    """
    df = df.drop(columns=["fecha", "codparcela"])
    return df


def build_dataset_with_features(
    numeric_features=True,
    categorical_features=True,
    binary_features=True,
    date_parcela_features=True,
    climate_features=True,
) -> pd.DataFrame:
    """
    Creates a dataset with all the given features.
    """
    if climate_features:
        climate_features = load_climatic_features()
        climate_features.dropna(inplace=True)
        spine = build_spine()
        df = pd.merge(climate_features, spine, on=["codparcela", "fecha"], how="left")
        df.dropna(subset="next_y", inplace=True)
        df[climatic_vars] = df[climatic_vars].astype('float32')
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
    """
    Splits the dataset into x and y variables.
    """
    y_data = df["next_y"]
    x_data = df.drop(columns=["next_y"])

    return x_data, y_data


def mse_score_discrete(y_true, y_pred, threshold=0.5):
    """
    MSE after rounding the predictions to the nearest integer specified by the threshoñld.
    """
    y_pred_rounded = []

    for pred in y_pred:
        if pred - int(pred) < threshold:
            y_pred_rounded.append(max(0, math.floor(pred)))
        else:
            y_pred_rounded.append(max(0, math.ceil(pred)))

    return metrics.mean_squared_error(y_true, y_pred_rounded)

