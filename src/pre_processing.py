import pandas as pd
import datetime
import logging
import os

VARIABLES = ["stl1", "tp", "swvl1", "u10", "v10", "ssr", "value_ndvi", "value_fapar"]
STATISTICS = ["mean", "std", "min", "max", "median"]


def _load_parcela_climate_data(cod_parcela: str):
    current_dir = os.path.dirname(os.path.realpath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    path = os.path.join(
        project_root,
        "zrive-ds-4q23-olive",
        "data",
        "raw",
        "dataset_climatico_parcelas",
        f"{cod_parcela}.csv",
    )

    if not os.path.exists(path):
        raise FileNotFoundError(f"The file {path} does not exist.")

    df_aux = pd.read_csv(path, sep="\t", index_col=0, parse_dates=["date"])

    df_aux.set_index("date", inplace=True)  # Necessary for slicing with .loc afterwards

    return df_aux


def _calculate_stats_for_week(week_df):
    """
    The function assumes that the dataframse passed is already an individual week
    """
    stats = {}
    for var in VARIABLES:
        stats[f"{var}_mean"] = week_df[var].mean()
        stats[f"{var}_std"] = week_df[var].std()
        stats[f"{var}_min"] = week_df[var].min()
        stats[f"{var}_max"] = week_df[var].max()
        stats[f"{var}_median"] = week_df[var].median()
    return stats


def calculate_parcela_weekly_statistics(
    date: pd.Timestamp, cod_parcela: str, num_prev_weeks: int
):
    """
    Returns a DataFrame with the climate data of the last num_prev_days days
    """
    # Inserting the columns to the new dataframe
    columns = []

    for var in VARIABLES:
        for stat in STATISTICS:
            columns.append(f"{var}_{stat}")

    df = pd.DataFrame(columns=columns)

    # We read the csv file of climate data of the given parcela
    df_par = _load_parcela_climate_data(cod_parcela)

    for i in range(1, num_prev_weeks + 1):
        # We get the week i ago and slice the df_par with it
        start_week_i_ago = date - pd.Timedelta(days=7 * i)
        end_week_i_ago = date - pd.Timedelta(days=7 * (i - 1))

        week_i_df = df_par.loc[start_week_i_ago:end_week_i_ago]

        # We calculate the statistics for that week
        stats = _calculate_stats_for_week(week_i_df)
        stats_df = pd.DataFrame([stats])

        # concat gives a FutureWarning when having NaN values, but it looks like it's just a bug (https://github.com/pandas-dev/pandas/issues/55928)
        # stats_df.dropna(axis=1, how='all', inplace=True)

        df = pd.concat([df, stats_df], ignore_index=True)

    return df
