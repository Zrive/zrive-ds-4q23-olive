import os
import math
import pandas as pd
from datetime import timedelta


main_data_folder = "data"
meteo_data_folder = f"{main_data_folder}/clean/final"


output_data_folder = f"{main_data_folder}/clean/meteo_variables"

if not os.path.exists(output_data_folder):
    os.makedirs(output_data_folder)


VARIABLES = ["stl1", "tp", "swvl1", "u10", "v10", "ssr", "value_ndvi", "value_fapar"]

OPERATIONS = ["min", "max", "mean", "median", "std"]

METEO_COLUMNS = [
    f"{variable}_{operation}" for variable in VARIABLES for operation in OPERATIONS
]

N_WEEKS = 4


def assign_week_number(x):
    """It assigns a week number based on number of days

    Parameters:
    x (int): Number of days

    Returns:
    int: Week number (1 if day = 1-7, 2 if day = 8-14, etc...)

    """
    return 1 if x == 0 else math.ceil(x - 1) // 7 + 1


def read_raw_meteo_data(codparcela: str) -> pd.DataFrame:
    """It reads unprocessed meteorological data based on codparcela (smallholding ID)

    Parameters:
    codparcela (str): smallholding ID

    Returns:
    df_meteo_parcela (pd.DataFrame): Dataframe containing unprocessed
    meteorological values

    """
    file_path = f"{meteo_data_folder}/{codparcela}.csv"

    if not os.path.exists(file_path):
        print(
            f"Error: The file {codparcela}.csv does not exist in {meteo_data_folder}."
        )
        return None

    df_meteo_parcela = pd.read_csv(
        file_path, index_col=0, sep="\t", parse_dates=["date"]
    )
    return df_meteo_parcela


def compute_time_window_stats(
    fecha_muestra: pd.Timestamp, df_meteo_parcela: pd.DataFrame
):
    """It creates "processed" meteorological variables for every sample.
    Based on sample date, the function extracts data prior to N_WEEKS
    and compute values of min, max, mean, median and std

    Parameters:
    fecha_muestra (pd.Timestamp): Sample date

    df_meteo_parcela (pd.DataFrame): Dataframe containing unprocessed
    meteorological values

    Returns:
    df_meteo_variables_per_week (pd.DataFrame): Dataframe containing
    values of min, max, mean, median and std for every meteorological
    variable grouped by weeks

    """

    fecha_end_slice = fecha_muestra - timedelta(7 * N_WEEKS)

    df_meteo_sample = df_meteo_parcela[
        (df_meteo_parcela["date"] >= fecha_end_slice)
        & (df_meteo_parcela["date"] <= fecha_muestra)
    ].copy()

    df_meteo_sample["diff_days_sample"] = (
        fecha_muestra - df_meteo_sample["date"]
    ).dt.days

    df_meteo_sample["weeks"] = df_meteo_sample["diff_days_sample"].apply(
        assign_week_number
    )  # , args=(n_weeks,))

    df_meteo_variables_per_week = df_meteo_sample.groupby("weeks")[VARIABLES].agg(
        OPERATIONS
    )  # tem

    df_meteo_variables_per_week.columns = METEO_COLUMNS

    return df_meteo_variables_per_week


def flatten_meteo_variables(
    id_codparcela: str,
    fecha_muestra: pd.Timestamp,
    df_meteo_variables_per_week: pd.DataFrame,
):
    """It flattens the previous dataframe, grouping all the weekly
    variables in one row, and naming the columns with the format
    {variable}_{operation}_{n_week} (E.g. stl1_min_1). Also, it
    adds smallholding ID and sample date as spine

    Parameters:
    fecha_muestra (pd.Timestamp): Sample date

    df_meteo_variables_per_week (pd.DataFrame): Dataframe containing
    values of min, max, mean, median and std for every meteorological
    variable grouped by weeks

    Returns:
    df_meteo_variables_flattened (pd.DataFrame): Dataframe containing
    values of min, max, mean, median and std for every meteorological
    variable in one row

    """

    n_rows, n_column = df_meteo_variables_per_week.shape

    flat_values = df_meteo_variables_per_week.values.reshape(n_rows * n_column)

    resultados_columns = [
        f"{column}_{i}"
        for i in range(1, n_rows + 1)
        for column in df_meteo_variables_per_week.columns
    ]

    df_meteo_variables_flattened = pd.DataFrame(
        [flat_values], columns=resultados_columns
    )

    df_meteo_variables_flattened["codparcela"] = id_codparcela
    df_meteo_variables_flattened["fecha"] = fecha_muestra

    df_meteo_variables_flattened.insert(
        0, "codparcela", df_meteo_variables_flattened.pop("codparcela")
    )
    df_meteo_variables_flattened.insert(
        1, "fecha", df_meteo_variables_flattened.pop("fecha")
    )

    return df_meteo_variables_flattened


def get_meteo_variables_parcela(
    df_subsamples: pd.DataFrame, df_meteo_parcela: pd.DataFrame
):
    """It gets meteorological variables per smallholding for every sample

    Parameters:
    df_subsamples (pd.DataFrame): Dataframe containing every sample for
    a specific smallholding

    df_meteo_parcela (pd.DataFrame): Dataframe containing unprocessed
    meteorological values

    Returns:
    df_meteo_variables_parcela (pd.DataFrame): Dataframe containing
    values of min, max, mean, median and std for every meteorological
    variable for every sample in smallholding

    """
    df_meteo_variables_parcela = pd.DataFrame()

    for _, sample in df_subsamples.iterrows():
        fecha_muestra = sample["fecha"]

        id_codparcela = sample["codparcela"]

        df_meteo_variables_per_week = compute_time_window_stats(
            fecha_muestra, df_meteo_parcela
        )

        df_meteo_variables_flattened = flatten_meteo_variables(
            id_codparcela, fecha_muestra, df_meteo_variables_per_week
        )

        df_meteo_variables_parcela = pd.concat(
            [df_meteo_variables_parcela, df_meteo_variables_flattened],
            ignore_index=True,
        )

    return df_meteo_variables_parcela


def main():
    sample_filename = "subset_muestreos_parcelas.parquet"

    df_samples = pd.read_parquet(f"{main_data_folder}/{sample_filename}")

    df_samples["fecha"] = pd.to_datetime(df_samples["fecha"])

    df_samples["codparcela"] = df_samples["codparcela"].str.replace("/", "")

    codparcelas = df_samples["codparcela"].unique()

    for _, codparcela in enumerate(codparcelas):
        df_subsamples_parcela = df_samples[df_samples["codparcela"] == codparcela]

        df_meteo_parcela = read_raw_meteo_data(codparcela)

        df_meteo_variables_parcela = get_meteo_variables_parcela(
            df_subsamples_parcela, df_meteo_parcela
        )

        # Stores all the proccesed data per smallholding

        df_meteo_variables_parcela.to_parquet(
            f"{output_data_folder}/{codparcela}.parquet"
        )

    # Joins all the data in one parquet

    parquet_files = os.listdir(output_data_folder)

    df_meteo_variables_final = pd.DataFrame()

    for _, file in enumerate(parquet_files):
        df_parcela = pd.read_parquet(f"{output_data_folder}/{file}")

        df_meteo_variables_final = pd.concat(
            [df_meteo_variables_final, df_parcela],
            axis=0,
            ignore_index=True,
            keys=["df_meteo_variables_final", "df_parcela"],
        )

        df_meteo_variables_final.to_parquet(
            f"{main_data_folder}/meteo_variables_dataset.parquet"
        )


if __name__ == "__main__":
    main()
