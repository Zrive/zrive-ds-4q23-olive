import os
import math
import pandas as pd
from datetime import timedelta



main_data_folder = "data"
meteo_data_folder = f"{main_data_folder}/clean/final"


VARIABLES = ["stl1", "tp", "swvl1", "u10", "v10", "ssr", "value_ndvi", "value_fapar"]

OPERATIONS = ["min", "max", "mean", "std"]

METEO_COLUMNS = [
    f"{variable}_{operation}" for variable in VARIABLES for operation in OPERATIONS
]

N_WEEKS = 4


def group_days_in_weeks(x):
    return 1 if x == 0 else math.ceil(x - 1) // 7 + 1





def read_raw_meteo_data(codparcela: str) -> pd.DataFrame:
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


def group_meteo_data_row(fecha_muestra: pd.DataFrame, df_meteo_parcela: pd.DataFrame):
    fecha_end_slice = fecha_muestra - timedelta(7 * N_WEEKS)

    df_meteo_sample = df_meteo_parcela[
        (df_meteo_parcela["date"] >= fecha_end_slice)
        & (df_meteo_parcela["date"] <= fecha_muestra)
    ].copy()

    df_meteo_sample["diff_days_sample"] = (
        fecha_muestra - df_meteo_sample["date"]
    ).dt.days

    df_meteo_sample["weeks"] = df_meteo_sample["diff_days_sample"].apply(
        group_days_in_weeks
    )  # , args=(n_weeks,))

    df_meteo_raw_grouped = df_meteo_sample.groupby("weeks")[VARIABLES].agg(OPERATIONS)

    df_meteo_raw_grouped.columns = METEO_COLUMNS

    return df_meteo_raw_grouped


def get_meteo_variables_row(
    id_codparcela: str, fecha_muestra: pd.Timestamp, df_meteo_raw_grouped: pd.DataFrame
):
    n_rows, n_column = df_meteo_raw_grouped.shape

    flat_values = df_meteo_raw_grouped.values.reshape(n_rows * n_column)

    resultados_columns = [
        f"{column}_{i}"
        for i in range(1, n_rows + 1)
        for column in df_meteo_raw_grouped.columns
    ]

    df_meteo_proccessed = pd.DataFrame([flat_values], columns=resultados_columns)

    df_meteo_proccessed["codparcela"] = id_codparcela
    df_meteo_proccessed["fecha"] = fecha_muestra

    df_meteo_proccessed.insert(0, "codparcela", df_meteo_proccessed.pop("codparcela"))
    df_meteo_proccessed.insert(1, "fecha", df_meteo_proccessed.pop("fecha"))

    return df_meteo_proccessed


def get_meteo_variables_parcela(
    df_subsamples: pd.DataFrame, df_meteo_parcela: pd.DataFrame
):
    df_concat_meteo_variables_parcela = pd.DataFrame()

    for _, row in df_subsamples.iterrows():
        fecha_muestra = row["fecha"]

        id_codparcela = row["codparcela"]

        df_meteo_raw_grouped = group_meteo_data_row(fecha_muestra, df_meteo_parcela)

        df_meteo_proccessed = get_meteo_variables_row(
            id_codparcela, fecha_muestra, df_meteo_raw_grouped
        )

        if df_concat_meteo_variables_parcela.empty:
            df_concat_meteo_variables_parcela = df_meteo_proccessed.copy()
        else:
            df_concat_meteo_variables_parcela = pd.concat(
                [df_concat_meteo_variables_parcela, df_meteo_proccessed],
                axis=0,
                ignore_index=True,
                keys=["df_concat_meteo_variables_parcela", "df_meteo_proccessed"],
            )

    return df_concat_meteo_variables_parcela


def main():

    sample_filename = "subset_muestreos_parcelas.parquet"

    df_samples = pd.read_parquet(f"{main_data_folder}/{sample_filename}")

    df_samples["fecha"] = pd.to_datetime(df_samples["fecha"])

    df_samples["codparcela"] = df_samples["codparcela"].str.replace("/", "")


    codparcelas = df_samples["codparcela"].unique()

    df_meteo_variables_parcela_total = pd.DataFrame()

    for _, codparcela in enumerate(codparcelas):
        df_subsamples_parcela = df_samples[df_samples["codparcela"] == codparcela]

        df_meteo_parcela = read_raw_meteo_data(codparcela)

        df_meteo_variables_parcela = get_meteo_variables_parcela(
            df_subsamples_parcela, df_meteo_parcela
        )

        if df_meteo_variables_parcela.empty:
            df_meteo_variables_parcela_total = df_meteo_variables_parcela.copy()
        else:
            df_meteo_variables_parcela_total = pd.concat(
                [df_meteo_variables_parcela_total, df_meteo_variables_parcela],
                axis=0,
                ignore_index=True,
                keys=["df_meteo_variables_parcela_total", "df_meteo_variables_parcela"],
            )

        df_meteo_variables_parcela_total.to_parquet(
            f"{main_data_folder}/meteo_variables_dataset.parquet"
        )
    


if __name__ == "__main__":
    main()
