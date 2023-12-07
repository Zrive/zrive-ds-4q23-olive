import pandas as pd
import numpy as np
import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, project_root)

import src.pre_processing as prep


DATA = {
    "date": pd.date_range(start="2023-03-01", periods=7, freq="D"),
    "stl1": [10, 12, 11, 13, 14, 15, 13],
    "tp": [5, 6, 5, 7, 6, 5, 6],
    "swvl1": [200, 205, 210, 200, 195, 190, 185],
    "u10": [3.5, 3.6, 3.7, 3.8, 3.9, 4.0, 4.1],
    "v10": [2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7],
    "ssr": [100, 105, 110, 115, 120, 125, 130],
    "value_ndvi": [
        0.5,
        0.55,
        0.6,
        0.65,
        0.7,
        0.75,
        0.8,
    ],
    "value_fapar": [
        0.3,
        0.32,
        0.34,
        0.36,
        0.38,
        0.4,
        0.42,
    ],
}


def test_calculate_stats_for_week():
    df_one_week = pd.DataFrame(DATA)
    df_one_week.set_index("date", inplace=True)

    results = prep._calculate_stats_for_week(df_one_week)

    expected_values = {}
    for column in df_one_week.columns:
        expected_values[f"{column}_mean"] = df_one_week[column].mean()
        expected_values[f"{column}_std"] = df_one_week[column].std()
        expected_values[f"{column}_min"] = df_one_week[column].min()
        expected_values[f"{column}_max"] = df_one_week[column].max()
        expected_values[f"{column}_median"] = df_one_week[column].median()

    for key, value in expected_values.items():
        assert (
            results[key] == value
        ), f"Stat {key} is incorrect. Expected: {value}, and got: {results[key]}"

    print("\n\033[92mTest for one week data passed!\033[0m\n\n")


def test_calculate_parcela_weekly_statistics():
    test_date = pd.Timestamp("2017-03-10 00:00:00")
    test_cod_parcela = "001-00004-01-0"
    test_num_prev_weeks = 2

    result_df = prep.calculate_parcela_weekly_statistics(
        test_date, test_cod_parcela, test_num_prev_weeks
    )

    # Calculating expected values

    current_dir = os.path.dirname(os.path.realpath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    path = os.path.join(
        project_root,
        "zrive-ds-4q23-olive",
        "data",
        "raw",
        "dataset_climatico_parcelas",
        f"{test_cod_parcela}.csv",
    )

    df_aux = pd.read_csv(
        path,
        sep="\t",
        index_col=0,
        parse_dates=["date"],
    )

    df_aux.set_index("date", inplace=True)

    date_week_1 = test_date - pd.Timedelta(weeks=1)
    date_week_2 = date_week_1 - pd.Timedelta(weeks=1)

    df_week_1 = df_aux.loc[date_week_1:test_date]

    df_week_2 = df_aux.loc[date_week_2:date_week_1]

    expected_values1 = prep._calculate_stats_for_week(df_week_1)

    expected_values2 = prep._calculate_stats_for_week(df_week_2)

    # Check if the result is as expected
    assert not result_df.empty, "The result DataFrame should not be empty."

    assert (
        len(result_df) == test_num_prev_weeks
    ), f"The result DataFrame should have {test_num_prev_weeks} rows."

    for key, expected_value in expected_values1.items():
        actual_value = result_df[key].iloc[0]
        if np.isnan(expected_value) and np.isnan(actual_value):
            continue
        elif expected_value != actual_value:
            raise AssertionError(
                f"Statistic {key} is incorrect. Expected: {expected_value}, Got: {actual_value}"
            )

    for key, expected_value in expected_values2.items():
        actual_value = result_df[key].iloc[1]
        if np.isnan(expected_value) and np.isnan(actual_value):
            continue
        elif expected_value != actual_value:
            raise AssertionError(
                f"Statistic {key} is incorrect. Expected: {expected_value}, Got: {actual_value}"
            )

    print("\n\n\033[92mTest for a real .csv and 2 weeks passed!\033[0m\n")


def main():
    test_calculate_stats_for_week()
    test_calculate_parcela_weekly_statistics()


if __name__ == "__main__":
    main()
