import pandas as pd

estados = [
    "estado_fenologico_1",
    "estado_fenologico_2",
    "estado_fenologico_3",
    "estado_fenologico_4",
    "estado_fenologico_5",
    "estado_fenologico_6",
    "estado_fenologico_7",
    "estado_fenologico_8",
    "estado_fenologico_9",
    "estado_fenologico_10",
    "estado_fenologico_11",
    "estado_fenologico_12",
    "estado_fenologico_13",
    "estado_fenologico_14"]

def load_dataset():
    df = pd.read_parquet("notebooks/subset_muestreos_parcelas.parquet")
    return df


def find_estado_with_value_two(row) -> int:
    """
    Returns numero del siguiente etado (yt+1). (e.g., 13)
    """
    for column in row.index:
        if row[column] == 2:
            number_growth_stage = int(column.split("_")[-1])
            return number_growth_stage


def get_valid_dataset(df: pd.DataFrame(), max_days_till_next_date: int) -> pd.DataFrame():
    
    # Removing null and filling empty estados with 0
    df = df.dropna(subset=estados, how="all")
    df = df.dropna(subset=["codparcela"])
    df[estados] = df[estados].fillna(0)

    #Filtering Dataset to keep rows only with one unique 2
    df["count_2s"] = df[estados].eq(2).sum(axis=1)
    df = df[df.count_2s == 1]

    # Sorting by date
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.sort_values(by="fecha", inplace=True)

    # Creating a column to display the number of days till the next observation
    df["next_date"] = df.groupby("codparcela", observed=True)["fecha"].shift(-1)
    df["days_until_next_visit"] = (df["next_date"] - df["fecha"]).dt.days

    # Creating a column to display the next estado_fenológico (yt+1)
    df["estado_actual"] = df[estados].apply(find_estado_with_value_two, axis=1)
    df["next_estado"] = df.groupby("codparcela", observed=True)["estado_actual"].shift(-1)

    # Removing the parcels with only one entry and the last entry for every parcel
    df = df.dropna(subset=["days_until_next_visit"])  # 5150 entries removed

    #Removing entries where estado decreases
    mask_estado_decrease = df["next_estado"] - df["estado_actual"] < 0
    df = df[~mask_estado_decrease]

    #Changing datatypes
    df["days_until_next_visit"] = df["days_until_next_visit"].astype("int16")
    df["next_estado"] = df["next_estado"].astype("int8")
    df[estados] = df[estados].astype("int8")

    # Filtering the max days
    df = df[df["days_until_next_visit"] < max_days_till_next_date]

    excluded_columns = ['count_2s', 'next_date']

    return df.loc[:, [i for i in df.columns if i not in excluded_columns]]


def build_spine() -> pd.DataFrame():
    """
    Returns a spine with the field name, date and next estado
    """

    df = load_dataset()
    columns_needed = estados + ['codparcela', 'fecha']
    df = get_valid_dataset(df[columns_needed], 30)
    
    return df['codparcela','fecha','next_estado']


def build_features_parcela() -> pd.DataFrame():
    """
    Returns a dataset with the features for each field.
    """
    pass