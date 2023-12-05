import pandas as pd
import numpy as np

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
    df = pd.read_parquet("subset_muestreos_parcelas.parquet")
    df = get_valid_dataset(df,32)
    return df


def get_valid_dataset(df: pd.DataFrame(), max_days_till_next_date: int) -> pd.DataFrame():
    
    def find_estado_with_value_two(row) -> int:
        """
        Returns numero del siguiente etado (yt+1). (e.g., 13)
        """
        for column in row.index:
            if row[column] == 2:
                number_growth_stage = int(column.split("_")[-1])
                return number_growth_stage
    
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
    df["next_y"] = df["next_estado"] - df["estado_actual"]

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

    excluded_columns = ['count_2s', 'next_date', 'next_estado']

    return df.loc[:, [i for i in df.columns if i not in excluded_columns]]


def build_spine() -> pd.DataFrame():
    """
    Returns a spine with the field name, date and next estado, current estados
    """

    df = load_dataset()
    
    return df[['codparcela','fecha','next_y']]


def build_numeric_features_parcela(spine: pd.DataFrame) -> pd.DataFrame():

    """
    Returns a dataset with the numeric features for each field.
    """
    # We are not using '305_diámetro_de_copa_(m)', '311_fecha_de_plantación_variedad_principal', '306_altura_de_copa_(m)'
    # As they have > 70% NA
    numeric_features = ['104_altitud_(m)',
    '201_superf_cultivada_en_la_parcela_agrícola_(ha)',
    '202_pendiente_(%)',
    '301_marco_(m_x_m)',
    '302_densidad_(plantas/ha)',
    '303_nº_pies_por_árbol',
    'porcentaje_floracion']

    df = load_dataset()
    df_parcelas = df.groupby(by='codparcela')[numeric_features].first() #Selects first non-empty entry for each feature

    def replace_nullwithmean_remove_outliers(df, threshold = np.inf):
        df = df.copy()
        mean_value = df.mean()
        df.fillna(mean_value, inplace=True)
        df.loc[df > threshold] = mean_value
        return df
    
    #Replace outliers and null with mean
    df_parcelas['104_altitud_(m)'] = replace_nullwithmean_remove_outliers(df_parcelas['104_altitud_(m)'], 2500)
    df_parcelas['201_superf_cultivada_en_la_parcela_agrícola_(ha)'] = replace_nullwithmean_remove_outliers(df_parcelas['201_superf_cultivada_en_la_parcela_agrícola_(ha)'],10000)
    df_parcelas['202_pendiente_(%)'] = replace_nullwithmean_remove_outliers(df_parcelas['202_pendiente_(%)'],100)
    df_parcelas['302_densidad_(plantas/ha)'] = replace_nullwithmean_remove_outliers(df_parcelas['302_densidad_(plantas/ha)'],10000)
    df_parcelas['303_nº_pies_por_árbol'] = replace_nullwithmean_remove_outliers(df_parcelas['303_nº_pies_por_árbol'],100)
    df_parcelas['porcentaje_floracion'] = replace_nullwithmean_remove_outliers(df_parcelas['porcentaje_floracion'],100)

    df_parcelas['301_marco_(m_x_m)'] = df_parcelas['301_marco_(m_x_m)'].astype(str).str.replace(",", ".")
    df_parcelas['301_marco_(m_x_m)'] = df_parcelas['301_marco_(m_x_m)'].str.extract(r'(\d+\.\d+|\d+)')[0].astype(float) #Finds the first number (e.g. from 1.5 x 1.5, finds 1.5)
    df_parcelas['301_marco_(m_x_m)'] = replace_nullwithmean_remove_outliers(df_parcelas['301_marco_(m_x_m)'],100)

    #Datatypes
    df_parcelas['104_altitud_(m)'] = df_parcelas['104_altitud_(m)'].astype('int32')
    df_parcelas['104_altitud_(m)'] = df_parcelas['104_altitud_(m)'].astype('float32')
    df_parcelas['202_pendiente_(%)'] = df_parcelas['202_pendiente_(%)'].astype('int32')
    df_parcelas['302_densidad_(plantas/ha)'] = df_parcelas['302_densidad_(plantas/ha)'].astype('int32')
    df_parcelas['303_nº_pies_por_árbol'] = df_parcelas['303_nº_pies_por_árbol'].astype('int32')
    df_parcelas['porcentaje_floracion'] = df_parcelas['porcentaje_floracion'].astype('int32')
    df_parcelas['301_marco_(m_x_m)'] = df_parcelas['301_marco_(m_x_m)'].astype('float32')

    return pd.merge(spine, df_parcelas, left_on='codparcela',right_index=True, how='left')

def build_binary_features_parcela(spine: pd.DataFrame) -> pd.DataFrame():
    """
    Returns a dataset with the features for each field.
    """
    #We don't use 214_cultivo_asociado/otro_aprovechamiento, '209_riego:_calidad_del_agua'

    binary_features = [
    '211_utilización_de_cubierta_vegetal',
    '208_riego:_procedencia_del_agua',
    '207_riego:_sistema_usual_de_riego',
    '109_sistema_para_el_cumplimiento_gestión_integrada']

    #Grouped By Parcel
    df = load_dataset()
    df_parcelas = df.groupby(by='codparcela')[binary_features].first()


    #For now replacing null with 0 and the rest to a binay 1 or 0
    replace_dict = {'Si': 1, 'No': 0, 'NO': 0,'SI':1}
    df_parcelas["211_utilización_de_cubierta_vegetal"] = df_parcelas["211_utilización_de_cubierta_vegetal"].replace(replace_dict).fillna(0).astype(int)

    replace_dict_riego = {'Pozo': 1, 'POZO': 1}

    def apply_dict(row, replace_dict):
        """
        If the key is in the dict it uses the value of the dict, if not it returns 0
        """
        if row in replace_dict.keys():
            return replace_dict[row]
        else:
            return 0

    df_parcelas['208_riego:_procedencia_del_agua'] = df_parcelas['208_riego:_procedencia_del_agua'].astype('str').apply(apply_dict, replace_dict=replace_dict_riego).fillna(0).astype(int)

    dict_replace_rieg= {}
    for i in df_parcelas['207_riego:_sistema_usual_de_riego'].str.lower().unique():
        if pd.isna(i):
            continue
        if "got" in i:
            dict_replace_rieg[i] = 1
        else: dict_replace_rieg[i] = 0
    
    df_parcelas['207_riego:_sistema_usual_de_riego'] = df_parcelas['207_riego:_sistema_usual_de_riego'].apply(apply_dict, replace_dict=dict_replace_rieg).fillna(0).astype(int)
    replace_dict_gest = {'Producción Integrada (PI)': 1}
    df_parcelas['109_sistema_para_el_cumplimiento_gestión_integrada'] = df_parcelas['109_sistema_para_el_cumplimiento_gestión_integrada'].apply(apply_dict, replace_dict=replace_dict_gest).fillna(0).astype(int)

    return pd.merge(spine, df_parcelas, left_on='codparcela',right_index=True, how='left')


def build_date_variables_parcelas(spine: pd.DataFrame) -> pd.DataFrame():

    df = load_dataset()
    relevant_cols = estados + ['porcentaje_floracion','codparcela','fecha']
    df = df[relevant_cols]

    return pd.merge(spine, df, on=['codparcela','fecha'], how='left')


def build_categorical_features_parcela(spine: pd.DataFrame) -> pd.DataFrame():
    """
    Returns a dataset with the features for each field.
    """
    pass