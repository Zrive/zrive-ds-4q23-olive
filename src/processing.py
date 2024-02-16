import pandas as pd
import numpy as np
from config_parcelas import mean_thresholds_config
from utils import replace_nullwithmean_remove_outliers

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
    "estado_fenologico_14",
]


def load_dataset() -> pd.DataFrame():
    """
    Loads and shrinks dataset
    """
    df = pd.read_parquet("subset_muestreos_parcelas.parquet")
    df = get_valid_dataset(df)
    return df


def find_estado_with_value_two(row) -> int:
    """
    Returns numero del siguiente etado (yt+1). (e.g., 13)
    """
    for column in row.index:
        if row[column] == 2:
            number_growth_stage = int(column.split("_")[-1])
            return number_growth_stage


def keep_valid_estados(df: pd.DataFrame()) -> pd.DataFrame():
    """
    Removes rows with non-valid estados and codparcela.
    """
    df = df.dropna(subset=estados, how="all")
    df = df.dropna(subset=["codparcela"])
    df = df.drop_duplicates(
        subset=["codparcela", "fecha"], keep="first"
    )  # Drop 5k duplicates

    df[estados] = df[estados].fillna(0).astype("int8")

    # Filtering Dataset to keep rows only with one unique 2
    df["count_2s"] = df[estados].eq(2).sum(axis=1)
    df = df[df.count_2s == 1]
    return df


def obtain_next_estado(
    df: pd.DataFrame, days_till_next_sample: int, spam: int
) -> pd.DataFrame:
    """
    Returns a dataset with the growth stage at y(t+ n_days).
    """
    # Creating a column to display the current growth stage
    df["estado_actual"] = df[estados].apply(find_estado_with_value_two, axis=1)
    df.set_index("fecha", inplace=True)

    for i in range(days_till_next_sample - spam, days_till_next_sample + spam + 1):
        df[f"next_estado_{i}"] = df.groupby("codparcela")["estado_actual"].transform(
            lambda x: x.shift(-i, freq="D")
        )

    # Next estado
    df = df.reset_index()
    estados_to_check = [
        f"next_estado_{i}"
        for i in range(days_till_next_sample - spam, days_till_next_sample + spam + 1)
    ]
    df["next_estado"] = df.loc[:, estados_to_check].apply(
        lambda x: x[x.first_valid_index()]
        if x.first_valid_index() is not None
        else None,
        axis=1,
    )

    df = df.drop(columns=estados_to_check)

    # Removing parcels with null y values.
    df = df.dropna(subset=["next_estado"])

    return df


def get_valid_dataset(
    df: pd.DataFrame(), days_till_next_sample=14, spam=1, y_relative=True
) -> pd.DataFrame():
    """
    Filters dataset by number of days and removes irrelevant rows
    """
    df = keep_valid_estados(df)

    # Sorting by date
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.sort_values(by="fecha", inplace=True)

    df = obtain_next_estado(df, days_till_next_sample, spam)

    if y_relative:
        df["next_y"] = df["next_estado"] - df["estado_actual"]
    else:
        df["next_y"] = df["next_estado"]

    # Removing entries where estado decreases
    mask_estado_decrease = df["next_estado"] - df["estado_actual"] < 0
    df = df[~mask_estado_decrease]

    # Changing datatypes
    df["next_y"] = df["next_y"].astype("int8")

    excluded_columns = [
        "count_2s",
        "next_estado",
        "estado_actual",
    ]

    return df.loc[:, [i for i in df.columns if i not in excluded_columns]]


def build_spine() -> pd.DataFrame():
    """
    Returns a spine with the field name, date and next estado, current estados
    """
    df = load_dataset()

    return df[["codparcela", "fecha", "next_y"]]


def clean_301_marco(df_301_marco) -> pd.DataFrame():
    df = df_301_marco.copy()
    df["301_marco_(m_x_m)"] = df["301_marco_(m_x_m)"].astype(str).str.replace(",", ".")
    df["301_marco_(m_x_m)"] = (
        df["301_marco_(m_x_m)"].str.extract(r"(\d+\.\d+|\d+)")[0].astype(float)
    )  # Finds the first number (e.g. from 1.5 x 1.5, finds 1.5)

    return df


def compute_numeric_features_parcela(df: pd.DataFrame()) -> pd.DataFrame():
    df["301_marco_(m_x_m)"] = clean_301_marco(df[["301_marco_(m_x_m)"]])

    for key, values in mean_thresholds_config.items():
        threshold = values.get("threshold")
        is_int = values.get("is_int")
        df[key] = replace_nullwithmean_remove_outliers(df[key], threshold, is_int)

    return df


def build_numeric_features_parcela(spine: pd.DataFrame()) -> pd.DataFrame():
    """
    Returns a dataset with the numeric features for each field.
    """
    # We are not using '305_diámetro_de_copa_(m)', '311_fecha_de_plantación_variedad_principal', '306_altura_de_copa_(m)'
    # As they have > 70% NA
    numeric_features = [
        "104_altitud_(m)",
        "201_superf_cultivada_en_la_parcela_agrícola_(ha)",
        "202_pendiente_(%)",
        "301_marco_(m_x_m)",
        "302_densidad_(plantas/ha)",
        "303_nº_pies_por_árbol",
        "porcentaje_floracion",
    ]

    df = load_dataset()
    df_parcelas = df.groupby(by="codparcela")[
        numeric_features
    ].first()  # Selects first non-empty entry for each feature

    df_parcelas = compute_numeric_features_parcela(df_parcelas)

    return pd.merge(
        spine, df_parcelas, left_on="codparcela", right_index=True, how="left"
    )


def convert_to_binary(df: pd.DataFrame(), replace_dict: dict) -> pd.DataFrame():
    """
    Replaces considering dictionary and changes datatypes
    """
    df = df.astype("str").str.lower()
    df = df.map(replace_dict).fillna(0).astype("int8")

    return df


def convert_207_riego_to_binary(df: pd.DataFrame()) -> pd.DataFrame():
    dict_replace_riego = {}  # Solo 1s para las que ponga algo sobre goteo
    for i in df.str.lower().unique():
        if pd.isna(i):
            continue
        if "got" in i:
            dict_replace_riego[i] = 1
        else:
            dict_replace_riego[i] = 0

    return convert_to_binary(df, dict_replace_riego)


def build_binary_features_parcela(spine: pd.DataFrame()) -> pd.DataFrame():
    """
    Returns a dataset with the features for each field.
    """
    # We don't use 214_cultivo_asociado/otro_aprovechamiento, '209_riego:_calidad_del_agua'
    binary_features = [
        "211_utilización_de_cubierta_vegetal",
        "208_riego:_procedencia_del_agua",
        "207_riego:_sistema_usual_de_riego",
        "109_sistema_para_el_cumplimiento_gestión_integrada",
    ]

    # Grouped By Field
    df = load_dataset()
    df_parcelas = df.groupby(by="codparcela")[binary_features].first()

    # Clean variables
    df_parcelas["207_riego:_sistema_usual_de_riego"] = convert_207_riego_to_binary(
        df_parcelas["207_riego:_sistema_usual_de_riego"]
    )

    mapping_dict = {
        "211_utilización_de_cubierta_vegetal": {"si": 1, "no": 0},
        "208_riego:_procedencia_del_agua": {"pozo": 1},
        "109_sistema_para_el_cumplimiento_gestión_integrada": {
            "producción integrada (pi)": 1
        },
    }

    for feature, map_dict in mapping_dict.items():
        df_parcelas[feature] = convert_to_binary(df_parcelas[feature], map_dict)

    return pd.merge(
        spine, df_parcelas, left_on="codparcela", right_index=True, how="left"
    )

def build_days_in_current_stage() -> pd.DataFrame():
    """
    Builds the number of days in the current growth-stage. (e.g. 0 if it just changed, the cumulative sum of days between overvations otherwise)
    """

    df = pd.read_parquet("subset_muestreos_parcelas.parquet")
    df = keep_valid_estados(df)
    df["estado_actual"] = df[estados].apply(find_estado_with_value_two, axis=1)
    df = df[["codparcela", "fecha", "estado_actual"]]

    # Sorting by date
    df["fecha"] = pd.to_datetime(df["fecha"])
    df.sort_values(by="fecha", inplace=True)

    df['prev_estado'] = df.groupby('codparcela')['estado_actual'].shift(1)
    df['prev_fecha'] = df.groupby('codparcela')['fecha'].shift(1)

    # Calculate the number of days for each row
    df['number_days_current_estado'] = (df['fecha'] - df['prev_fecha']).dt.days * (df['estado_actual'] == df['prev_estado'])
    df['number_days_current_estado'].fillna(0, inplace=True)
    df['number_days_current_estado'] = df['number_days_current_estado'].astype('int8')


    return df.drop(columns=["prev_estado","prev_fecha","estado_actual"])


def build_date_variables_parcelas(spine: pd.DataFrame()) -> pd.DataFrame():
    """
    Returns a dataset with the date relevant features for each field.
    """
    df = load_dataset()
    relevant_cols = estados + ["codparcela", "fecha"]
    df = df[relevant_cols]
    df_days_current_estado = build_days_in_current_stage()
    df = pd.merge(df, df_days_current_estado, on=["codparcela", "fecha"], how="left")

    return pd.merge(spine, df, on=["codparcela", "fecha"], how="left")


def preprocess_campaña(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()
    df2["campaña"] = df2["fecha"].dt.year
    return df2


def preprocess_comarca(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()
    # Sorting out the comarcas with I, II or III.
    replacements_comarcas = {
        "105_comarca": {
            "SIERRA DE CADIZ - II": "SIERRA DE CADIZ",
            "SIERRA DE CADIZ - III": "SIERRA DE CADIZ",
            "CAMPIÑA DE CADIZ - II": "CAMPIÑA DE CADIZ",
            "CAMPIÑA ALTA - III": "CAMPIÑA ALTA",
            "CAMPIÑA ALTA - II": "CAMPIÑA ALTA",
            "LA SIERRA - III": "LA SIERRA",
            "LA SIERRA - II": "LA SIERRA",
            "PEDROCHES - II": "PEDROCHES",
            "GUADIX - II": "GUADIX",
            "COSTA - II": "COSTA",
            "SIERRA MORENA - II": "SIERRA MORENA",
            "SIERRA SUR - II": "SIERRA SUR",
            "SIERRA DE CAZORLA - II": "SIERRA DE CAZORLA",
            "SIERRA DE CAZORLA - III": "SIERRA DE CAZORLA",
            "CAMPIÑA DEL NORTE - III": "CAMPIÑA DEL NORTE",
            "CAMPIÑA DEL NORTE - II": "CAMPIÑA DEL NORTE",
            "MAGINA - II": "MAGINA",
            "SERRANIA DE RONDA - II": "SERRANIA DE RONDA",
            "LA SIERRA NORTE - II": "LA SIERRA NORTE",
            "LA SIERRA SUR - II": "LA SIERRA SUR",
        }
    }
    df2.replace(replacements_comarcas, inplace=True)

    # Sort comarcas of some municipios.
    replacement_municipios = {
        "antequera": "NORTE O ANTEQUERA",
        "mollina": "NORTE O ANTEQUERA",
        "torredonjimeno": "CAMPIÑA DEL SUR",
        "fuente palmera": "LAS COLONIAS",
        "santaella": "CAMPIÑA BAJA",
        "guadalcazar": "LAS COLONIAS",
        "palma del rio": "CAMPIÑA BAJA",
        "luque": "PENIBETICA",
        "jayena": "ALHAMA",
        "villanueva mesia": "DE LA  VEGA",
        "iznalloz": "IZNALLOZ",
        "pinar": "IZNALLOZ",
        "escuzar": "ALHAMA",
        "campotejar": "IZNALLOZ",
        "montefrio": "MONTEFRIO",
        "iznajar": "PENIBETICA",
        "lucena": "CAMPIÑA ALTA",
        "pozoblanco": "PEDROCHES",
        "obejo": "LA SIERRA",
        "torreperogil": "LA LOMA",
        "ubeda": "LA LOMA",
        "fuente de piedra": "NORTE O ANTEQUERA",
        "montejicar": "IZNALLOZ",
        "puente genil": "CAMPIÑA ALTA",
        "aguilar": "CAMPIÑA ALTA",
        "villanueva del trabuco": "NORTE O ANTEQUERA",
        "archidona": "NORTE O ANTEQUERA",
        "villanueva de algaidas": "NORTE O ANTEQUERA",
        "guadahortuna": "IZNALLOZ",
        "villanueva del rosario": "NORTE O ANTEQUERA",
        "santisteban del puerto": "EL CONDADO",
        "navas de san juan": "EL CONDADO",
        "montillana": "IZNALLOZ",
        "villacarrillo": "LA LOMA",
        "iznatoraf": "LA LOMA",
        "alcaudete": "CAMPIÑA DEL SUR",
        "priego de cordoba": "PENIBETICA",
        "jimena": "MAGINA",
        "torreblascopedro": "CAMPIÑA DEL NORTE",
    }
    df2["105_comarca"].fillna(
        df2["municipio"].map(replacement_municipios), inplace=True
    )
    return df2


def preprocess_orientacion(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an orientation
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["203_orientación"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "203_orientación"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "203_orientación": {
            "2 - NE": "NE",
            "1 - N": "N",
            "4 - SE": "SE",
            "5 - S": "S",
            "3 - E": "E",
            "0 - Llana": "LLANO",
            "8 - NO": "NO",
            "6 - SO": "SO",
            "7 - O": "O",
            "9 - Varias": "SIN DEFINIR",
            "N-S": "SIN DEFINIR",
            "X": "SIN DEFINIR",
            "EO": "SIN DEFINIR",
            "1/2 N 1/2 S": "SIN DEFINIR",
            "LL": "LLANO",
            "LLANA": "LLANO",
            "ESTE": "E",
            "NS": "SIN DEFINIR",
            "E-NE": "NE",
            "E-O": "SIN DEFINIR",
            "SUROESTE": "SO",
            "N-E": "NE",
            "llano": "LLANO",
            "SUR ESTE": "SE",
            "SURESTE": "SE",
            "N-O": "NO",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    # Null values in pendiente means LLano, non null values in pendiente means No definido
    for index, row in df2.iterrows():
        if row["203_orientación"] is np.nan:
            if row["202_pendiente_(%)"] is np.nan:
                df2.at[index, "203_orientación"] = "LLANO"
            else:
                df2.at[index, "203_orientación"] = "SIN DEFINIR"

    return df2


def preprocess_textura(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an orientation
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["204_textura_del_suelo"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "204_textura_del_suelo"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "204_textura_del_suelo": {
            "Franco-arcilloso": "FRANCO-ARCILLOSO",
            "FRANCO-ARCILLO-ARENOSO": "FRANCO",
            "Calizas y margas (localmente areniscas o radiolari": "FRANCO",
            "FRANCO-ARCILLO-LIMOSO": "FRANCO",
            "ARENO-FRANCO": "FRANCO-ARENOSO",
            "Franco": "FRANCO",
            "Arcilloso": "ARCILLOSO",
            "Areno-franco": "FRANCO-ARENOSO",
            "Conglomerados, arenas, lutitas y calizas": "ARCILLO-ARENOSO",
            "Margas, margocalizas, calizas (localmente calcaren": "FRANCO",
            "Franco-arcillo-arenoso": "FRANCO",
            "Calcarenitas, arenas, margas y calizas": "FRANCO-ARENOSO",
            "Franco-arenoso": "FRANCO-ARENOSO",
            "Margas yesíferas, areniscas y calizas": "FRANCO-ARENOSO",
            "MEDIA": "SIN DEFINIR",
            "Margas, areniscas y lutitas o silexitas": "FRANCO-ARENOSO",
            "Franco-arcillo-limoso": "FRANCO",
            "Arcillo-arenoso": "ARCILLO-ARENOSO",
            "FRANCO ARCILLOSO": "FRANCO-ARCILLOSO",
            "Arenas, limos, arcillas, gravas y cantos": "ARENO-LIMOSO",
            "Limoso": "LIMOSO",
            "Arenoso": "ARENOSO",
            "FRANCO CON ELEMENTOS GRUESOS": "FRANCO",
            "Franco-Arenoso-Arcilloso": "FRANCO",
            "Margas, margocalizas, calizas(localmente calcaren)": "FRANCO",
            "Calizas y margas (localmente arenisca": "FRANCO-ARENOSO",
            "Arcillo-limoso": "ARCILLO-LIMOSO",
            "Areniscas, margas y lutitas": "FRANCO-ARENOSO",
            "FRANCO LIMOSO": "FRANCO-LIMOSO",
            "FRANCO - ARCILLO - ARENOSO": "FRANCO",
            "FRANCO-ARC": "FRANCO-ARCILLOSO",
            "Franco arcilloso": "FRANCO-ARCILLOSO",
            "FRANCO - ARENOSO": "FRANCO-ARENOSO",
            "calizo": "ARENOSO",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    # Null values in pendiente means LLano, non null values in pendiente means No definido

    df2["204_textura_del_suelo"] = df2["204_textura_del_suelo"].fillna("SIN DEFINIR")

    return df2


def preprocess_riego(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["206_secano_/_regadío"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "206_secano_/_regadío"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "206_secano_/_regadío": {
            "REGADÍO": "REGADIO",
            "Secano": "SECANO",
            "Regadío": "REGADIO",
            "Riego de apoyo": "RIEGO DE APOYO",
            "SECANO CON RIEGO DE APOYO": "RIEGO DE APOYO",
            "RIEGO": "SIN DEFINIR",
            "RlEGO": "SIN DEFINIR",
            "Regadio": "REGADIO",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    # Null values in pendiente means LLano, non null values in pendiente means No definido

    df2["206_secano_/_regadío"] = df2["206_secano_/_regadío"].fillna("SIN DEFINIR")

    return df2


def preprocess_cubierta(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["212_tipo_de_cubierta_vegetal"]
        .first()
        .dropna()
        .to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[
                index, "212_tipo_de_cubierta_vegetal"
            ] = dataframe_nonnull_perparcela[row["codparcela"]]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "212_tipo_de_cubierta_vegetal": {
            "Silvestre": "SILVESTRE",
            "SILVESTRE FORMADA CON HERBICIDA": "SILVESTRE",
            "ESPONTANEA": "SILVESTRE",
            "Inerte ( restos poda, ..)": "RESTOS PODA",
            "SILVESTRE FORMADA CON LABORES": "SILVESTRE",
            "Instalada con siembra": "SIEMBRA",
            "INSTALADA CON SIEMBRA": "SIEMBRA",
            "INERTE (RESTOS DE PODA)": "RESTOS PODA",
            "NO": "SIN DEFINIR",
            "NATURAL": "SILVESTRE",
            "CONTROLADA CON GANADO": "SILVESTRE",
            "RESTOS VEGETALES": "RESTOS PODA",
            "SILVESTRE DESBROZADORA (PARCELAS)": "SILVESTRE",
            "SILVESTRE FORMADA CON HERBICIDA Y RESTOS DE PODA": "SILVESTRE",
            "PARCIAL": "SILVESTRE",
            "PIEDRA NATURAL": "SIN DEFINIR",
            "SI": "SILVESTRE",
            "EXPONTANEA": "SILVESTRE",
            "INSTALADA CON HERBICIDAS": "SILVESTRE",
            "RESTOS PODA": "RESTOS PODA",
            "CUBIERTA SILVESTRE CON DESBROZADORA": "SILVESTRE",
            "SILVESTRE CONTROLADA POR PASTOREO": "SILVESTRE",
            "SE INSTALARA EN SEPTIENBRE 2006": "SIN DEFINIR",
            "SILVESTRE FORMADA CON GANADO OVINO": "SILVESTRE",
            "FLORA NATURAL": "SILVESTRE",
            "NINGUNA": "SIN DEFINIR",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    df2["212_tipo_de_cubierta_vegetal"] = df2["212_tipo_de_cubierta_vegetal"].fillna(
        "SIN DEFINIR"
    )

    return df2


def preprocess_formacion(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["304_formación"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "304_formación"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "304_formación": {
            "Vaso": "FORMACION",
            "VASO": "FORMACION",
            "MARCO REAL": "FORMACION",
            "TRADICIONAL": "FORMACION",
            "vaso": "FORMACION",
            "Espaldera": "MANTENIMIENTO",
            "TRESBOLILLO": "FORMACION",
            "VAS0": "FORMACION",
            "Natural": "FORMACION",
            "PODA DE RENOVACIÓN": "MANTENIMIENTO",
            "EN VASO LIBRE": "FORMACION",
            "NATURAL": "FORMACION",
            "PRODUCCION Y REJUVENECIMIENTO": "REJUVENECIMIENTO",
            "MARCO TRESBOLILLO": "FORMACION",
            "LIBRE": "FORMACION",
            "cuadro": "FORMACION",
            "3 PIES": "FORMACION",
            "De Producción": "MANTENIMIENTO",
            "PODA DE FORMACIÓN": "FORMACION",
            "tresbolillo": "FORMACION",
            "ENBASO": "FORMACION",
            "TRADICIONAL VARIOS PIES": "FORMACION",
            "PODA JAEN": "REJUVENECIMIENTO",
            "2 PIES": "FORMACION",
            "FORMACION Y PRODUCCION": "MANTENIMIENTO",
            "FORMACION Y REJUVENECIMIENTO": "REJUVENECIMIENTO",
            "VARIABLE": "SIN DEFINIR",
            "VARIOS PIES": "SIN DEFINIR",
            "3 OROS": "SIN DEFINIR",
            "ALEATORIA": "FORMACION",
            "10 METROS": "SIN DEFINIR",
            "rectangulo": "SIN DEFINIR",
            "PODA DE RENOVACION": "REJUVENECIMIENTO",
            "Rejuvenecimiento": "REJUVENECIMIENTO",
            "TRADICIONAL 3-4 PIES": "SIN DEFINIR",
            "GLOBO": "FORMACION",
            "UN PIE": "SIN DEFINIR",
            "INTENSIVO": "MANTENIMIENTO",
            "De formación": "FORMACION",
            "Tradicional": "FORMACION",
            "ESTACAS A 3 PIES": "SIN DEFINIR",
            "GLOBOSA": "FORMACION",
            "Globosa": "FORMACION",
            "COPA": "FORMACION",
            "GARROTE TRADICIONAL": "SIN DEFINIR",
            "Estacas a 3 pies": "SIN DEFINIR",
            "ESTACAS A 2 PIES": "SIN DEFINIR",
            "PALANCAS 1 PIE": "SIN DEFINIR",
            "REAL": "FORMACION",
            "3 pies": "SIN DEFINIR",
            "PLANTACIÓN IRREGULAR": "SIN DEFINIR",
            "CINCO DE OROS": "SIN DEFINIR",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    df2["304_formación"] = df2["304_formación"].fillna("SIN DEFINIR")

    return df2


def preprocess_variedad_principal(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["308_variedad_principal"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "308_variedad_principal"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "308_variedad_principal": {
            "Picual, Marteño": "PICUAL",
            "Hojiblanca, Lucentino": "HOJIBLANCA",
            "PICUAL / MARTEÑO": "PICUAL",
            "HOJIBLANCO": "HOJIBLANCA",
            "Manzanilla de Sevilla": "MANZANILLA",
            "Picudo": "PICUDO",
            "Arbequina": "ARBEQUINA",
            "Lechín de Sevilla, Zorzaleño, Ecijano": "LECHIN",
            "MANZANILLO": "MANZANILLA",
            "Nevadillo Negro": "NEVADILLO NEGRO",
            "NEVADILLO BLANCO": "PICUAL",
            "Picual": "PICUAL",
            "Gordal Sevillana": "GORDAL",
            "Cornicabra": "CORNICABRA",
            "Verdial de Huévar": "VERDIAL",
            "NEVADILLO": "PICUAL",
            "PICUAL Y PICUDO": "PICUAL",
            "Lucio": "LUCIO",
            "Verdial de Vélez-Málaga": "VERDIAL",
            "ALOREÑA": "MANZANILLA",
            "PICUDO/PICUAL": "PICUDO",
            "PICUAL/MARTEÑO": "PICUAL",
            "Aceite+Mesa": "SIN DEFINIR",
            "Lechín de Sevilla": "LECHIN",
            "MARTEÑO": "PICUAL",
            "MARTEÑA": "PICUAL",
            "ARBEQUINA (15000 OLIVOS)": "ARBEQUINA",
            "MANZ. ZAHURD 87-C.ALIZN 72-COTO 86- DETR. NAVE 98": "MANZANILLA",
            "Royal de Cazorla": "SIN DEFINIR",
            "PICUDO Y MARTEÑO": "PICUDO",
            "Hojiblanca": "HOJIBLANCA",
            "Manzanilla Cacereña": "MANZANILLA",
            "Lechín de Granada, Cuquillo": "LECHIN",
            "PICUDO/HOJIBLANCO": "PICUDO",
            "Manzanilla de Huelva": "MANZANILLA",
            "Picual de Almería": "PICUAL",
            "VIDUEÑO": "SIN DEFINIR",
            "Mollar de Cieza": "SIN DEFINIR",
            "Frantoio": "SIN DEFINIR",
            "HOJIBLAANCO": "HOJIBLANCA",
            "Morona": "SIN DEFINIR",
            "Arbequino": "ARBEQUINA",
            "PICUAL, MARTEÑO": "PICUAL",
            "HOJIBLANCO/ARBEQUINO": "HOJIBLANCA",
            "Ocal": "SIN DEFINIR",
            "ZARZALEÑO": "LECHIN",
            "OTRAS": "SIN DEFINIR",
            "Pajarero": "SIN DEFINIR",
            "Gordal de Granada": "SIN DEFINIR",
            "Arbosana": "SIN DEFINIR",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    df2["308_variedad_principal"] = df2["308_variedad_principal"].fillna("SIN DEFINIR")

    return df2


def preprocess_zonabio(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["120_zona_biológica_raif"].first().dropna().to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[index, "120_zona_biológica_raif"] = dataframe_nonnull_perparcela[
                row["codparcela"]
            ]

    df2["120_zona_biológica_raif"] = df2["120_zona_biológica_raif"].cat.add_categories(
        "SIN DEFINIR"
    )

    df2["120_zona_biológica_raif"] = df2["120_zona_biológica_raif"].fillna(
        "SIN DEFINIR"
    )

    return df2


def preprocess_patron_variedad_principal(df: pd.DataFrame()) -> pd.DataFrame():
    df2 = df.copy()

    # Check for the parcelas that might have both null values and an secano/regadio
    dataframe_nonnull_perparcela = (
        df2.groupby("codparcela")["310_patrón_variedad_principal"]
        .first()
        .dropna()
        .to_dict()
    )
    for index, row in df2.iterrows():
        if row["codparcela"] in dataframe_nonnull_perparcela:
            df2.at[
                index, "310_patrón_variedad_principal"
            ] = dataframe_nonnull_perparcela[row["codparcela"]]

    # Sorting out homogeneization.
    replacements_orientacion = {
        "310_patrón_variedad_principal": {
            "PICUAL O MARTEÑA": "PICUAL",
            "Picual": "PICUAL",
            "Hojiblanca": "HOJIBLANCA",
            "Ninguno": "SIN DEFINIR",
            "NINGUNO": "SIN DEFINIR",
            "Acebuche": "ACEBUCHE",
            "Lechin de Sevilla": "LECHIN",
            "Manzanilla": "MANZANILLA",
            "NEVADILLO": "PICUAL",
            "Arbequina": "ARBEQUINA",
            "Verdial de Huevar": "VERDIAL",
            "NO": "SIN DEFINIR",
            "hojiblanco": "HOJIBLANCA",
            "HOHIBLANCA": "HOJIBLANCA",
            "NINGUNA": "SIN DEFINIR",
            "hojiblanca": "HOJIBLANCA",
            "I 18": "SIN DEFINIR",
            "AUTOENRAIZADA (NO INJERTADO)": "SIN DEFINIR",
            "PICO LIMON": "SIN DEFINIR",
        }
    }
    df2.replace(replacements_orientacion, inplace=True)

    df2["310_patrón_variedad_principal"] = df2["310_patrón_variedad_principal"].fillna(
        "SIN DEFINIR"
    )
    return df2


def build_categorical_features_parcela(spine: pd.DataFrame()) -> pd.DataFrame():
    """
    Returns a dataset with the categorical features for each field.
    """

    df = load_dataset()

    categorical_columns = [
        "campaña",
        "105_comarca",
        "203_orientación",
        "204_textura_del_suelo",
        "206_secano_/_regadío",
        "212_tipo_de_cubierta_vegetal",
        "304_formación",
        "308_variedad_principal",
        "120_zona_biológica_raif",
        "310_patrón_variedad_principal",
    ]

    df = df[
        categorical_columns + ["fecha", "codparcela", "municipio", "202_pendiente_(%)"]
    ]

    df = preprocess_campaña(df)
    df = preprocess_comarca(df)
    df = preprocess_cubierta(df)
    df = preprocess_formacion(df)
    df = preprocess_orientacion(df)
    df = preprocess_patron_variedad_principal(df)
    df = preprocess_zonabio(df)
    df = preprocess_riego(df)
    df = preprocess_textura(df)
    df = preprocess_variedad_principal(df)

    df = df[categorical_columns + ["fecha", "codparcela"]]

    return pd.merge(spine, df, on=["codparcela", "fecha"], how="left")
