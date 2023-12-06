import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from collections import Counter

def find_estado_with_value_two(row) -> int:
    """
    Returns numero del siguiente etado (yt+1). (e.g., 13)
    """
    for column in row.index:
        if row[column] == 2:
            number_growth_stage = int(column.split("_")[-1])
            return number_growth_stage


def get_valid_dataset(df: pd.DataFrame(), max_days_till_next_date: int) -> pd.DataFrame():
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

    excluded_columns = ['estado_actual','count_2s', 'next_date']

    return df.loc[:, [i for i in df.columns if i not in excluded_columns]]

def preprocess_campaña(data_frame):
  data_frame2=data_frame.copy()
  data_frame2.drop(columns=['campaña'])
  data_frame2['campaña']=data_frame2["fecha"].dt.year
  return data_frame2

def preprocess_comarca(data_frame):
  data_frame2=data_frame.copy()
  #Sorting out the comarcas with I, II or III.
  replacements_comarcas = {"105_comarca": {"SIERRA DE CADIZ - II": "SIERRA DE CADIZ",
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
                                  }}
  data_frame2.replace(replacements_comarcas, inplace=True)

  #Sort comarcas of some municipios.
  replacement_municipios={"antequera":"NORTE O ANTEQUERA",
                          "mollina":"NORTE O ANTEQUERA",
                          "torredonjimeno":"CAMPIÑA DEL SUR",
                          "fuente palmera":"LAS COLONIAS",
                          "santaella":"CAMPIÑA BAJA",
                          "guadalcazar":"LAS COLONIAS",
                          "palma del rio":"CAMPIÑA BAJA",
                          "luque":"PENIBETICA",
                          "jayena":"ALHAMA",
                          "villanueva mesia":"DE LA  VEGA",
                          "iznalloz":"IZNALLOZ",
                          "pinar":"IZNALLOZ",
                          "escuzar":"ALHAMA",
                          "campotejar":"IZNALLOZ",
                          "montefrio":"MONTEFRIO",
                          "iznajar":"PENIBETICA",
                          "lucena":"CAMPIÑA ALTA",
                          "pozoblanco":"PEDROCHES",
                          "obejo":"LA SIERRA",
                          "torreperogil":"LA LOMA",
                          "ubeda":"LA LOMA",
                          "fuente de piedra":"NORTE O ANTEQUERA",
                          "montejicar":"IZNALLOZ",
                          "puente genil":"CAMPIÑA ALTA",
                          "aguilar":"CAMPIÑA ALTA",
                          "villanueva del trabuco":"NORTE O ANTEQUERA",
                          "archidona":"NORTE O ANTEQUERA",
                          "villanueva de algaidas":"NORTE O ANTEQUERA",
                          "guadahortuna":"IZNALLOZ",
                          "villanueva del rosario":"NORTE O ANTEQUERA",
                          "santisteban del puerto":"EL CONDADO",
                          "navas de san juan":"EL CONDADO",
                          "montillana":"IZNALLOZ",
                          "villacarrillo":"LA LOMA",
                          "iznatoraf":"LA LOMA",
                          "alcaudete":"CAMPIÑA DEL SUR",
                          "priego de cordoba":"PENIBETICA",
                          "jimena":"MAGINA",
                          "torreblascopedro":"CAMPIÑA DEL NORTE"}
  data_frame2["105_comarca"].fillna(data_frame2['municipio'].map(replacement_municipios), inplace=True)
  return data_frame2

def preprocess_orientacion(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an orientation
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["203_orientación"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index, "203_orientación"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"203_orientación": {"2 - NE": "NE",
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
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)

  #Null values in pendiente means LLano, non null values in pendiente means No definido
  for index, row in data_frame2.iterrows():
    if row["203_orientación"] is np.nan:
      if row["202_pendiente_(%)"] is np.nan:
        data_frame2.at[index, "203_orientación"] = "LLANO"
      else:
        data_frame2.at[index, "203_orientación"] = "SIN DEFINIR"

  return data_frame2

def preprocess_textura(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an orientation
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["204_textura_del_suelo"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index, "204_textura_del_suelo"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"204_textura_del_suelo": {"Franco-arcilloso": "FRANCO-ARCILLOSO",
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
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)

  #Null values in pendiente means LLano, non null values in pendiente means No definido

  data_frame2["204_textura_del_suelo"] = data_frame2["204_textura_del_suelo"].fillna("SIN DEFINIR")
  """
  for index, row in data_frame2.iterrows():
    if row["204_textura_del_suelo"] is np.nan:
      data_frame2.at[index, "204_textura_del_suelo"] = "SIN DEFINIR"
  """

  return data_frame2

def preprocess_riego(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["206_secano_/_regadío"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index, "206_secano_/_regadío"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"206_secano_/_regadío": {"REGADÍO": "REGADIO",
                                  "Secano": "SECANO",
                                  "Regadío": "REGADIO",
                                  "Riego de apoyo": "RIEGO DE APOYO",
                                  "SECANO CON RIEGO DE APOYO": "RIEGO DE APOYO",
                                  "RIEGO": "SIN DEFINIR",
                                  "RlEGO": "SIN DEFINIR",
                                  "Regadio": "REGADIO",
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)

  #Null values in pendiente means LLano, non null values in pendiente means No definido

  data_frame2["206_secano_/_regadío"] = data_frame2["206_secano_/_regadío"].fillna("SIN DEFINIR")
  """
  for index, row in data_frame2.iterrows():
    if row["206_secano_/_regadío"] is np.nan:
      data_frame2.at[index, "206_secano_/_regadío"] = "SIN DEFINIR"
  """


  return data_frame2

def preprocess_cubierta(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["212_tipo_de_cubierta_vegetal"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index, "212_tipo_de_cubierta_vegetal"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"212_tipo_de_cubierta_vegetal": {"Silvestre": "SILVESTRE",
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
                                  "NINGUNA": "SIN DEFINIR"
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)


  data_frame2["212_tipo_de_cubierta_vegetal"] = data_frame2["212_tipo_de_cubierta_vegetal"].fillna("SIN DEFINIR")
  """
  for index, row in data_frame2.iterrows():
    if row["212_tipo_de_cubierta_vegetal"] is np.nan:
      data_frame2.at[index, "212_tipo_de_cubierta_vegetal"] = "SIN DEFINIR"
  """

  return data_frame2

def preprocess_formacion(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["304_formación"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index,"304_formación"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"304_formación": {"Vaso": "FORMACION",
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
                                  "VARIABLE":"SIN DEFINIR",
                                  "VARIOS PIES":"SIN DEFINIR",
                                  "3 OROS":"SIN DEFINIR",
                                  "ALEATORIA":"FORMACION",
                                  "10 METROS":"SIN DEFINIR",
                                  "rectangulo":"SIN DEFINIR",
                                   "PODA DE RENOVACION":"REJUVENECIMIENTO",
                                   "Rejuvenecimiento":"REJUVENECIMIENTO",
                                      "TRADICIONAL 3-4 PIES":"SIN DEFINIR",
                                      "GLOBO":"FORMACION",
                                      "UN PIE":"SIN DEFINIR",
                                       "INTENSIVO":"MANTENIMIENTO",
                                         "De formación":"FORMACION",
                                         "Tradicional":"FORMACION",
                                       "ESTACAS A 3 PIES":"SIN DEFINIR",
                                         "GLOBOSA":"FORMACION",
                                           "Globosa":"FORMACION",
                                           "COPA":"FORMACION",
                                           "GARROTE TRADICIONAL":"SIN DEFINIR",
                                           "Estacas a 3 pies":"SIN DEFINIR",
                                           "ESTACAS A 2 PIES":"SIN DEFINIR",
                                            "PALANCAS 1 PIE":"SIN DEFINIR",
                                            "REAL":"FORMACION",
                                            "3 pies":"SIN DEFINIR",
                                            "PLANTACIÓN IRREGULAR":"SIN DEFINIR",
                                            "CINCO DE OROS":"SIN DEFINIR",
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)

  data_frame2["304_formación"] = data_frame2["304_formación"].fillna("SIN DEFINIR")
  """
  for index, row in data_frame2.iterrows():
    if row["304_formación"] is np.nan:
      data_frame2.at[index, "304_formación"] = "SIN DEFINIR"
  """
  return data_frame2

def preprocess_varprinc(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["308_variedad_principal"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index,"308_variedad_principal"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"308_variedad_principal": {"Picual, Marteño": "PICUAL",
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
                                  "ARBEQUINA (15000 OLIVOS)":"ARBEQUINA",
                                  "MANZ. ZAHURD 87-C.ALIZN 72-COTO 86- DETR. NAVE 98":"MANZANILLA",
                                  "Royal de Cazorla":"SIN DEFINIR",
                                  "PICUDO Y MARTEÑO":"PICUDO",
                                  "Hojiblanca":"HOJIBLANCA",
                                  "Manzanilla Cacereña":"MANZANILLA",
                                  "Lechín de Granada, Cuquillo":"LECHIN",
                                  "PICUDO/HOJIBLANCO":"PICUDO",
                                  "Manzanilla de Huelva":"MANZANILLA",
                                  "Picual de Almería":"PICUAL",
                                  "VIDUEÑO":"SIN DEFINIR",
                                  "Mollar de Cieza":"SIN DEFINIR",
                                  "Frantoio":"SIN DEFINIR",
                                  "HOJIBLAANCO":"HOJIBLANCA",
                                  "Morona":"SIN DEFINIR",
                                  "Arbequino":"ARBEQUINA",
                                  "PICUAL, MARTEÑO":"PICUAL",
                                  "HOJIBLANCO/ARBEQUINO":"HOJIBLANCA",
                                  "Ocal":"SIN DEFINIR",
                                  "ZARZALEÑO":"LECHIN",
                                  "OTRAS":"SIN DEFINIR",
                                  "Pajarero":"SIN DEFINIR",
                                  "Gordal de Granada":"SIN DEFINIR",
                                  "Arbosana":"SIN DEFINIR",
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)

  data_frame2["308_variedad_principal"] = data_frame2["308_variedad_principal"].fillna("SIN DEFINIR")

  """
  for index, row in data_frame2.iterrows():
    if row["308_variedad_principal"] is np.nan:
      data_frame2.at[index, "308_variedad_principal"] = "SIN DEFINIR"
  """
  return data_frame2

def preprocess_zonabio(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["120_zona_biológica_raif"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index,"120_zona_biológica_raif"] = dataframe_nonnull_perparcela[row["codparcela"]]

  data_frame2["120_zona_biológica_raif"]=data_frame2["120_zona_biológica_raif"].cat.add_categories('SIN DEFINIR')
  """
  for index, row in data_frame2.iterrows():
    if row["120_zona_biológica_raif"] is np.nan:
      data_frame2.at[index, "120_zona_biológica_raif"] = "SIN DEFINIR"
  """

  data_frame2["120_zona_biológica_raif"] = data_frame2["120_zona_biológica_raif"].fillna("SIN DEFINIR")

  return data_frame2

def preprocess_patrvarprinc(data_frame):

  data_frame2=data_frame.copy()

  #Check for the parcelas that might have both null values and an secano/regadio
  dataframe_nonnull_perparcela=data_frame2.groupby('codparcela')["310_patrón_variedad_principal"].first().dropna().to_dict()
  for index, row in data_frame2.iterrows():
    if row["codparcela"] in dataframe_nonnull_perparcela:
      data_frame2.at[index,"310_patrón_variedad_principal"] = dataframe_nonnull_perparcela[row["codparcela"]]

  #Sorting out homogeneization.
  replacements_orientacion = {"310_patrón_variedad_principal": {"PICUAL O MARTEÑA": "PICUAL",
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
                                   "PICO LIMON": "SIN DEFINIR"
                                  }}
  data_frame2.replace(replacements_orientacion, inplace=True)
  """
  for index, row in data_frame2.iterrows():
    if row["310_patrón_variedad_principal"] is np.nan:
      data_frame2.at[index, "310_patrón_variedad_principal"] = "SIN DEFINIR"
  """

  data_frame2["310_patrón_variedad_principal"] = data_frame2["310_patrón_variedad_principal"].fillna("SIN DEFINIR")

  return data_frame2