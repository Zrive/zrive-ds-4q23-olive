import logging
import numpy as np
import pandas as pd
import requests
import time
import utm
from typing import Tuple


logger = logging.getLogger(__name__)
logger.level = logging.INFO


def utm_to_latlon(x_utm: float, y_utm: float) -> Tuple[float, float]:
    try:
        lat, lon = utm.to_latlon(x_utm, y_utm, 30, "S")
    except:  # noqa: E722
        lat, lon = -9999999, -9999999

    return pd.Series([lat, lon], index=["lat", "lon"])


def compute_lat_long_coordiantes(row: pd.Series) -> Tuple[float, float]:
    return utm_to_latlon(row["102_coordenada_x_(utm)"], row["103_coordenada_y_(utm)"])


def parcelas_from_samples(df_samples: pd.DataFrame) -> pd.DataFrame:
    df_samples["fecha"] = pd.to_datetime(df_samples["fecha"])
    df_samples.sort_values(by="fecha", inplace=True)
    df_samples["año"] = df_samples["fecha"].dt.year

    # Realizar el groupby y las agregaciones
    group_dates = (
        df_samples.groupby("codparcela")
        .agg({"fecha": ["min", "max", "count"]})
        .reset_index()
    )

    # Renombrar las columnas resultantes
    group_dates.columns = [
        "codparcela",
        "fecha_primera_muestra",
        "fecha_ultima_muestra",
        "n_muestras",
    ]

    group_dates.sort_values(by="n_muestras", ascending=False, inplace=True)

    group_coords = (
        df_samples.groupby("codparcela")
        .agg(
            {
                "municipio": "first",
                "102_coordenada_x_(utm)": "first",
                "103_coordenada_y_(utm)": "first",
            }
        )
        .reset_index()
    )

    df_parcelas = pd.merge(group_dates, group_coords, on="codparcela", how="inner")

    df_parcelas[["lat", "lon"]] = df_parcelas.apply(
        compute_lat_long_coordiantes, axis=1
    )

    df_parcelas[
        (df_parcelas["lat"] < 40)
        & (df_parcelas["lat"] > 35)
        & (df_parcelas["lat"] > -10)
        & (df_parcelas["lat"] < 0)
    ]

    df_parcelas["n_muestras_cumsum"] = df_parcelas["n_muestras"].cumsum()

    return df_parcelas


def request_with_cooloff(api_url: str, payload: str, num_attempts: int):
    cooloff = 1

    for call_count in range(cooloff):
        try:
            response = requests.get(api_url, params=payload)
            response.raise_for_status()
            break

        except requests.exceptions.ConnectionError as e:
            logger.info("API refused the connection")
            logger.warning(e)
            if call_count != (num_attempts - 1):
                time.sleep(cooloff)
                cooloff *= 2
                call_count += 1
                continue
            else:
                raise

        except requests.exceptions.HTTPError as e:
            logger.warning(e)
            if response.status_code == 404:
                raise

            logger.info(
                f"API return code {response.status_code} cooloff at {call_count}"
            )
            if call_count != (num_attempts - 1):
                time.sleep(cooloff)
                cooloff *= 2
                call_count += 1
                continue
            else:
                raise

    # We got through the loop without error so we've received a valid response
    return response

def replace_nullwithmean_remove_outliers(
    df: pd.DataFrame(), threshold=np.inf, is_int=True
) -> pd.DataFrame():
    df = df.copy()
    mean_value = df.mean()
    df.fillna(mean_value, inplace=True)
    df.loc[df > threshold] = mean_value
    if is_int:
        df = df.astype("int32")
    else:
        df = df.astype("float32")
    return df