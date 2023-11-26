from datetime import datetime
import logging
import os
import pandas as pd
import requests
import time
import utm


logger = logging.getLogger(__name__)
logger.level = logging.INFO



def utm_to_latlon(row: pd.Series):
    try:
        lat, lon = utm.to_latlon(row["102_coordenada_x_(utm)"], row["103_coordenada_y_(utm)"], 30, 'S')
    except:
        lat, lon = -9999999, -9999999

    return pd.Series([lat, lon], index=['lat', 'lon'])




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

            logger.info(f"API return code {response.status_code} cooloff at {call_count}")
            if call_count != (num_attempts - 1):
                time.sleep(cooloff)
                cooloff *= 2
                call_count += 1
                continue
            else:
                raise

    # We got through the loop without error so we've received a valid response
    return response

def getTimeseriesForPoint(covId:str, 
                          tsvBaseURL:str, 
                          start:datetime, 
                          end:datetime, 
                          lat:float, 
                          lon:float):
    
    tsURL = tsvBaseURL + covId + '/point'
    payload = {
        'lon': str(lon),
        'lat': str(lat),
        'startDate': start.strftime('%Y-%m-%d'),
        'endDate': end.strftime('%Y-%m-%d')
    }

    # Introduce a cooldown of 1 second between API calls
    response = request_with_cooloff(api_url=tsURL, payload=payload, num_attempts=15)
    timeseries = response.json()['results']
    
    return timeseries

def download_variable_parcela(variable:str, 
                              start_time:datetime,
                              end_time:datetime,
                              latitude:float,
                              longitude:float):
    
    raw_data = getTimeseriesForPoint(covId=variable,
                            tsvBaseURL='https://services.terrascope.be/timeseries/v1.0/ts/',
                                start=start_time,
                                end=end_time,
                                lat=latitude,
                                lon=longitude)
    
    df = pd.DataFrame()
    n_points = len(raw_data)
    dates = [raw_data[i]["date"] for i in range(n_points)]
    values = [raw_data[i]["result"]["average"] for i in range(n_points)]

    df["date"] = dates
    df["value"] = values
    
    return df


data_folder = "data/raw"

output_data_folder_name = "NVDI_FAPAR_data"

output_data_folder = f"{data_folder}/{output_data_folder_name}"

if not os.path.exists(output_data_folder):
        os.makedirs(output_data_folder)


sample_filename = "muestreos_parcelas.parquet"

df_samples = pd.read_parquet(f"{data_folder}/{sample_filename}")
df_samples["fecha"] = pd.to_datetime(df_samples["fecha"])
df_samples.sort_values(by="fecha", inplace=True)
df_samples['año'] = df_samples['fecha'].dt.year



# Realizar el groupby y las agregaciones
group_dates = df_samples.groupby('codparcela').agg({'fecha': ['min', 'max', 'count']}).reset_index()

# Renombrar las columnas resultantes
group_dates.columns = ['codparcela', 'fecha_primera_muestra', 'fecha_ultima_muestra', 'n_muestras']


group_dates.sort_values(by="n_muestras", ascending=False, inplace=True)

group_coords = df_samples.groupby('codparcela').agg({'municipio': 'first', 
                                      '102_coordenada_x_(utm)': 'first', 
                                      '103_coordenada_y_(utm)': 'first'}).reset_index()

df_parcelas  = pd.merge(group_dates, group_coords, on='codparcela', how='inner')


df_parcelas[['lat', 'lon']] = df_parcelas.apply(utm_to_latlon, axis=1)

df_parcelas[(df_parcelas["lat"] < 40) &
               (df_parcelas["lat"] > 35) &
               (df_parcelas["lat"] > -10) &
               (df_parcelas["lat"] < 0)]

df_parcelas["n_muestras_cumsum"] = df_parcelas["n_muestras"].cumsum()

df_parcelas.to_csv(f"{data_folder}/parcelas_download.txt", sep="\t")


for i, row in df_parcelas.iterrows():


    id_parcela = row["codparcela"]
    start_time = datetime.strptime(row["fecha_primera_muestra"].strftime("%d-%m-%Y"), "%d-%m-%Y")
    end_time =datetime.strptime(row["fecha_ultima_muestra"].strftime("%d-%m-%Y"), "%d-%m-%Y")

    start_time_document = start_time.strftime("%Y_%m_%d")
    end_time_document = end_time.strftime("%Y_%m_%d")

    lat_parcela = row["lat"]
    lon_parcela = row["lon"]

    cumsum = row["n_muestras_cumsum"]
    n_samples = row["n_muestras"] 
    

    variables_list = ["BIOPAR_FAPAR_V2_GLOBAL", "BIOPAR_NDVI_V2_GLOBAL"]


    for variable in variables_list:

        df_variable = download_variable_parcela(variable, 
                              start_time,
                              end_time,
                              lat_parcela,
                              lon_parcela)

        filename = f"{id_parcela}_{variable}_{start_time_document}_{end_time_document}.txt".replace("/", "")

        df_variable.to_csv(f"{output_data_folder}/{filename}", sep="\t")

    print(f"Parcela:{id_parcela}, cumsum samples={cumsum}")