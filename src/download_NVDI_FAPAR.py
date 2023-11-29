from datetime import datetime
import os
import pandas as pd
from typing import Tuple
from utils import parcelas_from_samples, request_with_cooloff


def get_timeseries_data_from_location(covId:str, 
                          tsvBaseURL:str, 
                          start:datetime, 
                          end:datetime, 
                          lat:float, 
                          lon:float):
    
    tsURL = f"{tsvBaseURL}{covId}/point"

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
    
    raw_data = get_timeseries_data_from_location(covId=variable,
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


def main():

    main_data_folder = "data"

    data_folder = f"{main_data_folder}/raw"


    output_data_folder_name = "NVDI_FAPAR_data"

    output_data_folder = f"{data_folder}/{output_data_folder_name}"

    if not os.path.exists(output_data_folder):
            os.makedirs(output_data_folder)


    sample_filename = "muestreos_parcelas.parquet"

    df_samples = pd.read_parquet(f"{main_data_folder}/{sample_filename}")



    df_parcelas = parcelas_from_samples(df_samples)

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
        

if __name__ == "__main__":
    main()