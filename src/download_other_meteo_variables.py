import calendar
import cdsapi
from dotenv import load_dotenv
import os


def get_days_of_month(year: str, month: str):
    _, days_in_month = calendar.monthrange(int(year), int(month))
    return [str(day) for day in range(1, days_in_month + 1)]


def main():
    data_folder = "data/raw"

    output_data_folder_name = "other_variables_data"

    output_data_folder = f"{data_folder}/{output_data_folder_name}"

    if not os.path.exists(output_data_folder):
        os.makedirs(output_data_folder)

    # Carga las variables de entorno desde el archivo .env
    load_dotenv()

    # Accede a la contraseña desde las variables de entorno
    CDS_API_KEY = os.getenv("CDS_API_KEY")

    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api/v2", key=CDS_API_KEY)

    years = [str(year) for year in range(2016, 2022)]  # 2016, 2017, etc..
    years.reverse()
    months = [str(number).zfill(2) for number in range(1, 13)]  # 01, 02, 03... 11, 12

    for year in years:
        for month in months:
            days_month = get_days_of_month(year, month)

            c.retrieve(
                "reanalysis-era5-land",
                {
                    "variable": [
                        "soil_temperature_level_1",
                        "total_precipitation",
                        "volumetric_soil_water_layer_1",
                        "10m_u_component_of_wind",
                        "10m_v_component_of_wind",
                        "surface_net_solar_radiation",
                    ],
                    "year": year,
                    "month": month,
                    "day": days_month,
                    "time": [
                        "00:00",
                        "01:00",
                        "02:00",
                        "03:00",
                        "04:00",
                        "05:00",
                        "06:00",
                        "07:00",
                        "08:00",
                        "09:00",
                        "10:00",
                        "11:00",
                        "12:00",
                        "13:00",
                        "14:00",
                        "15:00",
                        "16:00",
                        "17:00",
                        "18:00",
                        "19:00",
                        "20:00",
                        "21:00",
                        "22:00",
                        "23:00",
                    ],
                    "area": [
                        39,
                        -8,
                        35,
                        -1,
                    ],  # Area of Andalusia
                    "format": "netcdf.zip",
                },
                f"{output_data_folder}/METEO_DATA_{month}_{year}.zip",
            )


if __name__ == "__main__":
    main()
