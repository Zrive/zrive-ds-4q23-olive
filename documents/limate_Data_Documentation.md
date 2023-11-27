# Project Meteorological Data README

## Overview

This repository contains meteorological data relevant to the project, featuring seven primary variables:

1. **NVDI (Normalized Difference Vegetation Index):** Measures vegetation density.
2. **FAPAR (Fraction of Photosynthetically Active Radiation Absorbed by the Vegetation):** Describes the fraction of solar radiation absorbed by plants.
3. **Volumetric soil water layer 1:** Volume of water in soil layer 1 (0 - 7 cm).
4. **Total precipitation:** Cumulative precipitation.
5. **Soil temperature layer 1:** Temperature of the soil in layer 1 (0 - 7 cm).
6. **Surface net solar radiation:** Solar radiation absorbed by the Earth.
7. **Wind speed in meters per second.**

## Data Sources

The data variables have been sourced from two distinct channels:

1. **CLMS (Copernicus Land Monitoring System):** NVDI and FAPAR values were extracted for each smallholding via an API, utilizing smallholding positions (latitude and longitude), along with the first and last sample dates. The temporal resolution is 10 days, and the data have been stored in .csv format.
2. **ERA5-Land:** The remaining variables were extracted for an area covering Andalusia from 2006 to 2021 with hourly resolution, and the data have been stored in NetCDF4 format.
