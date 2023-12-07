```python
import xarray as xr
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

```

# FUNCTIONS


```python
def describe_in_xarray(ds: xr.Dataset):
    print(f"\t\t Mean \t   Std \t     Min\tMax")
    for var_name, variable in ds.data_vars.items():
        mean = variable.mean().values
        std = variable.std().values
        min = variable.min().values
        max = variable.max().values
        print(f"{var_name:<10}     {mean:^7.3f}   {std:^7.3f}   {min:^7.3f}   {max:^7.3f}")
```


```python
def print_nan_calculatations(dsx: xr.Dataset):
    for var_name, data_array in dsx.data_vars.items():
        total_nan = data_array.isnull().sum().item()
        total_non_nan = data_array.notnull().sum().item()

        total_elements = data_array.size

        nan_percentage = (total_nan / total_elements) * 100
        non_nan_percentage = (total_non_nan / total_elements) * 100

        print(f"{var_name:<10} Total NaNs: {total_nan:^10} Total Non-Nan: {total_non_nan:^10} NaN Percentage: {nan_percentage:^4.2f}%   Non-NaN Percentage: {non_nan_percentage:^4.2f}%")
```

# STL1, TP, SWVL1, U10, V10 & SSR Exploratory Data Analysis


```python
# Little script to unzip the files and change the name from data.nc
! cd ../data/raw/other_variables_data/; for file in *.zip; do unzip "$file"; mv "data.nc" "${file%.zip}.nc"; done
```

    Archive:  METEO_DATA_01_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_01_2021.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_02_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_03_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_04_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_05_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_06_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_07_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_08_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_09_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_10_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_11_2018.zip
      inflating: data.nc                 
    Archive:  METEO_DATA_12_2018.zip
      inflating: data.nc                 



```python
path_SL_01_18 = "../data/raw/other_variables_data/METEO_DATA_01_2018.nc"

ds = xr.open_dataset(path_SL_01_18)
for i in range(1, 13):
    path_aux = f"../data/raw/other_variables_data/METEO_DATA_{i:02d}_2018.nc"
    with xr.open_dataset(path_aux) as ds_aux:
            ds = xr.concat([ds, ds_aux], dim="time")
```


```python
ds
```




<div><svg style="position: absolute; width: 0; height: 0; overflow: hidden">
<defs>
<symbol id="icon-database" viewBox="0 0 32 32">
<path d="M16 0c-8.837 0-16 2.239-16 5v4c0 2.761 7.163 5 16 5s16-2.239 16-5v-4c0-2.761-7.163-5-16-5z"></path>
<path d="M16 17c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
<path d="M16 26c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
</symbol>
<symbol id="icon-file-text2" viewBox="0 0 32 32">
<path d="M28.681 7.159c-0.694-0.947-1.662-2.053-2.724-3.116s-2.169-2.030-3.116-2.724c-1.612-1.182-2.393-1.319-2.841-1.319h-15.5c-1.378 0-2.5 1.121-2.5 2.5v27c0 1.378 1.122 2.5 2.5 2.5h23c1.378 0 2.5-1.122 2.5-2.5v-19.5c0-0.448-0.137-1.23-1.319-2.841zM24.543 5.457c0.959 0.959 1.712 1.825 2.268 2.543h-4.811v-4.811c0.718 0.556 1.584 1.309 2.543 2.268zM28 29.5c0 0.271-0.229 0.5-0.5 0.5h-23c-0.271 0-0.5-0.229-0.5-0.5v-27c0-0.271 0.229-0.5 0.5-0.5 0 0 15.499-0 15.5 0v7c0 0.552 0.448 1 1 1h7v19.5z"></path>
<path d="M23 26h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 22h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 18h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
</symbol>
</defs>
</svg>
<style>/* CSS stylesheet for displaying xarray objects in jupyterlab.
 *
 */

:root {
  --xr-font-color0: var(--jp-content-font-color0, rgba(0, 0, 0, 1));
  --xr-font-color2: var(--jp-content-font-color2, rgba(0, 0, 0, 0.54));
  --xr-font-color3: var(--jp-content-font-color3, rgba(0, 0, 0, 0.38));
  --xr-border-color: var(--jp-border-color2, #e0e0e0);
  --xr-disabled-color: var(--jp-layout-color3, #bdbdbd);
  --xr-background-color: var(--jp-layout-color0, white);
  --xr-background-color-row-even: var(--jp-layout-color1, white);
  --xr-background-color-row-odd: var(--jp-layout-color2, #eeeeee);
}

html[theme=dark],
body[data-theme=dark],
body.vscode-dark {
  --xr-font-color0: rgba(255, 255, 255, 1);
  --xr-font-color2: rgba(255, 255, 255, 0.54);
  --xr-font-color3: rgba(255, 255, 255, 0.38);
  --xr-border-color: #1F1F1F;
  --xr-disabled-color: #515151;
  --xr-background-color: #111111;
  --xr-background-color-row-even: #111111;
  --xr-background-color-row-odd: #313131;
}

.xr-wrap {
  display: block !important;
  min-width: 300px;
  max-width: 700px;
}

.xr-text-repr-fallback {
  /* fallback to plain text repr when CSS is not injected (untrusted notebook) */
  display: none;
}

.xr-header {
  padding-top: 6px;
  padding-bottom: 6px;
  margin-bottom: 4px;
  border-bottom: solid 1px var(--xr-border-color);
}

.xr-header > div,
.xr-header > ul {
  display: inline;
  margin-top: 0;
  margin-bottom: 0;
}

.xr-obj-type,
.xr-array-name {
  margin-left: 2px;
  margin-right: 10px;
}

.xr-obj-type {
  color: var(--xr-font-color2);
}

.xr-sections {
  padding-left: 0 !important;
  display: grid;
  grid-template-columns: 150px auto auto 1fr 20px 20px;
}

.xr-section-item {
  display: contents;
}

.xr-section-item input {
  display: none;
}

.xr-section-item input + label {
  color: var(--xr-disabled-color);
}

.xr-section-item input:enabled + label {
  cursor: pointer;
  color: var(--xr-font-color2);
}

.xr-section-item input:enabled + label:hover {
  color: var(--xr-font-color0);
}

.xr-section-summary {
  grid-column: 1;
  color: var(--xr-font-color2);
  font-weight: 500;
}

.xr-section-summary > span {
  display: inline-block;
  padding-left: 0.5em;
}

.xr-section-summary-in:disabled + label {
  color: var(--xr-font-color2);
}

.xr-section-summary-in + label:before {
  display: inline-block;
  content: '►';
  font-size: 11px;
  width: 15px;
  text-align: center;
}

.xr-section-summary-in:disabled + label:before {
  color: var(--xr-disabled-color);
}

.xr-section-summary-in:checked + label:before {
  content: '▼';
}

.xr-section-summary-in:checked + label > span {
  display: none;
}

.xr-section-summary,
.xr-section-inline-details {
  padding-top: 4px;
  padding-bottom: 4px;
}

.xr-section-inline-details {
  grid-column: 2 / -1;
}

.xr-section-details {
  display: none;
  grid-column: 1 / -1;
  margin-bottom: 5px;
}

.xr-section-summary-in:checked ~ .xr-section-details {
  display: contents;
}

.xr-array-wrap {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: 20px auto;
}

.xr-array-wrap > label {
  grid-column: 1;
  vertical-align: top;
}

.xr-preview {
  color: var(--xr-font-color3);
}

.xr-array-preview,
.xr-array-data {
  padding: 0 5px !important;
  grid-column: 2;
}

.xr-array-data,
.xr-array-in:checked ~ .xr-array-preview {
  display: none;
}

.xr-array-in:checked ~ .xr-array-data,
.xr-array-preview {
  display: inline-block;
}

.xr-dim-list {
  display: inline-block !important;
  list-style: none;
  padding: 0 !important;
  margin: 0;
}

.xr-dim-list li {
  display: inline-block;
  padding: 0;
  margin: 0;
}

.xr-dim-list:before {
  content: '(';
}

.xr-dim-list:after {
  content: ')';
}

.xr-dim-list li:not(:last-child):after {
  content: ',';
  padding-right: 5px;
}

.xr-has-index {
  font-weight: bold;
}

.xr-var-list,
.xr-var-item {
  display: contents;
}

.xr-var-item > div,
.xr-var-item label,
.xr-var-item > .xr-var-name span {
  background-color: var(--xr-background-color-row-even);
  margin-bottom: 0;
}

.xr-var-item > .xr-var-name:hover span {
  padding-right: 5px;
}

.xr-var-list > li:nth-child(odd) > div,
.xr-var-list > li:nth-child(odd) > label,
.xr-var-list > li:nth-child(odd) > .xr-var-name span {
  background-color: var(--xr-background-color-row-odd);
}

.xr-var-name {
  grid-column: 1;
}

.xr-var-dims {
  grid-column: 2;
}

.xr-var-dtype {
  grid-column: 3;
  text-align: right;
  color: var(--xr-font-color2);
}

.xr-var-preview {
  grid-column: 4;
}

.xr-index-preview {
  grid-column: 2 / 5;
  color: var(--xr-font-color2);
}

.xr-var-name,
.xr-var-dims,
.xr-var-dtype,
.xr-preview,
.xr-attrs dt {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-right: 10px;
}

.xr-var-name:hover,
.xr-var-dims:hover,
.xr-var-dtype:hover,
.xr-attrs dt:hover {
  overflow: visible;
  width: auto;
  z-index: 1;
}

.xr-var-attrs,
.xr-var-data,
.xr-index-data {
  display: none;
  background-color: var(--xr-background-color) !important;
  padding-bottom: 5px !important;
}

.xr-var-attrs-in:checked ~ .xr-var-attrs,
.xr-var-data-in:checked ~ .xr-var-data,
.xr-index-data-in:checked ~ .xr-index-data {
  display: block;
}

.xr-var-data > table {
  float: right;
}

.xr-var-name span,
.xr-var-data,
.xr-index-name div,
.xr-index-data,
.xr-attrs {
  padding-left: 25px !important;
}

.xr-attrs,
.xr-var-attrs,
.xr-var-data,
.xr-index-data {
  grid-column: 1 / -1;
}

dl.xr-attrs {
  padding: 0;
  margin: 0;
  display: grid;
  grid-template-columns: 125px auto;
}

.xr-attrs dt,
.xr-attrs dd {
  padding: 0;
  margin: 0;
  float: left;
  padding-right: 10px;
  width: auto;
}

.xr-attrs dt {
  font-weight: normal;
  grid-column: 1;
}

.xr-attrs dt:hover span {
  display: inline-block;
  background: var(--xr-background-color);
  padding-right: 10px;
}

.xr-attrs dd {
  grid-column: 2;
  white-space: pre-wrap;
  word-break: break-all;
}

.xr-icon-database,
.xr-icon-file-text2,
.xr-no-icon {
  display: inline-block;
  vertical-align: middle;
  width: 1em;
  height: 1.5em !important;
  stroke-width: 0;
  stroke: currentColor;
  fill: currentColor;
}
</style><pre class='xr-text-repr-fallback'>&lt;xarray.Dataset&gt;
Dimensions:    (longitude: 71, latitude: 41, time: 9504)
Coordinates:
  * longitude  (longitude) float32 -8.0 -7.9 -7.8 -7.7 ... -1.3 -1.2 -1.1 -1.0
  * latitude   (latitude) float32 39.0 38.9 38.8 38.7 ... 35.3 35.2 35.1 35.0
  * time       (time) datetime64[ns] 2018-01-01 ... 2018-12-31T23:00:00
Data variables:
    stl1       (time, latitude, longitude) float32 281.3 281.9 ... 279.2 278.9
    tp         (time, latitude, longitude) float32 0.001718 ... 1.766e-06
    swvl1      (time, latitude, longitude) float32 0.2324 0.3148 ... 0.3006
    u10        (time, latitude, longitude) float32 0.3585 0.5566 ... -0.5192
    v10        (time, latitude, longitude) float32 1.521 1.372 ... 1.832 1.876
    ssr        (time, latitude, longitude) float32 4.229e+06 ... 9.704e+06
Attributes:
    Conventions:  CF-1.6
    history:      2023-12-03 12:50:47 GMT by grib_to_netcdf-2.24.0: /opt/ecmw...</pre><div class='xr-wrap' style='display:none'><div class='xr-header'><div class='xr-obj-type'>xarray.Dataset</div></div><ul class='xr-sections'><li class='xr-section-item'><input id='section-c92ecca3-f72e-4bba-ab5b-658a7cdef435' class='xr-section-summary-in' type='checkbox' disabled ><label for='section-c92ecca3-f72e-4bba-ab5b-658a7cdef435' class='xr-section-summary'  title='Expand/collapse section'>Dimensions:</label><div class='xr-section-inline-details'><ul class='xr-dim-list'><li><span class='xr-has-index'>longitude</span>: 71</li><li><span class='xr-has-index'>latitude</span>: 41</li><li><span class='xr-has-index'>time</span>: 9504</li></ul></div><div class='xr-section-details'></div></li><li class='xr-section-item'><input id='section-7f2ef25a-83b3-4aaa-b577-6e19ffb36e60' class='xr-section-summary-in' type='checkbox'  checked><label for='section-7f2ef25a-83b3-4aaa-b577-6e19ffb36e60' class='xr-section-summary' >Coordinates: <span>(3)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>longitude</span></div><div class='xr-var-dims'>(longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>-8.0 -7.9 -7.8 ... -1.2 -1.1 -1.0</div><input id='attrs-922176ef-6345-4a28-b46f-5df03c67e80c' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-922176ef-6345-4a28-b46f-5df03c67e80c' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-42b99906-8f5e-4452-a259-b9d0f4048faf' class='xr-var-data-in' type='checkbox'><label for='data-42b99906-8f5e-4452-a259-b9d0f4048faf' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>degrees_east</dd><dt><span>long_name :</span></dt><dd>longitude</dd></dl></div><div class='xr-var-data'><pre>array([-8. , -7.9, -7.8, -7.7, -7.6, -7.5, -7.4, -7.3, -7.2, -7.1, -7. , -6.9,
       -6.8, -6.7, -6.6, -6.5, -6.4, -6.3, -6.2, -6.1, -6. , -5.9, -5.8, -5.7,
       -5.6, -5.5, -5.4, -5.3, -5.2, -5.1, -5. , -4.9, -4.8, -4.7, -4.6, -4.5,
       -4.4, -4.3, -4.2, -4.1, -4. , -3.9, -3.8, -3.7, -3.6, -3.5, -3.4, -3.3,
       -3.2, -3.1, -3. , -2.9, -2.8, -2.7, -2.6, -2.5, -2.4, -2.3, -2.2, -2.1,
       -2. , -1.9, -1.8, -1.7, -1.6, -1.5, -1.4, -1.3, -1.2, -1.1, -1. ],
      dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>latitude</span></div><div class='xr-var-dims'>(latitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>39.0 38.9 38.8 ... 35.2 35.1 35.0</div><input id='attrs-c19b8039-875c-44d4-af3f-ca0b49caba9a' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-c19b8039-875c-44d4-af3f-ca0b49caba9a' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-833e8233-e480-4a63-9003-63643b0dc811' class='xr-var-data-in' type='checkbox'><label for='data-833e8233-e480-4a63-9003-63643b0dc811' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>degrees_north</dd><dt><span>long_name :</span></dt><dd>latitude</dd></dl></div><div class='xr-var-data'><pre>array([39. , 38.9, 38.8, 38.7, 38.6, 38.5, 38.4, 38.3, 38.2, 38.1, 38. , 37.9,
       37.8, 37.7, 37.6, 37.5, 37.4, 37.3, 37.2, 37.1, 37. , 36.9, 36.8, 36.7,
       36.6, 36.5, 36.4, 36.3, 36.2, 36.1, 36. , 35.9, 35.8, 35.7, 35.6, 35.5,
       35.4, 35.3, 35.2, 35.1, 35. ], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>time</span></div><div class='xr-var-dims'>(time)</div><div class='xr-var-dtype'>datetime64[ns]</div><div class='xr-var-preview xr-preview'>2018-01-01 ... 2018-12-31T23:00:00</div><input id='attrs-6baec327-27af-4b56-a779-3532d4594ad1' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-6baec327-27af-4b56-a779-3532d4594ad1' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-e811ef69-007f-45ab-b146-f9b426691c0c' class='xr-var-data-in' type='checkbox'><label for='data-e811ef69-007f-45ab-b146-f9b426691c0c' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>long_name :</span></dt><dd>time</dd></dl></div><div class='xr-var-data'><pre>array([&#x27;2018-01-01T00:00:00.000000000&#x27;, &#x27;2018-01-01T01:00:00.000000000&#x27;,
       &#x27;2018-01-01T02:00:00.000000000&#x27;, ..., &#x27;2018-12-31T21:00:00.000000000&#x27;,
       &#x27;2018-12-31T22:00:00.000000000&#x27;, &#x27;2018-12-31T23:00:00.000000000&#x27;],
      dtype=&#x27;datetime64[ns]&#x27;)</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-aaa02a87-be36-4419-9e2d-ea39bb887f67' class='xr-section-summary-in' type='checkbox'  checked><label for='section-aaa02a87-be36-4419-9e2d-ea39bb887f67' class='xr-section-summary' >Data variables: <span>(6)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span>stl1</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>281.3 281.9 281.8 ... 279.2 278.9</div><input id='attrs-4426133b-ff28-43ef-8c1d-8e72637c9369' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-4426133b-ff28-43ef-8c1d-8e72637c9369' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-ed25f2b3-0293-4f86-8b09-b63a81620f5d' class='xr-var-data-in' type='checkbox'><label for='data-ed25f2b3-0293-4f86-8b09-b63a81620f5d' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>K</dd><dt><span>long_name :</span></dt><dd>Soil temperature level 1</dd><dt><span>standard_name :</span></dt><dd>surface_temperature</dd></dl></div><div class='xr-var-data'><pre>array([[[281.32974, 281.9022 , 281.75558, ..., 281.36298, 281.86517,
         282.27893],
        [281.92374, 281.943  , 281.7008 , ..., 281.13666, 281.39246,
         282.18936],
        [281.89615, 281.74197, 281.46274, ..., 281.2946 , 282.0507 ,
         282.30045],
        ...,
        [      nan,       nan,       nan, ..., 285.75186, 285.29654,
         284.99765],
        [      nan,       nan,       nan, ..., 285.06226, 284.87674,
         284.74976],
        [      nan,       nan,       nan, ..., 284.48602, 284.65228,
         284.65793]],

       [[280.70966, 281.29724, 281.13928, ..., 280.8423 , 281.33655,
         281.72308],
        [281.30518, 281.32065, 281.06296, ..., 280.5865 , 280.82074,
         281.64902],
        [281.25037, 281.06107, 280.76218, ..., 280.74063, 281.5436 ,
         281.79752],
...
        [      nan,       nan,       nan, ..., 280.94382, 280.40466,
         279.9984 ],
        [      nan,       nan,       nan, ..., 280.063  , 279.83267,
         279.61557],
        [      nan,       nan,       nan, ..., 279.66464, 279.66843,
         279.33655]],

       [[281.0046 , 281.69595, 281.5162 , ..., 275.12555, 276.69397,
         277.1999 ],
        [281.87   , 281.76428, 281.32895, ..., 274.87372, 275.4442 ,
         276.93637],
        [281.90512, 281.5438 , 280.99103, ..., 275.024  , 275.95584,
         276.32697],
        ...,
        [      nan,       nan,       nan, ..., 280.34653, 279.83267,
         279.43243],
        [      nan,       nan,       nan, ..., 279.52606, 279.29578,
         279.08282],
        [      nan,       nan,       nan, ..., 279.1825 , 279.19232,
         278.8642 ]]], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>tp</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>0.001718 0.001616 ... 1.766e-06</div><input id='attrs-d090fff6-1b5b-4bfe-88d4-4bf5db95e8f7' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-d090fff6-1b5b-4bfe-88d4-4bf5db95e8f7' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-e2b05539-f216-4422-a059-d0f32ffdb1ff' class='xr-var-data-in' type='checkbox'><label for='data-e2b05539-f216-4422-a059-d0f32ffdb1ff' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>m</dd><dt><span>long_name :</span></dt><dd>Total precipitation</dd></dl></div><div class='xr-var-data'><pre>array([[[1.7178971e-03, 1.6158447e-03, 1.5244223e-03, ...,
         1.2757257e-05, 4.9602240e-06, 7.0780516e-07],
        [1.6002525e-03, 1.4223680e-03, 1.2366883e-03, ...,
         2.8349459e-06, 7.0780516e-07, 0.0000000e+00],
        [1.6059224e-03, 1.3748854e-03, 1.1403039e-03, ...,
         1.4174730e-06, 0.0000000e+00, 0.0000000e+00],
        ...,
        [          nan,           nan,           nan, ...,
         4.3230131e-05, 4.8900023e-05, 5.0317496e-05],
        [          nan,           nan,           nan, ...,
         5.3152442e-05, 5.8114529e-05, 5.7404861e-05],
        [          nan,           nan,           nan, ...,
         6.1657280e-05, 6.4492226e-05, 6.1657280e-05]],

       [[0.0000000e+00, 0.0000000e+00, 0.0000000e+00, ...,
         0.0000000e+00, 0.0000000e+00, 0.0000000e+00],
        [0.0000000e+00, 0.0000000e+00, 0.0000000e+00, ...,
         0.0000000e+00, 0.0000000e+00, 0.0000000e+00],
        [0.0000000e+00, 0.0000000e+00, 0.0000000e+00, ...,
         0.0000000e+00, 0.0000000e+00, 0.0000000e+00],
...
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06]],

       [[1.7657876e-06, 1.7657876e-06, 1.7657876e-06, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [1.7657876e-06, 1.7657876e-06, 1.7657876e-06, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [1.7657876e-06, 1.7657876e-06, 1.7657876e-06, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        ...,
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06],
        [          nan,           nan,           nan, ...,
         1.7657876e-06, 1.7657876e-06, 1.7657876e-06]]], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>swvl1</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>0.2324 0.3148 ... 0.2936 0.3006</div><input id='attrs-b69918f2-7c76-4bf9-9b3e-f59e4814a3c4' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-b69918f2-7c76-4bf9-9b3e-f59e4814a3c4' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-6ec4009c-06af-4441-8600-1ff43105402d' class='xr-var-data-in' type='checkbox'><label for='data-6ec4009c-06af-4441-8600-1ff43105402d' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>m**3 m**-3</dd><dt><span>long_name :</span></dt><dd>Volumetric soil water layer 1</dd></dl></div><div class='xr-var-data'><pre>array([[[0.23239046, 0.31483507, 0.31907585, ..., 0.22792146,
         0.21722382, 0.20979512],
        [0.32660025, 0.3179494 , 0.30667013, ..., 0.19087365,
         0.16983178, 0.20504636],
        [0.31977528, 0.31594682, 0.30325398, ..., 0.17401366,
         0.2143893 , 0.21019271],
        ...,
        [       nan,        nan,        nan, ..., 0.33328536,
         0.3372022 , 0.33966124],
        [       nan,        nan,        nan, ..., 0.33303505,
         0.33270374, 0.33210737],
        [       nan,        nan,        nan, ..., 0.3203569 ,
         0.31813347, 0.32490692]],

       [[0.23205915, 0.31448168, 0.31873718, ..., 0.2277963 ,
         0.21712077, 0.20969942],
        [0.32624686, 0.3176402 , 0.30642718, ..., 0.19067487,
         0.16964772, 0.204958  ],
        [0.31944397, 0.3156744 , 0.30304044, ..., 0.17381488,
         0.21426412, 0.21009699],
...
        [       nan,        nan,        nan, ..., 0.30066177,
         0.30392373, 0.30741575],
        [       nan,        nan,        nan, ..., 0.30618408,
         0.30512834, 0.3047629 ],
        [       nan,        nan,        nan, ..., 0.2944627 ,
         0.29357615, 0.30056703]],

       [[0.21339443, 0.31170636, 0.3193943 , ..., 0.2462305 ,
         0.24641323, 0.24549961],
        [0.3138111 , 0.3158887 , 0.3092227 , ..., 0.22458117,
         0.21476825, 0.23909076],
        [0.31094164, 0.31259292, 0.3010069 , ..., 0.19911495,
         0.24029538, 0.23980811],
        ...,
        [       nan,        nan,        nan, ..., 0.30072266,
         0.3039846 , 0.30746314],
        [       nan,        nan,        nan, ..., 0.30624497,
         0.30518925, 0.3048238 ],
        [       nan,        nan,        nan, ..., 0.29451007,
         0.29363707, 0.30062792]]], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>u10</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>0.3585 0.5566 ... -0.5644 -0.5192</div><input id='attrs-f09d957e-79c3-4683-9c7d-afda9c1f1339' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-f09d957e-79c3-4683-9c7d-afda9c1f1339' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-df787f85-6bc1-4265-9052-ced70423e708' class='xr-var-data-in' type='checkbox'><label for='data-df787f85-6bc1-4265-9052-ced70423e708' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>m s**-1</dd><dt><span>long_name :</span></dt><dd>10 metre U wind component</dd></dl></div><div class='xr-var-data'><pre>array([[[ 3.5852790e-01,  5.5663109e-01,  7.4722147e-01, ...,
          5.1367292e+00,  4.9837027e+00,  4.8429356e+00],
        [ 2.8735304e-01,  5.3448772e-01,  7.8913546e-01, ...,
          5.1936684e+00,  5.0343161e+00,  4.8749638e+00],
        [-4.8589706e-03,  3.0672860e-01,  6.2029290e-01, ...,
          5.3858404e+00,  5.2423048e+00,  5.0430155e+00],
        ...,
        [           nan,            nan,            nan, ...,
          4.2383451e+00,  3.9326885e+00,  3.7832215e+00],
        [           nan,            nan,            nan, ...,
          3.6796229e+00,  3.7353761e+00,  3.8164365e+00],
        [           nan,            nan,            nan, ...,
          3.1280181e+00,  3.4728205e+00,  3.7950842e+00]],

       [[-4.0778732e-01, -1.6856098e-01,  5.9989214e-02, ...,
          4.8967113e+00,  4.7152157e+00,  4.5617943e+00],
        [-5.6397641e-01, -2.4962115e-01,  6.7897558e-02, ...,
          4.8579607e+00,  4.6535311e+00,  4.4819212e+00],
        [-7.5931168e-01, -4.1648638e-01, -7.0893288e-02, ...,
          4.8820810e+00,  4.6974220e+00,  4.5179033e+00],
...
        [           nan,            nan,            nan, ...,
         -1.3184845e+00, -1.2755998e+00, -1.2099446e+00],
        [           nan,            nan,            nan, ...,
         -9.6288335e-01, -9.6971452e-01, -9.4049227e-01],
        [           nan,            nan,            nan, ...,
         -6.3498646e-01, -6.7103994e-01, -6.6800386e-01]],

       [[-1.8095709e+00, -1.8387932e+00, -1.8691540e+00, ...,
          2.1673176e+00,  2.3525188e+00,  2.4033730e+00],
        [-1.7985650e+00, -1.8190587e+00, -1.8327210e+00, ...,
          2.1191199e+00,  2.2933152e+00,  2.2940741e+00],
        [-1.8923041e+00, -1.8611842e+00, -1.8300644e+00, ...,
          1.8272765e+00,  1.9866707e+00,  1.9912250e+00],
        ...,
        [           nan,            nan,            nan, ...,
         -1.1218982e+00, -1.1070973e+00, -1.0543454e+00],
        [           nan,            nan,            nan, ...,
         -8.3764493e-01, -8.3195227e-01, -7.8982663e-01],
        [           nan,            nan,            nan, ...,
         -5.7312626e-01, -5.6439751e-01, -5.1923579e-01]]], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>v10</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>1.521 1.372 1.226 ... 1.832 1.876</div><input id='attrs-914b1978-f1c1-4b07-959a-e33ab69ea1f9' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-914b1978-f1c1-4b07-959a-e33ab69ea1f9' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-e4d20689-28e2-45df-956e-0373763f9802' class='xr-var-data-in' type='checkbox'><label for='data-e4d20689-28e2-45df-956e-0373763f9802' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>m s**-1</dd><dt><span>long_name :</span></dt><dd>10 metre V wind component</dd></dl></div><div class='xr-var-data'><pre>array([[[ 1.5211203 ,  1.3715353 ,  1.2260971 , ..., -2.6497838 ,
         -2.4311826 , -2.2122848 ],
        [ 1.2660851 ,  1.1090951 ,  0.9565479 , ..., -3.085506  ,
         -2.8381722 , -2.5795827 ],
        [ 0.5406719 ,  0.42041153,  0.29748517, ..., -3.5698059 ,
         -3.3627565 , -3.152745  ],
        ...,
        [        nan,         nan,         nan, ...,  1.1597466 ,
          0.8463586 ,  0.5963591 ],
        [        nan,         nan,         nan, ...,  0.7749724 ,
          0.5522241 ,  0.35109884],
        [        nan,         nan,         nan, ...,  0.37657273,
          0.22906119,  0.08747375]],

       [[ 1.5759189 ,  1.4402554 ,  1.3093317 , ..., -2.5052345 ,
         -2.2013252 , -1.910449  ],
        [ 1.3786442 ,  1.2204692 ,  1.0670335 , ..., -2.8956366 ,
         -2.5538125 , -2.2297611 ],
        [ 0.6715959 ,  0.5516316 ,  0.4284091 , ..., -3.2833729 ,
         -3.0108614 , -2.7688596 ],
...
        [        nan,         nan,         nan, ...,  0.92651755,
          0.9807169 ,  1.0494989 ],
        [        nan,         nan,         nan, ...,  1.3958395 ,
          1.4395877 ,  1.4845512 ],
        [        nan,         nan,         nan, ...,  1.8180103 ,
          1.8542241 ,  1.8841187 ]],

       [[ 0.45670974,  0.26956433,  0.09675865, ...,  0.88835937,
          0.7017001 ,  0.78579396],
        [ 0.72916424,  0.45962626,  0.19203268, ...,  0.7466636 ,
          0.6827425 ,  0.87766534],
        [ 0.42827332,  0.1910605 , -0.05514507, ...,  0.15484665,
          0.36678272,  0.616877  ],
        ...,
        [        nan,         nan,         nan, ...,  1.0317565 ,
          1.1100174 ,  1.2009165 ],
        [        nan,         nan,         nan, ...,  1.4186858 ,
          1.4898982 ,  1.5513889 ],
        [        nan,         nan,         nan, ...,  1.7642971 ,
          1.8316208 ,  1.8760983 ]]], dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span>ssr</span></div><div class='xr-var-dims'>(time, latitude, longitude)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>4.229e+06 4.227e+06 ... 9.704e+06</div><input id='attrs-69fce031-690b-4887-a82a-d67f39688856' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-69fce031-690b-4887-a82a-d67f39688856' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-5bec15bd-2077-446a-8d66-d00aba996902' class='xr-var-data-in' type='checkbox'><label for='data-5bec15bd-2077-446a-8d66-d00aba996902' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>units :</span></dt><dd>J m**-2</dd><dt><span>long_name :</span></dt><dd>Surface net solar radiation</dd><dt><span>standard_name :</span></dt><dd>surface_net_downward_shortwave_flux</dd></dl></div><div class='xr-var-data'><pre>array([[[4229230.5, 4226818.5, 4154261.2, ..., 6756076. , 7077459. ,
         7400851.5],
        [4254957. , 4253952. , 4207724.5, ..., 6569557.5, 6747634.5,
         7192023. ],
        [4310229. , 4243099. , 4227421.5, ..., 6534384. , 6889533.5,
         6987616.5],
        ...,
        [      nan,       nan,       nan, ..., 8565588. , 8532827. ,
         8468108. ],
        [      nan,       nan,       nan, ..., 8489815. , 8584080. ,
         8597948. ],
        [      nan,       nan,       nan, ..., 8695026. , 8791702. ,
         8597546. ]],

       [[      0. ,       0. ,       0. , ...,       0. ,       0. ,
               0. ],
        [      0. ,       0. ,       0. , ...,       0. ,       0. ,
               0. ],
        [      0. ,       0. ,       0. , ...,       0. ,       0. ,
               0. ],
...
        [      nan,       nan,       nan, ..., 9836939. , 9734525. ,
         9598263. ],
        [      nan,       nan,       nan, ..., 9719597. , 9767680. ,
         9724805. ],
        [      nan,       nan,       nan, ..., 9924946. , 9979277. ,
         9704496. ]],

       [[8314444. , 8398979. , 8339440. , ..., 8268965.5, 8556939. ,
         8847690. ],
        [8278860. , 8388911. , 8409047. , ..., 8097639. , 8197449. ,
         8628108. ],
        [8338398.5, 8314444. , 8390994. , ..., 8088786.5, 8388217. ,
         8392209. ],
        ...,
        [      nan,       nan,       nan, ..., 9836939. , 9734525. ,
         9598263. ],
        [      nan,       nan,       nan, ..., 9719597. , 9767680. ,
         9724805. ],
        [      nan,       nan,       nan, ..., 9924946. , 9979277. ,
         9704496. ]]], dtype=float32)</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-ffd51322-76e2-4337-a84d-55e334a0ea90' class='xr-section-summary-in' type='checkbox'  ><label for='section-ffd51322-76e2-4337-a84d-55e334a0ea90' class='xr-section-summary' >Indexes: <span>(3)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-index-name'><div>longitude</div></div><div class='xr-index-preview'>PandasIndex</div><div></div><input id='index-181631e4-f959-456b-a3a1-fd835ad537ca' class='xr-index-data-in' type='checkbox'/><label for='index-181631e4-f959-456b-a3a1-fd835ad537ca' title='Show/Hide index repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-index-data'><pre>PandasIndex(Index([               -8.0,  -7.900000095367432,  -7.800000190734863,
        -7.699999809265137,  -7.599999904632568,                -7.5,
        -7.400000095367432,  -7.300000190734863,  -7.199999809265137,
        -7.099999904632568,                -7.0,  -6.900000095367432,
        -6.800000190734863,  -6.699999809265137,  -6.599999904632568,
                      -6.5,  -6.400000095367432,  -6.300000190734863,
        -6.199999809265137,  -6.099999904632568,                -6.0,
        -5.900000095367432,  -5.800000190734863,  -5.699999809265137,
        -5.599999904632568,                -5.5,  -5.400000095367432,
        -5.300000190734863,  -5.199999809265137,  -5.099999904632568,
                      -5.0,  -4.900000095367432,  -4.800000190734863,
        -4.699999809265137,  -4.599999904632568,                -4.5,
        -4.400000095367432,  -4.300000190734863,  -4.199999809265137,
        -4.099999904632568,                -4.0, -3.9000000953674316,
        -3.799999952316284,  -3.700000047683716, -3.5999999046325684,
                      -3.5, -3.4000000953674316,  -3.299999952316284,
        -3.200000047683716, -3.0999999046325684,                -3.0,
       -2.9000000953674316,  -2.799999952316284,  -2.700000047683716,
       -2.5999999046325684,                -2.5, -2.4000000953674316,
        -2.299999952316284,  -2.200000047683716, -2.0999999046325684,
                      -2.0,  -1.899999976158142, -1.7999999523162842,
       -1.7000000476837158,  -1.600000023841858,                -1.5,
        -1.399999976158142, -1.2999999523162842, -1.2000000476837158,
        -1.100000023841858,                -1.0],
      dtype=&#x27;float32&#x27;, name=&#x27;longitude&#x27;))</pre></div></li><li class='xr-var-item'><div class='xr-index-name'><div>latitude</div></div><div class='xr-index-preview'>PandasIndex</div><div></div><input id='index-d3b4b831-bf8c-4f70-8235-f18ecf684bb5' class='xr-index-data-in' type='checkbox'/><label for='index-d3b4b831-bf8c-4f70-8235-f18ecf684bb5' title='Show/Hide index repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-index-data'><pre>PandasIndex(Index([              39.0, 38.900001525878906,  38.79999923706055,
        38.70000076293945, 38.599998474121094,               38.5,
       38.400001525878906,  38.29999923706055,  38.20000076293945,
       38.099998474121094,               38.0, 37.900001525878906,
        37.79999923706055,  37.70000076293945, 37.599998474121094,
                     37.5, 37.400001525878906,  37.29999923706055,
        37.20000076293945, 37.099998474121094,               37.0,
       36.900001525878906,  36.79999923706055,  36.70000076293945,
       36.599998474121094,               36.5, 36.400001525878906,
        36.29999923706055,  36.20000076293945, 36.099998474121094,
                     36.0, 35.900001525878906,  35.79999923706055,
        35.70000076293945, 35.599998474121094,               35.5,
       35.400001525878906,  35.29999923706055,  35.20000076293945,
       35.099998474121094,               35.0],
      dtype=&#x27;float32&#x27;, name=&#x27;latitude&#x27;))</pre></div></li><li class='xr-var-item'><div class='xr-index-name'><div>time</div></div><div class='xr-index-preview'>PandasIndex</div><div></div><input id='index-5351f37b-17f8-40d0-874d-2086a76323e8' class='xr-index-data-in' type='checkbox'/><label for='index-5351f37b-17f8-40d0-874d-2086a76323e8' title='Show/Hide index repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-index-data'><pre>PandasIndex(DatetimeIndex([&#x27;2018-01-01 00:00:00&#x27;, &#x27;2018-01-01 01:00:00&#x27;,
               &#x27;2018-01-01 02:00:00&#x27;, &#x27;2018-01-01 03:00:00&#x27;,
               &#x27;2018-01-01 04:00:00&#x27;, &#x27;2018-01-01 05:00:00&#x27;,
               &#x27;2018-01-01 06:00:00&#x27;, &#x27;2018-01-01 07:00:00&#x27;,
               &#x27;2018-01-01 08:00:00&#x27;, &#x27;2018-01-01 09:00:00&#x27;,
               ...
               &#x27;2018-12-31 14:00:00&#x27;, &#x27;2018-12-31 15:00:00&#x27;,
               &#x27;2018-12-31 16:00:00&#x27;, &#x27;2018-12-31 17:00:00&#x27;,
               &#x27;2018-12-31 18:00:00&#x27;, &#x27;2018-12-31 19:00:00&#x27;,
               &#x27;2018-12-31 20:00:00&#x27;, &#x27;2018-12-31 21:00:00&#x27;,
               &#x27;2018-12-31 22:00:00&#x27;, &#x27;2018-12-31 23:00:00&#x27;],
              dtype=&#x27;datetime64[ns]&#x27;, name=&#x27;time&#x27;, length=9504, freq=None))</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-a15654c9-30f8-4d1e-a18a-abc97c9b1513' class='xr-section-summary-in' type='checkbox'  checked><label for='section-a15654c9-30f8-4d1e-a18a-abc97c9b1513' class='xr-section-summary' >Attributes: <span>(2)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><dl class='xr-attrs'><dt><span>Conventions :</span></dt><dd>CF-1.6</dd><dt><span>history :</span></dt><dd>2023-12-03 12:50:47 GMT by grib_to_netcdf-2.24.0: /opt/ecmwf/eccodes/bin/grib_to_netcdf -S param -o /cache/tmp/d24d01a1-52dd-4e9f-9ae4-26fd30d1df8f-adaptor.mars.internal-1701607838.3740206-13387-28-tmp.nc /cache/tmp/d24d01a1-52dd-4e9f-9ae4-26fd30d1df8f-adaptor.mars.internal-1701607184.0897892-13387-26-tmp.grib</dd></dl></div></li></ul></div></div>




```python
print("Dataset Info:")
print(ds.info())

```

    Dataset Info:
    xarray.Dataset {
    dimensions:
    	longitude = 71 ;
    	latitude = 41 ;
    	time = 9504 ;
    
    variables:
    	float32 longitude(longitude) ;
    		longitude:units = degrees_east ;
    		longitude:long_name = longitude ;
    	float32 latitude(latitude) ;
    		latitude:units = degrees_north ;
    		latitude:long_name = latitude ;
    	datetime64[ns] time(time) ;
    		time:long_name = time ;
    	float32 stl1(time, latitude, longitude) ;
    		stl1:units = K ;
    		stl1:long_name = Soil temperature level 1 ;
    		stl1:standard_name = surface_temperature ;
    	float32 tp(time, latitude, longitude) ;
    		tp:units = m ;
    		tp:long_name = Total precipitation ;
    	float32 swvl1(time, latitude, longitude) ;
    		swvl1:units = m**3 m**-3 ;
    		swvl1:long_name = Volumetric soil water layer 1 ;
    	float32 u10(time, latitude, longitude) ;
    		u10:units = m s**-1 ;
    		u10:long_name = 10 metre U wind component ;
    	float32 v10(time, latitude, longitude) ;
    		v10:units = m s**-1 ;
    		v10:long_name = 10 metre V wind component ;
    	float32 ssr(time, latitude, longitude) ;
    		ssr:units = J m**-2 ;
    		ssr:long_name = Surface net solar radiation ;
    		ssr:standard_name = surface_net_downward_shortwave_flux ;
    
    // global attributes:
    	:Conventions = CF-1.6 ;
    	:history = 2023-12-03 12:50:47 GMT by grib_to_netcdf-2.24.0: /opt/ecmwf/eccodes/bin/grib_to_netcdf -S param -o /cache/tmp/d24d01a1-52dd-4e9f-9ae4-26fd30d1df8f-adaptor.mars.internal-1701607838.3740206-13387-28-tmp.nc /cache/tmp/d24d01a1-52dd-4e9f-9ae4-26fd30d1df8f-adaptor.mars.internal-1701607184.0897892-13387-26-tmp.grib ;
    }None



```python
# Just to make sure that the date values are indeed correct (from January to December)
print(ds["time"].min().values)
print(ds["time"].max().values)
```

    2018-01-01T00:00:00.000000000
    2018-12-31T23:00:00.000000000



```python
# Resampling of the data so that the plots are not hourly
# I've also tried it with a 7 day resampling mean and while the plots are smoother it doesn't add that much info

num_time_steps_before = ds.time.size

ds = ds.sortby("time")  # Necessary to use resample based on time
ds = ds.resample(time='3D').mean()

num_time_steps_after = ds.time.size

print(f"Number of time registers before resampling: {num_time_steps_before}")
print(f"Number of time registers after resampling: {num_time_steps_after}")

```

    Number of time registers before resampling: 9504
    Number of time registers after resampling: 122



```python
# Because we don't have an specific .describe() method for xarray.Dataset
describe_in_xarray(ds)
```

    		 Mean 	   Std 	     Min	Max
    stl1           290.339    8.029    272.900   311.206
    tp              0.001     0.002     0.000     0.022 
    swvl1           0.261     0.104     0.015     0.498 
    u10             0.627     1.559    -10.060    8.920 
    v10             0.001     1.124    -7.012     6.333 
    ssr            7420109.500   3047484.750   1314466.125   14069236.000


U10 and V10 can take negative values basically because they represent the speed of the wind from a direction to another. U10 will take postive values from west to east and negative ones from east to west.


```python
print_nan_calculatations(ds)
```

    stl1       Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    tp         Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    swvl1      Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    u10        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    v10        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    ssr        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%


Given that there is a value for each hour, a mean or LCF for inputing NaNs might work, however, there is almost a 40% of missing values,
Do we need a high fidelity dataset? Can it make our model worse?
Straight NaN drop??


```python
for var_name, _ in ds.data_vars.items():
    ds[var_name] = ds[var_name].interpolate_na(dim="time", method="linear")

print_nan_calculatations(ds)
```

    stl1       Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    tp         Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    swvl1      Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    u10        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    v10        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%
    ssr        Total NaNs:   136884   Total Non-Nan:   218258   NaN Percentage: 38.54%   Non-NaN Percentage: 61.46%


The percentages are not changing, might be that there are not enought values to interpolate.
The .interpolate_na() method needs the value before and after the NaN value to interpolate, if not, it won't apply it


```python
# NaN distribution
variable = ds['swvl1']

nan_matrix = np.isnan(variable).sum(dim='longitude').T 

plt.figure(figsize=(12, 6))
sns.heatmap(nan_matrix, cmap='viridis', cbar=True)
plt.title('NaN Distribution in stl1 over Time and Latitude')
plt.xlabel('Time')
plt.ylabel('Latitude')
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_17_0.png)
    


The problem might just be that the NaNs are surrounded by at leat another NaN in most cases, which makes it impossible to use the mean of the previous and next values.
Maybe input the mean of each variable? Tendency to delta distribution therefore?


```python
# NaNs inputed for each variable mean
# for var_name, _ in ds.data_vars.items():
#     ds[var_name] = ds[var_name].fillna(ds[var_name].mean())
```


```python
# for var_name, _ in ds.data_vars.items():
#     ds[var_name] = ds[var_name].dropna(dim="time")
```


```python
for var in ds.data_vars:
    # Time series plot
    ds[var].mean(dim=['latitude', 'longitude']).plot()
    plt.title(f"{var} ({ds[var].long_name})")
    plt.show()

    # Value distribution plot
    ds[var].plot.hist(bins=20)  # Value of bins is arbitrary
    plt.title(f"Values distribution of {var} ({ds[var].long_name})")
    plt.xlabel(var)
    plt.ylabel('Frequency')
    plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_0.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_1.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_2.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_3.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_4.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_5.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_6.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_7.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_8.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_9.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_10.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_21_11.png)
    


# FAPAR NDVI Exploratory Data Analysis


```python
path_fapar = "../data/raw/003-00073-00-00_BIOPAR_FAPAR_V2_GLOBAL_2006_05_08_2021_09_23.txt"
path_ndvi = "../data/raw/003-00073-00-00_BIOPAR_NDVI_V2_GLOBAL_2006_05_08_2021_09_23.txt"
```


```python
# Even if the files are .txt, the csv method works just fine
# In this case, the data is separated by tabs, ther is an index column and the dates have to be parsed
ds_fapar = pd.read_csv(path_fapar, sep="\t", index_col=0, parse_dates=["date"])
ds_ndvi = pd.read_csv(path_ndvi, sep="\t", index_col=0, parse_dates=["date"])
```


```python
ds_fapar = ds_fapar.rename(columns={"value": "fapar"})
ds_ndvi = ds_ndvi.rename(columns={"value": "ndvi"})
ds_fapar.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>fapar</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>510</td>
      <td>510.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>2013-06-05 00:16:56.470588160</td>
      <td>0.252902</td>
    </tr>
    <tr>
      <th>min</th>
      <td>2006-05-10 00:00:00</td>
      <td>0.104000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>2009-11-22 12:00:00</td>
      <td>0.188000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>2013-06-05 00:00:00</td>
      <td>0.252000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2016-12-17 12:00:00</td>
      <td>0.315000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2020-06-30 00:00:00</td>
      <td>0.460000</td>
    </tr>
    <tr>
      <th>std</th>
      <td>NaN</td>
      <td>0.080305</td>
    </tr>
  </tbody>
</table>
</div>




```python
ds_ndvi.describe()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>ndvi</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>count</th>
      <td>528</td>
      <td>521.000000</td>
    </tr>
    <tr>
      <th>mean</th>
      <td>2013-09-05 08:13:38.181818368</td>
      <td>0.345190</td>
    </tr>
    <tr>
      <th>min</th>
      <td>2006-05-11 00:00:00</td>
      <td>0.220000</td>
    </tr>
    <tr>
      <th>25%</th>
      <td>2010-01-08 12:00:00</td>
      <td>0.300000</td>
    </tr>
    <tr>
      <th>50%</th>
      <td>2013-09-06 00:00:00</td>
      <td>0.340000</td>
    </tr>
    <tr>
      <th>75%</th>
      <td>2017-05-03 12:00:00</td>
      <td>0.380000</td>
    </tr>
    <tr>
      <th>max</th>
      <td>2021-01-01 00:00:00</td>
      <td>0.548000</td>
    </tr>
    <tr>
      <th>std</th>
      <td>NaN</td>
      <td>0.057982</td>
    </tr>
  </tbody>
</table>
</div>




```python
ds_fapar.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Index: 510 entries, 0 to 509
    Data columns (total 2 columns):
     #   Column  Non-Null Count  Dtype         
    ---  ------  --------------  -----         
     0   date    510 non-null    datetime64[ns]
     1   fapar   510 non-null    float64       
    dtypes: datetime64[ns](1), float64(1)
    memory usage: 12.0 KB



```python
ds_ndvi.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Index: 528 entries, 0 to 527
    Data columns (total 2 columns):
     #   Column  Non-Null Count  Dtype         
    ---  ------  --------------  -----         
     0   date    528 non-null    datetime64[ns]
     1   ndvi    521 non-null    float64       
    dtypes: datetime64[ns](1), float64(1)
    memory usage: 12.4 KB


NaNs?
In this case, the time between each value is 10 days apart, doing a mean or LCF wouldn't be a good solution due to the variability of the data.
The best solution is probably to drop them

FAPAR


```python
# for year in ds_fapar["date"].dt.year.unique():
#     plt.figure(figsize=(3, 2))
#     plt.plot(ds_fapar[ds_fapar["date"].dt.year == year]["fapar"], marker='o', linestyle='-')
#     plt.title(f"FAPAR values for {year}")
#     plt.xlabel("Date")
#     plt.ylabel("FAPAR Value")
#     plt.grid(True)
#     plt.show()


# Plot for each FAPAR values year

years = ds_fapar["date"].dt.year.unique()

# We'll show 4 year plots per row
num_cols = 4
num_rows = int(np.ceil(len(years) / num_cols))

plt.figure(figsize=(20, num_rows * 5))  # Adjust the figure size as needed

for i, year in enumerate(years):
    plt.subplot(num_rows, num_cols, i + 1)
    yearly_data = ds_fapar[ds_fapar["date"].dt.year == year]
    plt.plot(yearly_data['date'], yearly_data['fapar'], marker='o', linestyle='-')
    plt.title(f"FAPAR values for {year}")
    plt.xlabel("Date")
    plt.ylabel("FAPAR Value")
    plt.grid(True)

plt.tight_layout()
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_31_0.png)
    



```python
# FAPAR values over the years with and without year division

# Without year division
plt.figure(figsize=(12, 6))
plt.plot(ds_fapar['date'], ds_fapar['fapar'], marker='o', linestyle='-')
plt.title("FAPAR values over years")
plt.xlabel("Date")
plt.ylabel("FAPAR Value")
plt.grid(True)
plt.show()


# With year division
plt.figure(figsize=(12, 6))

for year in ds_fapar["date"].dt.year.unique().tolist():
    # Filter the data for each year and plot it on the same figure
    yearly_data = ds_fapar[ds_fapar["date"].dt.year == year]
    plt.plot(yearly_data['date'], yearly_data['fapar'], marker='o', linestyle='-', label=str(year))

plt.title("FAPAR values over years")
plt.xlabel("Date")
plt.ylabel("FAPAR Value")
plt.legend(title='Year')  # Without this, it's not possible to know which line corresponds to which year
plt.grid(True)
plt.show()

```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_32_0.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_32_1.png)
    



```python
# Histogram of distribution
plt.figure(figsize=(12, 6))
plt.hist(ds_fapar['fapar'], bins=20, color='blue', edgecolor='black')
plt.title('FAPAR Values Distribution')
plt.xlabel('FAPAR Value')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_33_0.png)
    


NDVI


```python
years = ds_ndvi["date"].dt.year.unique()
num_cols = 4
num_rows = int(np.ceil(len(years) / num_cols))

plt.figure(figsize=(20, num_rows * 5))  # Adjust the figure size as needed

for i, year in enumerate(years):
    plt.subplot(num_rows, num_cols, i + 1)
    yearly_data = ds_ndvi[ds_ndvi["date"].dt.year == year]
    plt.plot(yearly_data['date'], yearly_data['ndvi'], marker='o', linestyle='-')
    plt.title(f"ndvi values for {year}")
    plt.xlabel("Date")
    plt.ylabel("ndvi Value")
    plt.grid(True)

plt.tight_layout()
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_35_0.png)
    



```python
# ndvi values over the years with and without year division

# Without year division
plt.figure(figsize=(12, 6))
plt.plot(ds_ndvi['date'], ds_ndvi['ndvi'], marker='o', linestyle='-')
plt.title("ndvi values over years")
plt.xlabel("Date")
plt.ylabel("ndvi Value")
plt.grid(True)
plt.show()


# With year division
plt.figure(figsize=(12, 6))

for year in ds_ndvi["date"].dt.year.unique().tolist():
    # Filter the data for each year and plot it on the same figure
    yearly_data = ds_ndvi[ds_ndvi["date"].dt.year == year]
    plt.plot(yearly_data['date'], yearly_data['ndvi'], marker='o', linestyle='-', label=str(year))

plt.title("ndvi values over Years")
plt.xlabel("Date")
plt.ylabel("ndvi Value")
plt.grid(True)
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_36_0.png)
    



    
![png](initial_climate_data_eda_files/initial_climate_data_eda_36_1.png)
    



```python
# Histogram of distribution
plt.figure(figsize=(8, 6))
plt.hist(ds_ndvi['ndvi'], bins=20, color='blue', edgecolor='black')
plt.title('ndvi Values Distribution')
plt.xlabel('NDVI Value')
plt.ylabel('Frequency')
plt.grid(True)
plt.show()
```


    
![png](initial_climate_data_eda_files/initial_climate_data_eda_37_0.png)
    


# Conclusions

- STL1 measures the soil temperature level at an specific depth.

- SWVL1 represents the volumetric soil water layer, i.e. the amount of water in the soil. Each crop might need a different value for optimal growth

- SSR, surface net solar radiation is the difference between the incoming solar radiation and the reflected and emitter back to the atmosphere. Key to understand evapotransporation.

- STL1, SWVL1 and SSR show a clear seasonal cycle based on the Earth's rotation. The higher values of STL1 and SSR appear on the summer months, where the temperatures are higher, while the SWVL1 has it's lowest values during those months indicating that the soil has less moisture as a result of the evapotransporation of water from the soil due to higher solar radiation and lower precipitation rates.

- TP is the total precipitation.

- In Andalucía most rainfalls occur during autumn and winter (September to February) but there also are some notable peaks during april visible in the TP time series plot.

- U10 and V10 are the wind components (U --> from west to east ; V --> from south to north) at 10 metres from the ground. Affects agriculture pollination and like most of the other variables, could lead to crop damages in case of extreme values.

- Compared to the previous variables, TP, U10 and V10 show much more variability and clearly can't be explained by a phenomenom as simple and as consistent as the cycle of seasons.

- FAPAR measures the fraction of the solar radiation that is absorbed by the vegetation canopy (only the radiation within the limits of the photosynthesis wavelengths). Usually, higher FAPAR values indicate that the vegetation is healthy.

- FAPAR has a cyclic tendency for each year, making a sin function altogether.

- Over the years, we can clearly see the values have been growing consistently, probably due to climate change (CO2 fertilization effect).

- It will be a good idea to look at how FAPAR fluctuates when the olives are ready for haverst. Peak FAPAR values could provide some insight on the current state of the olive

- NDVI measures the normalized difference vegetation index. The difference refers the variation between near-infrared and red light given that the one is reflected and the later absorbed. It's a way to check the "sanity and robust" of the vegetation, being higher values better.

- NDVI is much more irregular than FAPAR thanks to how the data is collected and how it, therefore, reacts to instant environmental changes. However, it's still possible to see that on the mid months of the year, the values seem to be lower than the beggining / end parts. This could perfectly be related with the seasonal growth cycles of the vegetation.

- A lof of noise and outliers.
