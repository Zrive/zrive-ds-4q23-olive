```python
import pandas as pd

#pd.set_option("display.max_rows", 100)
#pd.set_option("display.max_columns", 100)
```


```python
df = pd.read_parquet("muestreos_parcelas.parquet")

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
```


```python
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

    excluded_columns = ['estado_actual','count_2s', 'next_date']

    return df.loc[:, [i for i in df.columns if i not in excluded_columns]]

```


```python
df_new = get_valid_dataset(df, 10)
```


```python
df_new
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
      <th>generated_muestreos</th>
      <th>codparcela</th>
      <th>provincia</th>
      <th>municipio</th>
      <th>fecha</th>
      <th>campaña</th>
      <th>poligono</th>
      <th>parcela</th>
      <th>recinto</th>
      <th>subrecinto</th>
      <th>...</th>
      <th>108_u_h_c_a_la_que_pertenece</th>
      <th>316_fecha_de_plantación_variedad_secundaria</th>
      <th>315_patrón_variedad_secundaria</th>
      <th>317_%_superficie_ocupada_variedad_secundaria</th>
      <th>306_altura_de_copa_(m)</th>
      <th>310_patrón_variedad_principal</th>
      <th>411_representa_a_la_u_h_c_(si/no)</th>
      <th>109_sistema_para_el_cumplimiento_gestión_integrada</th>
      <th>days_until_next_visit</th>
      <th>next_estado</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>388249</th>
      <td>2020-04-23 17:00:15</td>
      <td>001-00163-00-00</td>
      <td>malaga</td>
      <td>archidona</td>
      <td>2005-08-30</td>
      <td>2005</td>
      <td>1</td>
      <td>163</td>
      <td>0</td>
      <td>0</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7</td>
      <td>10</td>
    </tr>
    <tr>
      <th>374542</th>
      <td>2020-04-23 17:00:15</td>
      <td>001-00162-00-00</td>
      <td>malaga</td>
      <td>antequera</td>
      <td>2005-08-30</td>
      <td>2005</td>
      <td>1</td>
      <td>162</td>
      <td>0</td>
      <td>0</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>7</td>
      <td>10</td>
    </tr>
    <tr>
      <th>386907</th>
      <td>2020-04-23 17:00:15</td>
      <td>166-00500-00-00</td>
      <td>malaga</td>
      <td>antequera</td>
      <td>2005-08-31</td>
      <td>2005</td>
      <td>166</td>
      <td>500</td>
      <td>0</td>
      <td>0</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>5</td>
      <td>10</td>
    </tr>
    <tr>
      <th>375863</th>
      <td>2020-04-23 17:00:15</td>
      <td>011-00001-00-00</td>
      <td>malaga</td>
      <td>antequera</td>
      <td>2005-08-31</td>
      <td>2005</td>
      <td>11</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6</td>
      <td>10</td>
    </tr>
    <tr>
      <th>385226</th>
      <td>2020-04-23 17:00:15</td>
      <td>110-00004-00-00</td>
      <td>malaga</td>
      <td>antequera</td>
      <td>2005-08-31</td>
      <td>2005</td>
      <td>110</td>
      <td>4</td>
      <td>0</td>
      <td>0</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6</td>
      <td>10</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>558747</th>
      <td>2021-04-05 17:26:06</td>
      <td>004-00069-02-05</td>
      <td>sevilla</td>
      <td>marinaleda</td>
      <td>2021-03-24</td>
      <td>2021</td>
      <td>4</td>
      <td>69</td>
      <td>2</td>
      <td>5</td>
      <td>...</td>
      <td>DOCTORA</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>SI</td>
      <td>Producción Integrada (PI)</td>
      <td>7</td>
      <td>4</td>
    </tr>
    <tr>
      <th>558617</th>
      <td>2021-04-05 17:26:06</td>
      <td>002-00087-08-01</td>
      <td>sevilla</td>
      <td>marinaleda</td>
      <td>2021-03-24</td>
      <td>2021</td>
      <td>2</td>
      <td>87</td>
      <td>8</td>
      <td>1</td>
      <td>...</td>
      <td>MOSTAZARES</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>SI</td>
      <td>Producción Integrada (PI)</td>
      <td>7</td>
      <td>4</td>
    </tr>
    <tr>
      <th>554133</th>
      <td>2021-04-05 17:26:06</td>
      <td>012-00030-01-1</td>
      <td>sevilla</td>
      <td>gilena</td>
      <td>2021-03-24</td>
      <td>2021</td>
      <td>12</td>
      <td>30</td>
      <td>1</td>
      <td>1</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>Producción Integrada (PI)</td>
      <td>5</td>
      <td>3</td>
    </tr>
    <tr>
      <th>570933</th>
      <td>2021-04-05 17:26:06</td>
      <td>009-00033-02-02</td>
      <td>sevilla</td>
      <td>pedrera</td>
      <td>2021-03-24</td>
      <td>2021</td>
      <td>9</td>
      <td>33</td>
      <td>2</td>
      <td>2</td>
      <td>...</td>
      <td>MINERIA  CERRO DEL OJO</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>SI</td>
      <td>Producción Integrada (PI)</td>
      <td>7</td>
      <td>3</td>
    </tr>
    <tr>
      <th>570682</th>
      <td>2021-04-05 17:26:06</td>
      <td>006-00028-05-01</td>
      <td>sevilla</td>
      <td>pedrera</td>
      <td>2021-03-25</td>
      <td>2021</td>
      <td>6</td>
      <td>28</td>
      <td>5</td>
      <td>1</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>6</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
<p>485812 rows × 63 columns</p>
</div>



## EDA Diferencia Coordenadas para una misma parcela


```python
df_new[df_new["102_coordenada_x_(utm)"].isnull()].codparcela.value_counts()
```




    codparcela
    001-00048-00-50    171
    019-00160-00-10    109
    014-00018-00-00    106
    075-00019-00-00    106
    005-00044-00-00    101
                      ... 
    009-00107-01-01      0
    009-00106-03-01      0
    009-00106-02-00      0
    009-00106-00-00      0
    015-00013-01         0
    Name: count, Length: 5239, dtype: int64




```python
df_new[df_new['codparcela']=='001-00048-00-50']["102_coordenada_x_(utm)"].value_counts()
```




    102_coordenada_x_(utm)
    435359.0    40
    435109.0    19
    Name: count, dtype: int64




```python
df_new[df_new['codparcela']=='001-00048-00-50']["103_coordenada_y_(utm)"].value_counts()
```




    103_coordenada_y_(utm)
    4126802.0     40
    41326610.0    19
    Name: count, dtype: int64




```python
grouped = df_new.groupby('codparcela', observed=True)["102_coordenada_x_(utm)"]
df_new['max_UTC'] = grouped.transform('max')
df_new['min_UTC'] = grouped.transform('min')
df_new['difference'] = df_new['max_UTC'] - df_new['min_UTC']
```


```python
df_diff = df_new['difference'].value_counts()
df_diff[df_diff.index>10000].sum() / len(df)
```




    0.07452306920158888



In 8% of the dataset the parcels have a difference of more than 10km between observations in their x_coordinate.


```python
grouped = df_new.groupby('codparcela', observed=True)["103_coordenada_y_(utm)"]
df_new['max_UTC_y'] = grouped.transform('max')
df_new['min_UTC_y'] = grouped.transform('min')
df_new['difference_y'] = df_new['max_UTC_y'] - df_new['min_UTC_y']
```


```python
df_diff = df_new['difference_y'].value_counts()
df_diff[df_diff.index>10000].sum() / len(df)
```




    0.07906935972072542



In 8.5% of the dataset the parcels have a difference of more than 10km between observations in their y_coordinate.
