```python
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 100)
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

df_estados = df[estados]
df_estados.info()
```

    <class 'pandas.core.frame.DataFrame'>
    Index: 581793 entries, 0 to 581792
    Data columns (total 14 columns):
     #   Column                Non-Null Count   Dtype  
    ---  ------                --------------   -----  
     0   estado_fenologico_1   35816 non-null   float32
     1   estado_fenologico_2   62000 non-null   float32
     2   estado_fenologico_3   56106 non-null   float32
     3   estado_fenologico_4   43386 non-null   float32
     4   estado_fenologico_5   37543 non-null   float32
     5   estado_fenologico_6   40460 non-null   float32
     6   estado_fenologico_7   43052 non-null   float32
     7   estado_fenologico_8   43815 non-null   float32
     8   estado_fenologico_9   109202 non-null  float32
     9   estado_fenologico_10  243629 non-null  float32
     10  estado_fenologico_11  99547 non-null   float32
     11  estado_fenologico_12  70410 non-null   float32
     12  estado_fenologico_13  33154 non-null   float32
     13  estado_fenologico_14  7441 non-null    float32
    dtypes: float32(14)
    memory usage: 35.5 MB



```python
df_missing_estados = df_estados.isnull().sum() / len(df)
df_missing_values_estados = pd.DataFrame({"Missing Percentage": df_missing_estados})
df_missing_values_estados.sort_values(by="Missing Percentage", ascending=False)
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
      <th>Missing Percentage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>estado_fenologico_14</th>
      <td>0.987210</td>
    </tr>
    <tr>
      <th>estado_fenologico_13</th>
      <td>0.943014</td>
    </tr>
    <tr>
      <th>estado_fenologico_1</th>
      <td>0.938439</td>
    </tr>
    <tr>
      <th>estado_fenologico_5</th>
      <td>0.935470</td>
    </tr>
    <tr>
      <th>estado_fenologico_6</th>
      <td>0.930456</td>
    </tr>
    <tr>
      <th>estado_fenologico_7</th>
      <td>0.926001</td>
    </tr>
    <tr>
      <th>estado_fenologico_4</th>
      <td>0.925427</td>
    </tr>
    <tr>
      <th>estado_fenologico_8</th>
      <td>0.924690</td>
    </tr>
    <tr>
      <th>estado_fenologico_3</th>
      <td>0.903564</td>
    </tr>
    <tr>
      <th>estado_fenologico_2</th>
      <td>0.893433</td>
    </tr>
    <tr>
      <th>estado_fenologico_12</th>
      <td>0.878978</td>
    </tr>
    <tr>
      <th>estado_fenologico_11</th>
      <td>0.828896</td>
    </tr>
    <tr>
      <th>estado_fenologico_9</th>
      <td>0.812301</td>
    </tr>
    <tr>
      <th>estado_fenologico_10</th>
      <td>0.581245</td>
    </tr>
  </tbody>
</table>
</div>




```python
df_estados = df_estados[
    ~df[estados].isnull().all(axis=1)
]  # Removing entries that are missing for all states, in total 12,959 entries removed.
```


```python
for i in estados:
    print(df[i].value_counts())
```

    estado_fenologico_1
    2.0    19398
    1.0    16417
    0.0        1
    Name: count, dtype: int64
    estado_fenologico_2
    2.0    38430
    1.0    23569
    0.0        1
    Name: count, dtype: int64
    estado_fenologico_3
    2.0    31928
    1.0    24178
    Name: count, dtype: int64
    estado_fenologico_4
    2.0    21855
    1.0    21531
    Name: count, dtype: int64
    estado_fenologico_5
    1.0    19448
    2.0    18095
    Name: count, dtype: int64
    estado_fenologico_6
    1.0    21749
    2.0    18710
    3.0        1
    Name: count, dtype: int64
    estado_fenologico_7
    1.0    23097
    2.0    19955
    Name: count, dtype: int64
    estado_fenologico_8
    1.0    24920
    2.0    18895
    Name: count, dtype: int64
    estado_fenologico_9
    2.0    83951
    1.0    25245
    3.0        4
    0.5        2
    Name: count, dtype: int64
    estado_fenologico_10
    2.0    207618
    1.0     36008
    5.0         1
    0.0         1
    3.0         1
    Name: count, dtype: int64
    estado_fenologico_11
    1.0    49982
    2.0    49565
    Name: count, dtype: int64
    estado_fenologico_12
    1.0     42270
    2.0     28139
    11.0        1
    Name: count, dtype: int64
    estado_fenologico_13
    1.0    23320
    2.0     9834
    Name: count, dtype: int64
    estado_fenologico_14
    1.0    6455
    2.0     986
    Name: count, dtype: int64


There are only 13 entries with weird inputs, could easily be discarted.


```python
# Count 1s and 2s
df_estados.loc[:, "count_1s"] = df_estados[estados].eq(1).sum(axis=1)
df_estados.loc[:, "count_2s"] = df_estados[estados].eq(2).sum(axis=1)

print(df_estados.count_1s.value_counts(normalize=True))
df_estados.count_2s.value_counts(normalize=True)
```

    count_1s
    0     0.496950
    1     0.388655
    2     0.103506
    3     0.009892
    4     0.000789
    5     0.000116
    6     0.000060
    7     0.000021
    9     0.000004
    11    0.000004
    8     0.000002
    10    0.000002
    Name: proportion, dtype: float64





    count_2s
    1    0.997389
    0    0.002602
    2    0.000009
    Name: proportion, dtype: float64



99.7% of the data entries only have one 2. For the 1's, around 50% contain zero, 39% contain one, and 11% two or more.

### Analysis of the out-of-pattern entries


```python
# Rows con más de un 2 entre todos los estados

print(len(df_estados[df_estados.count_2s == 2]))
df_estados[df_estados.count_2s >= 2]
```

    5





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
      <th>estado_fenologico_1</th>
      <th>estado_fenologico_2</th>
      <th>estado_fenologico_3</th>
      <th>estado_fenologico_4</th>
      <th>estado_fenologico_5</th>
      <th>estado_fenologico_6</th>
      <th>estado_fenologico_7</th>
      <th>estado_fenologico_8</th>
      <th>estado_fenologico_9</th>
      <th>estado_fenologico_10</th>
      <th>estado_fenologico_11</th>
      <th>estado_fenologico_12</th>
      <th>estado_fenologico_13</th>
      <th>estado_fenologico_14</th>
      <th>count_1s</th>
      <th>count_2s</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>36445</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>38016</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>77579</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>85540</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>2</td>
      <td>2</td>
    </tr>
    <tr>
      <th>86746</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>NaN</td>
      <td>0</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>




```python
# Rows con ningún 1 o 2.

print(len(df_estados[(df_estados.count_1s == 0) & (df_estados.count_2s == 0)]))
df_estados[(df_estados.count_1s == 0) & (df_estados.count_2s == 0)]
```

    8





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
      <th>estado_fenologico_1</th>
      <th>estado_fenologico_2</th>
      <th>estado_fenologico_3</th>
      <th>estado_fenologico_4</th>
      <th>estado_fenologico_5</th>
      <th>estado_fenologico_6</th>
      <th>estado_fenologico_7</th>
      <th>estado_fenologico_8</th>
      <th>estado_fenologico_9</th>
      <th>estado_fenologico_10</th>
      <th>estado_fenologico_11</th>
      <th>estado_fenologico_12</th>
      <th>estado_fenologico_13</th>
      <th>estado_fenologico_14</th>
      <th>count_1s</th>
      <th>count_2s</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>47776</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>5.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>74901</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>115979</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>124307</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>124308</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>124443</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>124444</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>3.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>474682</th>
      <td>0.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>



Very few entries have non 1 and 2 values. Even if kept, they would have no impact in a dataset with 580k entries.


```python
# Rows con ningún dos.
df_estados["row_sum_estados"] = df_estados[estados].sum(axis=1)

print(len(df_estados[df_estados.count_2s == 0]))
print(df_estados[df_estados.count_2s == 0].row_sum_estados.value_counts())
df_estados[df_estados.count_2s == 0]
```

    1480
    row_sum_estados
    1.0    1022
    2.0     351
    3.0      97
    4.0       7
    0.0       2
    5.0       1
    Name: count, dtype: int64





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
      <th>estado_fenologico_1</th>
      <th>estado_fenologico_2</th>
      <th>estado_fenologico_3</th>
      <th>estado_fenologico_4</th>
      <th>estado_fenologico_5</th>
      <th>estado_fenologico_6</th>
      <th>estado_fenologico_7</th>
      <th>estado_fenologico_8</th>
      <th>estado_fenologico_9</th>
      <th>estado_fenologico_10</th>
      <th>estado_fenologico_11</th>
      <th>estado_fenologico_12</th>
      <th>estado_fenologico_13</th>
      <th>estado_fenologico_14</th>
      <th>count_1s</th>
      <th>count_2s</th>
      <th>row_sum_estados</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>4289</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>4290</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>5926</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>11101</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>11102</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
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
    </tr>
    <tr>
      <th>569454</th>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>569653</th>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>569654</th>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>569655</th>
      <td>NaN</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2</td>
      <td>0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>569780</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>1</td>
      <td>0</td>
      <td>1.0</td>
    </tr>
  </tbody>
</table>
<p>1480 rows × 17 columns</p>
</div>



If we follow the approach of using the 2's as our y variable, for the rows without any 2, we could save 1022 out of the 1480 by just replacing the ones by two in the cases were there is ony one 1 in the entire row.

### Analysis of row-wise sum


```python
print(df_estados["row_sum_estados"].value_counts())
sns.histplot(df_estados["row_sum_estados"], discrete=True, stat="probability")
plt.xlim((0, 7))
```

    row_sum_estados
    2.0     283018
    3.0     220154
    4.0      58537
    5.0       5536
    1.0       1022
    6.0        444
    7.0         66
    8.0         34
    9.0         12
    13.0         3
    2.5          2
    11.0         2
    0.0          2
    10.0         1
    12.0         1
    Name: count, dtype: int64





    (0.0, 7.0)




    
![png](EDA_estados_fenologicos_files/EDA_estados_fenologicos_15_2.png)
    


50% of entries are just a 2 in one of the states, around 38% a 2 and a 1, and 10% a 2 and two 1s.

### Checking if the 1 and 2 values are always together


```python
df_estados[estados] = df_estados[estados].fillna(0)

values = [1, 2]

df_0_between_values = pd.DataFrame()

# Creating a pattern to find any row that has: value, ..., 0 , ...., value

for i in range(2, 12):
    pattern_mask = (
        df_estados[estados].isin(values)
        & df_estados[estados].shift(-1, axis=1).eq(0)
        & df_estados[estados].shift(-i, axis=1).isin(values)
    ).any(axis=1)

    df_0_between_values = pd.concat([df_0_between_values, df_estados[pattern_mask]])

df_0_between_values = df_0_between_values[
    ~df_0_between_values.index.duplicated(keep="first")
]  # Removing duplicates

df_0_between_values
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
      <th>estado_fenologico_1</th>
      <th>estado_fenologico_2</th>
      <th>estado_fenologico_3</th>
      <th>estado_fenologico_4</th>
      <th>estado_fenologico_5</th>
      <th>estado_fenologico_6</th>
      <th>estado_fenologico_7</th>
      <th>estado_fenologico_8</th>
      <th>estado_fenologico_9</th>
      <th>estado_fenologico_10</th>
      <th>estado_fenologico_11</th>
      <th>estado_fenologico_12</th>
      <th>estado_fenologico_13</th>
      <th>estado_fenologico_14</th>
      <th>count_1s</th>
      <th>count_2s</th>
      <th>row_sum_estados</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>7529</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>23864</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>26009</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>26266</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>29287</th>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
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
    </tr>
    <tr>
      <th>133696</th>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>145604</th>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>159045</th>
      <td>0.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2</td>
      <td>1</td>
      <td>4.0</td>
    </tr>
    <tr>
      <th>460411</th>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>1</td>
      <td>1</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>121260</th>
      <td>1.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>2</td>
      <td>1</td>
      <td>4.0</td>
    </tr>
  </tbody>
</table>
<p>285 rows × 17 columns</p>
</div>



Only in 285 rows it is not the case, i.e. less than 0.05% of the dataset.

### Distribution of 2s


```python
df_estados_mod = df_estados[estados].replace(1, 0)
df_estados_mod = df_estados_mod.replace(2, 1)

# Percentage of entries being 2s
df_estados_mod.sum(axis=0) / len(df) * 100
```




    estado_fenologico_1      3.334176
    estado_fenologico_2      6.605442
    estado_fenologico_3      5.487863
    estado_fenologico_4      3.756491
    estado_fenologico_5      3.110213
    estado_fenologico_6      3.216436
    estado_fenologico_7      3.429914
    estado_fenologico_8      3.247719
    estado_fenologico_9     14.431937
    estado_fenologico_10    35.687263
    estado_fenologico_11     8.519353
    estado_fenologico_12     4.838491
    estado_fenologico_13     1.690292
    estado_fenologico_14     0.169476
    dtype: float64



### Distribution of 1s


```python
df_estados_mod = df_estados[estados].replace(2, 0)

# Percentage of entries being 1s
df_estados_mod.sum(axis=0) / len(df) * 100
```




    estado_fenologico_1     2.821794
    estado_fenologico_2     4.051097
    estado_fenologico_3     4.155774
    estado_fenologico_4     3.700801
    estado_fenologico_5     3.342770
    estado_fenologico_6     3.738787
    estado_fenologico_7     3.969969
    estado_fenologico_8     4.283310
    estado_fenologico_9     4.341407
    estado_fenologico_10    6.190518
    estado_fenologico_11    8.591028
    estado_fenologico_12    7.267361
    estado_fenologico_13    4.008298
    estado_fenologico_14    1.109501
    dtype: float64




```python
df.year = pd.to_datetime(df['fecha']).dt.year
df.year.value_counts()
```




    fecha
    2009    48258
    2010    47529
    2011    45702
    2007    42174
    2019    40781
    2006    39960
    2008    39743
    2017    39644
    2020    39395
    2018    37548
    2015    33432
    2014    31318
    2012    31165
    2013    31144
    2016    30679
    2021     3217
    2005      103
    2003        1
    Name: count, dtype: int64


