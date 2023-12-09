from processing import build_spine, build_numeric_features_parcela, build_binary_features_parcela, build_date_variables_parcelas, build_categorical_features_parcela, build_binary_features_parcela
import numpy as np
import pandas as pd

def train_test_val_split(df:pd.DataFrame, train_size=0.7, test_size = 0.2):

    if train_size + test_size > 1:
        raise ValueError("train_size + test_size must be less than 1")
    
    print(df.columns)

    #Getting the dates to split the data
    df_date_cumsum = df.groupby('fecha')['next_y'].count().cumsum()/len(df)
    df_val_index = df_date_cumsum[df_date_cumsum>train_size].index[0]
    df_test_index = df_date_cumsum[df_date_cumsum> train_size + test_size].index[0]

    print(f'Train split from {df_date_cumsum.index[0]}, till {df_val_index}')
    print(f'Validation split from {df_val_index}, till {df_test_index}')
    print(f'Test split from {df_test_index}, till {df_date_cumsum.index[-1]}')

    #Splitting the dataset
    train_df = df.loc[df['fecha'] < df_val_index]
    val_df = df.loc[df['fecha'].between(df_val_index,df_test_index, inclusive="left")]
    test_df = df.loc[df['fecha'] >= df_test_index]

    train_df = train_df.drop(columns=['fecha','codparcela'])
    test_df = test_df.drop(columns=['fecha','codparcela'])
    val_df = val_df.drop(columns=['fecha','codparcela'])

    return train_df, val_df, test_df


def build_dataset_with_features(numeric_features = True, categorical_features= True, binary_features=True, date_parcela_features=True, climate_features=True):

    spine = build_spine()

    if numeric_features:
        df = build_numeric_features_parcela(spine)
    
    if categorical_features:
        df = build_categorical_features_parcela(df)
    
    if binary_features:
        df = build_binary_features_parcela(df)

    if date_parcela_features:
        df = build_date_variables_parcelas(df)
    
    if climate_features:
        pass
    
    return df

def split_into_x_y(df: pd.DataFrame):
    y_data = df['next_y']
    x_data = df.drop(columns=['next_y'])
    return x_data, y_data



