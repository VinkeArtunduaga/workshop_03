import pandas as pd
from sklearn.model_selection import train_test_split

# Carga de los DataFrames
df_2015 = pd.read_csv("C:/Users/kevin/ETL/workshop_03/Data/2015.csv", delimiter=',')
df_2016 = pd.read_csv("C:/Users/kevin/ETL/workshop_03/Data/2016.csv", delimiter=',')
df_2017 = pd.read_csv("C:/Users/kevin/ETL/workshop_03/Data/2017.csv", delimiter=',')
df_2018 = pd.read_csv("C:/Users/kevin/ETL/workshop_03/Data/2018.csv", delimiter=',')
df_2019 = pd.read_csv("C:/Users/kevin/ETL/workshop_03/Data/2019.csv", delimiter=',')

# Transformaciones para cada DataFrame

# 2015
column_rename_2015 = {
    'Happiness Rank': 'happiness_rank',
    'Happiness Score': 'happiness_score',
    'Economy (GDP per Capita)': 'economy',
    'Health (Life Expectancy)': 'health',
    'Freedom': 'freedom',
    'Trust (Government Corruption)': 'government_corruption',
    'Country': 'country',
    'Generosity':'generosity',
    'Family':'social_support'
}

df_2015.rename(columns=column_rename_2015, inplace=True)

columns_to_drop_2015 = ['Standard Error', 'Region', 'Dystopia Residual']
df_2015 = df_2015.drop(columns=columns_to_drop_2015)
print("Transformaciones para 2015 completadas")

# 2016

column_rename_2016= {
    'Happiness Rank': 'happiness_rank',
    'Happiness Score': 'happiness_score',
    'Economy (GDP per Capita)': 'economy',
    'Health (Life Expectancy)': 'health',
    'Freedom': 'freedom',
    'Trust (Government Corruption)': 'government_corruption',
    'Country': 'country',
    'Generosity':'generosity',
    'Family':'social_support'
}

# Renombrando las columnas
df_2016.rename(columns=column_rename_2016, inplace=True)

columns_to_drop = ['Region','Lower Confidence Interval', 'Upper Confidence Interval','Dystopia Residual']
df_2016 = df_2016.drop(columns=columns_to_drop)

print("Transformaciones para 2016 completadas")

# 2017
column_rename_2017 = {
    'Happiness.Rank': 'happiness_rank',
    'Happiness.Score': 'happiness_score',
    'Economy..GDP.per.Capita.': 'economy',
    'Health..Life.Expectancy.': 'health',
    'Freedom': 'freedom',
    'Trust..Government.Corruption.': 'government_corruption',
    'Country': 'country',
    'Generosity':'generosity',
    'Family':'social_support'
}

df_2017.rename(columns=column_rename_2017, inplace=True)

columns_to_drop_2017 = ['Whisker.high', 'Whisker.low', 'Dystopia.Residual']
df_2017 = df_2017.drop(columns=columns_to_drop_2017)

print("Transformaciones para 2017 completadas")

# 2018
column_rename_2018 = {
    'Overall rank': 'happiness_rank',
    'Score': 'happiness_score',
    'GDP per capita': 'economy',
    'Healthy life expectancy': 'health',
    'Freedom to make life choices': 'freedom',
    'Perceptions of corruption': 'government_corruption',
    'Country or region': 'country',
    'Generosity':'generosity',
    'Social support':'social_support'
}

df_2018.rename(columns=column_rename_2018, inplace=True)
df_2018.dropna(subset=["government_corruption"], inplace=True)

print("Transformaciones para 2018 completadas")

# 2019
column_rename_2019 = {
    'Overall rank': 'happiness_rank',
    'Score': 'happiness_score',
    'GDP per capita': 'economy',
    'Healthy life expectancy': 'health',
    'Freedom to make life choices': 'freedom',
    'Perceptions of corruption': 'government_corruption',
    'Country or region': 'country',
    'Generosity':'generosity',
    'Social support':'social_support'
}

df_2019.rename(columns=column_rename_2019, inplace=True)
print("Transformaciones para 2019 completadas")

# Agregar la columna "year" a cada DataFrame
df_2015['year'] = 2015
df_2016['year'] = 2016
df_2017['year'] = 2017
df_2018['year'] = 2018
df_2019['year'] = 2019

dataframes = [df_2015, df_2016, df_2017, df_2018, df_2019]
df_final = pd.concat(dataframes)
print("Concatenación de DataFrames completada")

# Selecciona las características y la variable objetivo
X = df_final[["economy", "social_support", "health"]]
y = df_final["happiness_score"]

# Divide los datos en conjuntos de entrenamiento y prueba (70% entrenamiento, 30% prueba)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=230)

# Save the 30% of the test data to a CSV file
test_data_30_percent = pd.concat([X_test, y_test], axis=1)
test_data_30_percent.to_csv('testing_data.csv', index=False)
print("Guardado del 30% de los datos de prueba en 'testing_data.csv' completado")