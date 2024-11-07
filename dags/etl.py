import pandas as pd
import sys
import os
from dotenv import load_dotenv
from decouple import config
from transform.charts import get_data


load_dotenv()
work_dir = os.getenv('WORK_DIR')


import pandas as pd
import os
import sys
from decouple import config
from sqlalchemy.orm import sessionmaker
from src.database.dbconnection import getconnection
from src.model.models import *

from transform.DimensionalModels import DimensionalModel
from transform.TransformData import *

import logging as log
from sqlalchemy.orm import sessionmaker, aliased
import json
from transform.TransformData import DataTransform, DataTransformCauseOfDeaths

from src.streaming.kafka_utils import kafka_producer

def extract_data_cardio(**kwargs):
    engine = getconnection()
    Session = sessionmaker(bind=engine)
    session = Session()
    log.info("Starting data extraction")
    table = aliased(CardioTrain)
    query = str(session.query(table).statement)
    df = pd.read_sql(query, con=engine)
    log.info(f"Finish the data extraction {df}")
    kwargs['ti'].xcom_push(key='cardio', value=df.to_json(orient='records'))

    return df.to_json(orient='records')

def validate_cardio(**kwargs):
    log.info("Starting Data validate")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="extract_cardio", key='cardio')
    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)

    
    kwargs['ti'].xcom_push(key='cardio_validate', value=df.to_json(orient='records'))

    return df.to_json(orient='records')


def transform_cardio_data(**kwargs):
    log.info("Starting Data transform")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="validate_cardio_data", key='cardio_validate')
    if str_data is None:
        log.error("No data found in XCom for 'cardio'")
        return

    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)
    log.info(f"Data is {df}")
    file = DataTransform(df)
    file.gender_by_category()
    file.cholesterol_by_category()
    file.gluc_by_category()
    file.bmi()
    file.days_to_age()
    file.StandardizeBloodPressure()
    file.CategorizeBMI()
    file.categorize_blood_pressure()
    file.CalculatePulsePressure()

    df = file.df.copy()


    result = {
        "source":"deaths",
        "data": df.to_dict(orient='records')
    }
    kwargs['ti'].xcom_push(key='transform_cardio_data', value=json.dumps(result))

    return json.dumps(result)



def extract_owid_data(**kwargs):
    air_pollution = get_data('https://ourworldindata.org/grapher/long-run-air-pollution')
    air_pollution.rename(columns={'entities': 'Country', 'years': 'Year', 'nox': 'nitrogen_oxide(NOx)' , 'so2': 'sulphur_dioxide(SO2)', 'co': 'carbon_monoxide(CO)', 'bc': 'black_carbon(BC)', 'nh3': 'ammonia(NH3)', 'nmvoc': 'non_methane_volatile_organic_compounds'}, inplace=True)

    gdp_per_capita = get_data('https://ourworldindata.org/grapher/gdp-per-capita-penn-world-table')
    gdp_per_capita.rename(columns={'entities': 'Country', 'years': 'Year','gdp_per_capita_penn_world_table': 'gdp_per_capita'}, inplace=True)

    obesity = get_data('https://ourworldindata.org/grapher/obesity-prevalence-adults-who-gho')
    obesity.rename(columns={'entities': 'Country', 'years': 'Year', 'obesity_prevalence_adults_who_gho': 'obesity_prevalence_percentage'}, inplace=True)

    diabetes = get_data('https://ourworldindata.org/grapher/diabetes-prevalence-who-gho')
    diabetes.rename(columns={'entities': 'Country', 'years': 'Year', 'diabetes_prevalence_who_gho': 'diabetes_prevalence_percentage'}, inplace=True)

    population = get_data('https://ourworldindata.org/grapher/population')
    population.rename(columns={'entities': 'Country', 'years': 'Year'}, inplace=True)

    merged_air = pd.merge(air_pollution, gdp_per_capita, left_on=['Country', 'Year'],
                        right_on=['Country', 'Year'], how='left')
    merged_obsity = pd.merge(merged_air, obesity, left_on=['Country', 'Year'],
                        right_on=['Country', 'Year'], how='left')
    merged_diabetes = pd.merge(merged_obsity, diabetes, left_on=['Country', 'Year'],
                        right_on=['Country', 'Year'], how='left')
    merged_df = pd.merge(merged_diabetes, population, left_on=['Country', 'Year'],
                        right_on=['Country', 'Year'], how='left')
    
    merged_df = merged_df.sort_values(by=['Country', 'Year'], ascending=[True, True])

    kwargs['ti'].xcom_push(key='owid', value=merged_df.to_json(orient='records'))

    return merged_df.to_json(orient='records')

def validate_api(**kwargs):

    log.info("Starting Data validate")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="extract_api", key='owid')
    if str_data is None:
        log.error("No data found in XCom for 'cardio'")
        return
    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)

    kwargs['ti'].xcom_push(key='owid_validate', value=df.to_json(orient='records'))

    return df.to_json(orient='records')



def transform_owid(**kwargs):
    log.info("Starting Data transform")
    ti = kwargs['ti']
    str_data = ti.xcom_pull(task_ids="validate_api_data", key='owid_validate')
    if str_data is None:
        log.error("No data found in XCom for 'cardio'")
        return
    json_df = json.loads(str_data)
    df = pd.json_normalize(data=json_df)
    file = DataTransformOwid(df)

    file.feautres_imputation()
  
    rename_columns = {
        'nitrogen_oxide(NOx)': 'nitrogen_oxide',
        'sulphur_dioxide(SO2)': 'sulphur_dioxide',
        'carbon_monoxide(CO)': 'carbon_monoxide',
        'black_carbon(BC)': 'black_carbon',
        'ammonia(NH3)': 'ammonia'
    }

    df = file.df
    df = df.rename(columns=rename_columns)

    result = {
        "source":"owid_transform",
        "data": df.to_dict(orient='records')
    }

    kwargs['ti'].xcom_push(key='owidtransform', value=json.dumps(result))

    return json.dumps(result)


    

def extract_data_deaths(**kwargs):
    engine = getconnection()
    Session = sessionmaker(bind=engine)
    session = Session()
    log.info("Starting data extraction")
    table = aliased(CauseOfDeaths)
    query = str(session.query(table).statement)
    df = pd.read_sql(query, con=engine)
    log.info(f"Finish the data extraction {df}")

    result = {
        "source":"deaths",
        "data": df.to_dict(orient='records')
    }
    kwargs['ti'].xcom_push(key='deaths', value=json.dumps(result))

    return json.dumps(result)


def validate_deaths(**kwargs):
    log.info("Starting Data Validate")
    ti = kwargs['ti']

    json_1 = ti.xcom_pull(task_ids="extract_deaths", key='deaths')
    if json_1 is None:
        log.error("No data found in XCom for 'cardio'")
        return
    data1 = json.loads(json_1)
    df = pd.DataFrame(data1["data"])
    result = {
            "source":"deaths",
            "data": df.to_dict(orient='records')
    }
    kwargs['ti'].xcom_push(key='deaths_validated', value=json.dumps(result))

    return json.dumps(result)



def merge(**kwargs):
    log.info("Starting data merge")
    # Pull data from XCom
    ti = kwargs["ti"]
    json_2 = ti.xcom_pull(task_ids="transform_api", key='owidtransform')
    json_1 = ti.xcom_pull(task_ids="validate_deaths_data", key='deaths_validated')
    if json_1 is None:
        log.error("No data found in XCom for 'transform_cardio'")
        return
    

    if json_2 is None:
        log.error("No data found in XCom for 'transform_deaths'")
        return

    
    data1 = json.loads(json_1)
    data2 = json.loads(json_2)


    df1 = pd.DataFrame(data1["data"])
    df2 = pd.DataFrame(data2["data"])

    log.info(df1.columns, df2.columns)

    merged_df = pd.merge(df1, df2, left_on=['Country', 'Year'], right_on=['Country', 'Year'], how='left')
    
    log.info("Message")
    log.info(merged_df.isnull().sum())
    log.info(merged_df.columns)

    for col in merged_df.columns:
        if col not in ['id', 'Country', 'Code', 'Year']:  
            merged_df[col] = merged_df.groupby('Year')[col].transform(lambda x: x.fillna(x.median()) if x.notna().sum() > 0 else x) #besity_prevalence_percentage & diabetes_prevalence_percentage just have information until 2016

    log.info("Message2")
    log.info(merged_df.isnull().sum()) 
    
    merged_df['DeathRate'] = (merged_df['Cardiovascular'] / merged_df['population']) * 100000

    result = {
        "source":"merge_data",
        "data": merged_df.to_dict(orient='records')
    }

    kwargs['ti'].xcom_push(key='data_merged', value=json.dumps(result))

    return json.dumps(result)


def load_data(**kwargs):
    log.info("Starting data load")

    ti = kwargs["ti"]
    
    # Pull data from XCom
    json_2 = ti.xcom_pull(task_ids="Merge", key='data_merged')
    json_1 = ti.xcom_pull(task_ids="transform_cardio", key='transform_cardio_data')

    log.info(json_1)
    # Check if the data exists
    if json_1 is None:
        log.error("No data found in XCom for 'transform_cardio'")
        return
    
    log.info(json_2)
    if json_2 is None:
        log.error("No data found in XCom for 'transform_deaths'")
        return
        
    data1 = json.loads(json_1)
    data2 = json.loads(json_2)


    df1 = pd.DataFrame(data1["data"])
    df2 = pd.DataFrame(data2["data"])

    df2['obesity_prevalence_percentage'] = df2['obesity_prevalence_percentage'].fillna(df2['obesity_prevalence_percentage'].mean())
    df2['diabetes_prevalence_percentage'] = df2['diabetes_prevalence_percentage'].fillna(df2['diabetes_prevalence_percentage'].mean())

    df2_copy = df2.copy()

    df1_normalize, df2_normalize = DimensionalModel(df1,df2)

    
    log.info(df2_copy.columns)

    result = {
        "data_cardio": df1_normalize.to_dict(orient='records'),
        "data_deaths": df2_copy.to_dict(orient='records')
    }

    kwargs['ti'].xcom_push(key='data_load', value=json.dumps(result))

    return json.dumps(result)


def producer_kafka(**kwargs):
    log.info("kafka producer")
    ti = kwargs["ti"]

    json1 = ti.xcom_pull(task_ids="load", key='data_load')
    data = json.loads(json1)
    df1 = pd.DataFrame(data["data_deaths"])
    
    kafka_producer(df1)
    log.info("All messages send")