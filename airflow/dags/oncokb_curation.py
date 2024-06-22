from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.io.path import ObjectStoragePath
from collections import defaultdict
import pandas as pd
import logging
import os 

# Aieflow Object Storage path to s3
base = ObjectStoragePath("s3://my_aws_s3@oncokb-ge")
# Path to the file containing samples to be filtered; leave as '' to skip filtering
filter_path = ''

def upload_to_s3(filename,df):
    """
    Function to upload DataFrame to AWS S3 bucket.
    Args:
        path: file name in s3 bucket
        df: DataFrame to upload
    """
    path = base / filename
    with path.open("w") as file:
        df.to_csv(file, sep='\t', index=False)

def read_from_s3(filename):
    """
    Function to download file from AWS S3 bucket.
    Args:
        file_name: file name in s3 bucket
    Return: 
        DataFrame for file
    """
    path = base / filename
    with path.open() as f:
        df = pd.read_csv(f,sep='\t')
    return df

def exclude_samples(data,filter_path):
    """
    Function to filter the data.
    Args:
        data: DataFrame need to be filter
        filter_path: filter_path
    Return: 
        DataFrame after filter
    """
    excluded_sample_ids = pd.read_csv(filter_path,delimiter='\t',header=None)[0].tolist()
    filtered_data = data[~data['SAMPLE_ID'].isin(excluded_sample_ids)]
    return filtered_data

def get_statistic_file(ti):
        # Local file path, change to MSK API in the future
        test_name="msk_impact_2017"
        current_dir = os.path.dirname(__file__)
        clinical_path = os.path.join(current_dir, '..', '..','biodata',test_name,'data_clinical_sample.txt')
        sample_path = os.path.join(current_dir, '..', '..','biodata',test_name,'data_mutations.txt')
        filter_path = os.path.join(current_dir, '..', '..','biodata','msi+tmb_high_samples_msk_impact_2017.txt')

        # Get the mutation data
        sample = pd.read_csv(clinical_path, usecols=['SAMPLE_ID', 'CANCER_TYPE','CANCER_TYPE_DETAILED'], delimiter='\t', skiprows=4)
        mutation = pd.read_csv(sample_path, usecols=['Hugo_Symbol','Entrez_Gene_Id', 'Tumor_Sample_Barcode','HGVSp_Short'], delimiter='\t',skiprows=1)
        mutation['Entrez_Gene_Id'] = mutation['Entrez_Gene_Id'].fillna('n/a')
        data = pd.merge(sample, mutation, left_on='SAMPLE_ID', right_on='Tumor_Sample_Barcode').drop(columns=['Tumor_Sample_Barcode'])
        if filter_path:
            data = exclude_samples(data,filter_path)

        # All cancer frequency
        all_number = data[['Hugo_Symbol','Entrez_Gene_Id', 'HGVSp_Short']].value_counts().reset_index(name='Number')
        all_percentage = data[['Hugo_Symbol','Entrez_Gene_Id','HGVSp_Short']].value_counts(normalize=True).reset_index(name='Percentage')
        all_frequency = pd.merge(all_number, all_percentage, on=['Hugo_Symbol','Entrez_Gene_Id','HGVSp_Short'])
        upload_to_s3(f"{test_name}_all_frequency.txt",all_frequency)
        logging.info("upload all_frequecny file")

        # Cancer type frequency
        type_number = data.groupby('CANCER_TYPE')[['Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short']].value_counts().reset_index(name='Number')
        type_percentage = data.groupby('CANCER_TYPE')[['Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short']].value_counts(normalize=True).reset_index(name='Percentage')
        type_frequency = pd.merge(type_number, type_percentage, on=['CANCER_TYPE', 'Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short'])
        upload_to_s3(f"{test_name}_type_frequency.txt",type_frequency)
        logging.info("upload type_frequecny file")

        # Cancer type detailed frequency
        detailed_number = data.groupby(['CANCER_TYPE','CANCER_TYPE_DETAILED'])[['Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short']].value_counts().reset_index(name='Number')
        detailed_percentage = data.groupby(['CANCER_TYPE','CANCER_TYPE_DETAILED'])[['Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short']].value_counts(normalize=True).reset_index(name='Percentage')
        detailed_frequency = pd.merge(detailed_number, detailed_percentage, on=['CANCER_TYPE', 'CANCER_TYPE_DETAILED', 'Hugo_Symbol', 'Entrez_Gene_Id','HGVSp_Short'])
        upload_to_s3(f"{test_name}_detailed_frequency.txt",detailed_frequency)
        logging.info("upload detrailed_frequecny file")

        ti.xcom_push(key="test_name", value = test_name)

def merge_mutation(ti):
        # Get statistic data
        test_name = ti.xcom_pull(task_ids="get_statistic_file", key="test_name")
        all_frequency = read_from_s3(f"{test_name}_all_frequency.txt")
        type_frequency = read_from_s3(f"{test_name}_type_frequency.txt")
        detailed_frequency = read_from_s3(f"{test_name}_detailed_frequency.txt")

        # Set mutation as index
        type_frequency.set_index(['Hugo_Symbol', 'HGVSp_Short'], inplace=True)
        detailed_frequency.set_index(['Hugo_Symbol', 'HGVSp_Short'], inplace=True)

        new_rows = []
        for row in all_frequency.itertuples(index=False):
            new_row = {
                "Hugo_Symbol": row.Hugo_Symbol,
                "HGVSp_Short": row.HGVSp_Short,
                "Entrez_Gene_Id":row.Entrez_Gene_Id,
                "criteria_all": {
                    'allcancertypefrequency': {
                        'number': row.Number,
                        'percentage': row.Percentage
                    }
                }
            }

            # get cancer type
            type_df = type_frequency.loc[(row.Hugo_Symbol, row.HGVSp_Short)]
            type_json = defaultdict(dict)
            for type_row in type_df.itertuples(index=False):
                type_json[type_row.CANCER_TYPE]['number'] = type_row.Number
                type_json[type_row.CANCER_TYPE]['percentage'] = type_row.Percentage
            new_row["criteria_caner_type"] = dict(type_json)

            # get cancer detailed
            detailed_df = detailed_frequency.loc[(row.Hugo_Symbol, row.HGVSp_Short)]
            detailed_json = defaultdict(dict)
            for detailed_row in detailed_df.itertuples(index=False):
                detailed_json[detailed_row.CANCER_TYPE_DETAILED]['number'] = detailed_row.Number
                detailed_json[detailed_row.CANCER_TYPE_DETAILED]['percentage'] = detailed_row.Percentage
            new_row["criteria_detailed_caner_type"] = dict(detailed_json)

            new_rows.append(new_row)

        merge_table = pd.DataFrame(new_rows)
        upload_to_s3(f"{test_name}_merge_frequency.txt",merge_table)
        logging.info("upload merge_frequecny file")

with DAG(
    dag_id="oncokb_curation_v1",
    description="Get the statistic file and save to S3",
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval="@weekly",
    catchup=False,
    default_args = {
    	'owner': 'oncoKB',
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
    }
) as dag:
     
    get_statistic_file_task = PythonOperator(
          task_id = 'get_statistic_file',
           python_callable = get_statistic_file
     )

    aggregrate_statistic_file_task = PythonOperator(
          task_id = 'aggregrate_statistic_file',
           python_callable = merge_mutation
     )
    
    get_statistic_file_task >> aggregrate_statistic_file_task