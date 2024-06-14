from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
import pandas as pd
import logging

#Read data locally, we can change it to msk API later
local = "path/to/folder/with/msk/data"
base = ObjectStoragePath("s3://my_aws_s3@oncokb-ge")
filter_path = "path/to/data"

default_args = {
    	'owner': 'oncoKB',
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id="oncokb_curation_v1",
    description="Get the statistic file and save to S3",
    start_date=datetime(2024,6,8,0),
    schedule_interval="@weekly",
    catchup=False,
)

def curate_mutation_data():

    def exclude_samples(data,filter_path):
        excluded_sample_ids = pd.read_csv(filter_path,delimiter='\t',header=None)[0].tolist()
        filtered_data = data[~data['SAMPLE_ID'].isin(excluded_sample_ids)]
        return filtered_data
    
    def upload_to_s3(path,df):
        with path.open("w") as file:
            df.to_csv(file, sep='\t', index=False)

    def read_from_s3(path: ObjectStoragePath):
        with path.open() as f:
            df = pd.read_csv(f,sep='\t')
        return df

    @task
    def get_statistic_file():
        test_name="msk_impact_2017"
        file_path = local+test_name

        # get the mutation data
        sample = pd.read_csv(file_path+"/data_clinical_sample.txt", usecols=['SAMPLE_ID', 'CANCER_TYPE','CANCER_TYPE_DETAILED'], delimiter='\t', skiprows=4)
        mutation = pd.read_csv(file_path+"/data_mutations.txt", usecols=['Hugo_Symbol', 'Tumor_Sample_Barcode','HGVSp_Short'], delimiter='\t',skiprows=1)
        data = pd.merge(sample, mutation, left_on='SAMPLE_ID', right_on='Tumor_Sample_Barcode').drop(columns=['Tumor_Sample_Barcode'])
        if filter_path:
            data = exclude_samples(data,filter_path)

        #all cancer frequency
        all_number = data[['Hugo_Symbol', 'HGVSp_Short']].value_counts().reset_index(name='Number')
        all_percentage = data[['Hugo_Symbol', 'HGVSp_Short']].value_counts(normalize=True).reset_index(name='Percentage')
        all_frequency = pd.merge(all_number, all_percentage, on=['Hugo_Symbol', 'HGVSp_Short'])
        all_path = base / f"{test_name}_all_frequency.txt"
        upload_to_s3(all_path,all_frequency)
        logging.info("upload all_frequecny file")

        #cancer type frequency
        type_number = data.groupby('CANCER_TYPE')[['Hugo_Symbol', 'HGVSp_Short']].value_counts().rename("Number").reset_index()
        type_percentage = data.groupby('CANCER_TYPE')[['Hugo_Symbol', 'HGVSp_Short']].value_counts(normalize=True).rename("Percentage")
        type_frequency = pd.merge(type_number, type_percentage, on=['CANCER_TYPE', 'Hugo_Symbol', 'HGVSp_Short'])
        type_path = base / f"{test_name}_type_frequency.txt"
        upload_to_s3(type_path,type_frequency)
        logging.info("upload type_frequecny file")

        #cancer type detailed frequency
        detailed_number = data.groupby(['CANCER_TYPE','CANCER_TYPE_DETAILED'])[['Hugo_Symbol', 'HGVSp_Short']].value_counts().rename("Number").reset_index()
        detailed_percentage = data.groupby(['CANCER_TYPE','CANCER_TYPE_DETAILED'])[['Hugo_Symbol', 'HGVSp_Short']].value_counts(normalize=True).rename("Percentage")
        detailed_frequency = pd.merge(detailed_number, detailed_percentage, on=['CANCER_TYPE', 'CANCER_TYPE_DETAILED', 'Hugo_Symbol', 'HGVSp_Short'])
        detailed_path = base / f"{test_name}_detailed_frequency.txt"
        upload_to_s3(detailed_path,detailed_frequency)
        logging.info("upload detrailed_frequecny file")
        
        return test_name

    @task
    def merge_mutation(test_name):
        #Get file from s3
        all_path = base / f"{test_name}_all_frequency.txt"
        all_frequency = read_from_s3(all_path)
        type_path = base / f"{test_name}_type_frequency.txt"
        type_frequency = read_from_s3(type_path)
        detailed_path = base / f"{test_name}_detailed_frequency.txt"
        detailed_frequency = read_from_s3(detailed_path)

        #aggregrate data
        merge_table = pd.DataFrame(columns=["Hugo_Symbol", "HGVSp_Short", "criteria_all", "criteria_caner_type", "criteria_detailed_caner_type"])
        for index, row in all_frequency.iterrows():
            new_row = {}
            
            # add mutation
            new_row["Hugo_Symbol"] = row["Hugo_Symbol"]
            new_row["HGVSp_Short"] = row["HGVSp_Short"]
            
            # get all frequency
            all_json = {'allcancertypefrequency': {}}
            all_json['allcancertypefrequency']['number'] = row['Number']
            all_json['allcancertypefrequency']['percentage'] = row['Percentage']
            new_row["criteria_all"] = all_json
            
            # get cancer type
            type_df = type_frequency[(type_frequency['Hugo_Symbol'] == row['Hugo_Symbol']) & (type_frequency['HGVSp_Short'] == row['HGVSp_Short'])]
            type_json = {}
            for _, type_row in type_df.iterrows():
                type_json[type_row['CANCER_TYPE']] = {
                    'number': type_row['Number'],
                    'percentage': type_row['Percentage']
                }
            new_row["criteria_caner_type"] = type_json
            
            # get cancer detailed
            detailed_df = detailed_frequency[(detailed_frequency['Hugo_Symbol'] == row['Hugo_Symbol']) & (detailed_frequency['HGVSp_Short'] == row['HGVSp_Short'])]
            detailed_json = {}
            for _, detailed_row in detailed_df.iterrows():
                detailed_json[detailed_row['CANCER_TYPE_DETAILED']] = {
                    'number': detailed_row['Number'],
                    'percentage': detailed_row['Percentage']
                }
            new_row["criteria_detailed_caner_type"] = detailed_json

            merge_table = pd.concat([merge_table, pd.DataFrame([new_row])], ignore_index=True)
        
        # save to S3
        save_path = base / f"{test_name}_merge_frequency.txt"
        upload_to_s3(save_path,merge_table)
        logging.info("upload merge_frequecny file")

    test = get_statistic_file()
    merge_mutation(test)

curate_mutation_data()