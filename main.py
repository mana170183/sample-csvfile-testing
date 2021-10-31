import os
from google.cloud import bigquery

def csv_loader(data, context):
        client = bigquery.Client()
        dataset_id1 = os.environ['DATASET1']
		dataset_id2 = os.environ['DATASET2']
        dataset_ref1 = client.dataset(dataset_id1)
		dataset_ref2 = client.dataset(dataset_id2)
        job_config1 = bigquery.LoadJobConfig()
		job_config2 = bigquery.LoadJobConfig()
        job_config1.schema = [
                bigquery.SchemaField('Day', 'DATE'),
                bigquery.SchemaField('Country Name', 'STRING'),
                bigquery.SchemaField('Operator Name', 'STRING'),
                bigquery.SchemaField('Attempts', 'INTEGER'),
                bigquery.SchemaField('Answered', 'INTEGER'),
                bigquery.SchemaField('A-Release %', 'STRING'),
				bigquery.SchemaField('NER', 'STRING'),
				bigquery.SchemaField('ASR', 'STRING'),
				bigquery.SchemaField('ConV Time (Min)', 'INTEGER'),
				bigquery.SchemaField('ACT (Sec)', 'INTEGER'),
				bigquery.SchemaField('CST (Sec)', 'INTEGER')
                ]
		job_config2.schema = [
                bigquery.SchemaField('Hour', 'DATE'),
                bigquery.SchemaField('Country Name', 'STRING'),
                bigquery.SchemaField('Operator Name', 'STRING'),
                bigquery.SchemaField('Attempts', 'INTEGER'),
                bigquery.SchemaField('Answered', 'INTEGER'),
                bigquery.SchemaField('A-Release %', 'STRING'),
				bigquery.SchemaField('NER', 'STRING'),
				bigquery.SchemaField('ASR', 'STRING'),
				bigquery.SchemaField('ConV Time (Min)', 'INTEGER'),
				bigquery.SchemaField('ACT (Sec)', 'INTEGER'),
				bigquery.SchemaField('CST (Sec)', 'INTEGER')
				]
        job_config1.skip_leading_rows = 1
		job_config2.skip_leading_rows = 1
        job_config1.source_format = bigquery.SourceFormat.CSV
		job_config2.source_format = bigquery.SourceFormat.CSV
		
        # get the URI for uploaded CSV in GCS from 'data'
		
        uri = 'gs://' + os.environ['BUCKET'] + '/' + data['name']
		
        load_job1 = client.load_table_from_uri(
                uri,
                dataset_ref1.table(os.environ['TABLE1']),
                job_config=job_config1)
		 load_job2 = client.load_table_from_uri(
                uri,
                dataset_ref2.table(os.environ['TABLE2']),
                job_config=job_config2)
        print('Starting job {}'.format(load_job1.job_id))
		print('Starting job {}'.format(load_job2.job_id))
        print('Function=csv_loader, Version=' + os.environ['VERSION'])
        print('File: {}'.format(data['name']))
        load_job1.result()  
		load_job2.result()           # wait for table load to complete.
        print('Job finished.')
        destination_table1 = client.get_table(dataset_ref1.table(os.environ['TABLE1']))
		destination_table2 = client.get_table(dataset_ref2.table(os.environ['TABLE2']))
        print('Loaded {} rows.'.format(destination_table1.num_rows))
		print('Loaded {} rows.'.format(destination_table2.num_rows))
		
