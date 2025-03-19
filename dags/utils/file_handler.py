import json
import os
from datetime import datetime
import logging
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FileHandler:
    def __init__(self):
        self.data_dir = os.path.abspath('/opt/airflow/data')
        self.data_file_template = '{company_name}_job_data.jsonl'
        os.makedirs(self.data_dir, exist_ok=True)
        logger.info(f"FileHandler initialized with data_dir: {self.data_dir}, data_file_template: {self.data_file_template}")

    def get_file_path(self, company_name):
        file_name = self.data_file_template.format(company_name=company_name)
        file_path = os.path.join(self.data_dir, file_name)
        logger.info(f"File path: {file_path}")
        return file_path

    def read_previous_data(self, company_name):
        mongo_client = MongoClient("mongodb+srv://hiep:hiep@atlascluster.rz3atb5.mongodb.net/?retryWrites=true&w=majority&appName=AtlasCluster")
        db = mongo_client['topaib-job-information']
        collection = db['job-position-information']

        data = list(collection.find({'companyID': company_name}))

        logger.info(f"Read {len(data)} records for company: {company_name}")
        return data

    def store_job_data(self, job_data, company_name):
        file_path = self.get_file_path(company_name)
        mode = 'a' if os.path.exists(file_path) else 'w'
        with open(file_path, mode) as f:
            for job in job_data:
                f.write(json.dumps(job) + '\n')
        logger.info(f"Stored {len(job_data)} job records for company: {company_name}")

    def update_job_data(self, job_id, updates, company_name):
        file_path = self.get_file_path(company_name)
        temp_file_path = file_path + '.temp'

        updated_count = 0
        with open(file_path, 'r') as input_file, open(temp_file_path, 'w') as output_file:
            for line in input_file:
                job = json.loads(line)
                if job['id'] == job_id:
                    job.update(updates)
                    updated_count += 1
                output_file.write(json.dumps(job) + '\n')

        os.replace(temp_file_path, file_path)
        logger.info(f"Updated {updated_count} job records with id: {job_id} for company: {company_name}")

    def get_job_links(self, company_name):
        data = self.read_previous_data(company_name)
        job_links = [job for job in data if 'jobLink' in job and 'rawHtml' not in job]
        logger.info(f"Retrieved {len(job_links)} job links for company: {company_name}")
        return job_links

    def read_all_job_data(self, company_name):
        file_path = self.get_file_path(company_name)
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            return []
        with open(file_path, 'r') as f:
            data = [json.loads(line) for line in f]
        logger.info(f"Read {len(data)} records for company: {company_name}")
        return data

    def delete_file(self, company_name):
        file_path = self.get_file_path(company_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted file for company: {company_name}")
        else:
            logger.warning(f"File not found: {file_path}")
