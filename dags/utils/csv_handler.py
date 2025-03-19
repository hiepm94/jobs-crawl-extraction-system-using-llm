import csv
import logging

class CSVHandler:
    def __init__(self):
        self.config_file = '/opt/airflow/dags/company_config.csv'
        self.logger = logging.getLogger(__name__)

    def read_company_config(self, company_name):
        self.logger.info(f"Reading configuration for company: {company_name}")
        with open(self.config_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['companyID'] == company_name:
                    self.logger.info(f"Configuration found for company: {company_name}")
                    return row
        self.logger.warning(f"No configuration found for company: {company_name}")
        return None

    def read_all_company_configs(self):
        self.logger.info("Reading configurations for all companies")
        with open(self.config_file, 'r') as f:
            reader = csv.DictReader(f)
            configs = list(reader)
        self.logger.info(f"Read {len(configs)} company configurations")
        return configs
