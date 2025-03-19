from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import uuid
from utils.selenium_handler import SeleniumHandler
from utils.csv_handler import CSVHandler
from utils.file_handler import FileHandler
from bs4 import BeautifulSoup
import re
import os
from pymongo import MongoClient

def crawl_job_links(company_name):
    logging.info(f"Starting crawler for company: {company_name}")
    selenium_handler = SeleniumHandler()
    csv_handler = CSVHandler()
    file_handler = FileHandler()

    try:
        company_config = csv_handler.read_company_config(company_name)

        if not company_config:
            logging.warning(f"No configuration found for company: {company_name}")
            return

        job_links = selenium_handler.crawl_job_links(company_config)
        job_links = list(set(job_links))

        previous_data = file_handler.read_previous_data(company_name)
        previous_links = [job['jobLink'] for job in previous_data if 'jobLink' in job]
        new_links = [link for link in job_links if link not in previous_links]

        if not new_links:
            logging.info(f"No new links found for company: {company_name}. Stopping process.")
            return

        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_data = [
            {
                "id": str(uuid.uuid4()),
                "companyID": f"{company_config['companyID']}",
                "name": company_config['name'],
                "homeLink": company_config['homeLink'],
                "jobLink": link,
                "countryID": company_config['countryID'],
                "rootJob": company_config['rootJob'],
                "crawlDate": current_timestamp,
                "companyCareerLink": company_config['companyCareerLink']
            } for i, link in enumerate(new_links, start=len(previous_data))
        ]

        file_handler.store_job_data(job_data, company_name)
        logging.info(f"Crawler completed successfully for company: {company_name}")
        return job_data
    except Exception as e:
        logging.error(f"An error occurred while crawling {company_name}: {str(e)}")
    finally:
        selenium_handler.close()

def crawl_job_positions(company_name, **context):
    job_data = context['task_instance'].xcom_pull(task_ids=f'crawl_job_links_{company_name}')

    if not job_data:
        logging.info(f"No new job data to process for company: {company_name}. Stopping process.")
        return

    logging.info(f"Starting crawler job for company: {company_name}")
    selenium_handler = SeleniumHandler()
    file_handler = FileHandler()

    try:
        for index, job in enumerate(job_data, 1):
            logging.info(f"Processing job {index}/{len(job_data)}: {job['jobLink']}")
            raw_html = selenium_handler.get_page_source(job['jobLink'])

            soup = BeautifulSoup(raw_html, 'html.parser')
            text_elements = soup.find_all(string=True)
            all_text = [element.get_text().strip() for element in text_elements if element.strip()]
            text_string = '. '.join(all_text)
            raw_text = re.sub(r'\s+', ' ', text_string).strip()

            updates = {
                "rawHtml": raw_html,
                "rawText": raw_text
            }
            file_handler.update_job_data(job['id'], updates, company_name)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

    finally:
        selenium_handler.close()

def load_data_to_mongodb(company_name):
    file_handler = FileHandler()
    YOUR_MONGO_URI = os.getenv('MONGO_URI')
    mongo_client = MongoClient(YOUR_MONGO_URI)
    db = mongo_client['topaib-job-information']
    collection = db['job-position-information']

    job_data = file_handler.read_all_job_data(company_name)
    if job_data:

        try:
            result = collection.insert_many(job_data)
            if result.acknowledged:
                logging.info(f"Job data stored in MongoDB for company: {company_name}")
                file_handler.delete_file(company_name)
            else:
                logging.error(f"Failed to insert job data into MongoDB for company: {company_name}")
        except Exception as e:
            logging.error(f"Error inserting job data into MongoDB for company: {company_name}: {str(e)}")

    else:
        logging.info(f"No job data to load for company: {company_name}")

def create_dag(dag_id, schedule, default_args, company_name):
    dag = DAG(dag_id, schedule_interval=schedule, default_args=default_args, catchup=False)

    with dag:
        crawl_links = PythonOperator(
            task_id=f'crawl_job_links_{company_name}',
            python_callable=crawl_job_links,
            op_kwargs={'company_name': company_name}
        )

        crawl_positions = PythonOperator(
            task_id=f'crawl_job_positions_{company_name}',
            python_callable=crawl_job_positions,
            op_kwargs={'company_name': company_name},
            provide_context=True
        )

        load_to_mongodb = PythonOperator(
            task_id=f'load_data_to_mongodb_{company_name}',
            python_callable=load_data_to_mongodb,
            op_kwargs={'company_name': company_name}
        )

        crawl_links >> crawl_positions >> load_to_mongodb
    logging.info(f"DAG created for {company_name} with schedule: {schedule}")

    return dag


csv_handler = CSVHandler()
company_configs = csv_handler.read_all_company_configs()
logging.info(f"Loaded {len(company_configs)} company configurations")

for company in company_configs:
    company_name = company['companyID']
    crawl_frequency = company['crawlFrequently']

    logging.info(f"Processing company: {company_name} with frequency: {crawl_frequency}")

    if crawl_frequency == 'daily':
        schedule = '@daily'
    elif crawl_frequency == 'weekly':
        schedule = '@weekly'
    elif crawl_frequency == 'biweekly':
        schedule = '0 0 */14 * *'  # At 00:00 every 14 days
    elif crawl_frequency == 'monthly':
        schedule = '@monthly'
    else:
        logging.warning(f"Invalid crawl frequency for {company_name}: {crawl_frequency}. Skipping.")
        continue  # Skip if invalid frequency

    dag_id = f'trigger_crawl_{company_name}'
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 9),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    logging.info(f"Creating DAG for {company_name} with ID: {dag_id}")
    globals()[dag_id] = create_dag(dag_id, schedule, default_args, company_name)

logging.info("All DAGs created successfully")
