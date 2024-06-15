import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import config as cfg
import psycopg2
import io
import sys

#### GLOBAL VARIABLES ####
log_file = "aircraft.log"
table_name = "aircraft"


def extract_from_csv(file_2_process):

    return pd.read_csv(file_2_process)

def extract_from_json(file_2_process):

    return pd.read_json(file_2_process,lines=True)

def extract():

    try:
        extracted_data = pd.DataFrame(columns=["aircraft_id","aircraft_model","capacity"])

        ## Process all csv files
        for csv_file in glob.glob("./Files/aircraft/*.csv"):

            extracted_data = pd.concat([extracted_data,extract_from_csv(csv_file)],ignore_index=True)
        
        ## Process all json files
        for json_file in glob.glob("*.json"):

            extracted_data = pd.concat([extracted_data,extract_from_json(json_file)],ignore_index=True)
        

    except Exception as e:   
        log_process(f"Error extracting data: {e}")
        sys.exit(1) 

    return extracted_data


def transform(data):

    try:

       pass

    except Exception as e:
        log_process(f"Error transforming data: {e}")
        sys.exit(1)

    return data

def load_data(transformed_data,cur,conn):

    try:

        csv_buffer = io.StringIO()
        transformed_data.to_csv(csv_buffer, index=False, header=False) 
        csv_buffer.seek(0)

        cur.copy_expert(f"""COPY {table_name} ("aircraft_id","aircraft_model","capacity") FROM STDIN WITH (FORMAT CSV)""", csv_buffer)
        conn.commit()

    except Exception as e:
            
        log_process(f"Error loading data: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        sys.exit(1)

def log_process(msg):

    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now().strftime(timestamp_format)
    line = f'[{table_name} ' + now + ']: ' + msg + '\n'

    print(line)
    with open(log_file, "a") as f:

        f.write(line)


if __name__ == "__main__":

    conn = psycopg2.connect(
        dbname=cfg.dbname,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port 
    )

    cur = conn.cursor()
    log_process("ETL Job Started")

    log_process("Extract phase Started")
    extracted_data = extract()
    log_process("Extract phase Ended")

    log_process("Transform phase Started")
    #transformed_data = transform(extracted_data)
    log_process("Transformed data")

    log_process("Load phase Started")
    ### BULK LOAD EXAMPLE ###
    load_data(extracted_data,cur,conn) 
    log_process("Load phase Ended")

    log_process("ETL Job Ended")