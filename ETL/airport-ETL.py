import pandas as pd
from datetime import datetime
import psycopg2
import io
import config as cfg
from bs4 import BeautifulSoup
import requests
import sys

#### GLOBAL VARIABLES ####
log_file = "airport.log"
table_name = "airport"
data_url = 'https://www.flugzeuginfo.net/table_airportcodes_country-location_en.php'

def extract():

    try:
        extracted_data = pd.DataFrame(columns=["iata","location","airport",'country'])

        response = requests.get(data_url)
        html_content = response.text
        soup = BeautifulSoup(html_content, 'html.parser')

        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 0:
                    iata = cells[0].text
                    location = cells[2].text
                    airport = cells[3].text
                    country = cells[4].text

                    extracted_data = pd.concat([extracted_data, pd.DataFrame([{"iata_code":iata,"location":location, "airport":airport,'country':country}])], ignore_index = True)

        extracted_data.drop_duplicates(inplace=True)   

        return extracted_data

    except:

        log_process("Error extracting data")
        sys.exit(1)
    


def transform(data):

    try:

        data['city'] = data.apply(lambda x: f"{x['location']} ({x['airport']})", axis=1)
        data = data[['city','country','iata_code']]

    except Exception as e:
        log_process(f"Error transforming data: {e}")
        sys.exit(1)

    return data

def load_data(transformed_data,cur,conn):

    try:

        csv_buffer = io.StringIO()
        transformed_data.to_csv(csv_buffer, index=False, header=False) 
        csv_buffer.seek(0)

        cur.copy_expert(f"""COPY {table_name} ("city","country","iata_code") FROM STDIN WITH (FORMAT CSV)""", csv_buffer)
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
    line = '[' + now + ']: ' + msg + '\n'

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
    transformed_data = transform(extracted_data)
    log_process("Transformed data")

    log_process("Load phase Started")
    load_data(transformed_data,cur,conn) 
    log_process("Load phase Ended")

    log_process("ETL Job Ended")

    cur.close()
    conn.close()