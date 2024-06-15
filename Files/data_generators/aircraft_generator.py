import pandas as pd
import random
import string
import psycopg2

dbname = 'airline_db'
user = 'postgres'
password = 'SxdixCGG0nXAbcc'
host='localhost'
port=5430


def aircraft_id():

    random_letters = random.choices(string.ascii_letters, k=2)
    init = ''.join(random_letters)

    return str( init + str(random.randint(0000,9999)))

def aircraft_model():

    model_list = ['Airbus A320','Airbus A330','Airbus A350','Boeing B737','Boeing B747','Boeing B777','Boeing B787']

    return model_list[random.randint(0,6)]

def aircraft_capacity(model):

    capacity_dict = {
        'Airbus A320': 180,
        'Airbus A330': 250,
        'Airbus A350': 300,
        'Boeing B737': 200,
        'Boeing B747': 400,
        'Boeing B777': 350,
        'Boeing B787': 300
    }

    return capacity_dict[model]

def generate_df():

    df = pd.DataFrame(columns=['aircraft_id','aircraft_model','capacity'])

    for i in range(1,16):

        id = aircraft_id().upper()
        model = aircraft_model()
        capacity = aircraft_capacity(model)
        print(f"Generating aircraft {i} of 15: Generated aircraft {id} with model {model} and capacity {capacity}")
        df = pd.concat([df,pd.DataFrame([{'aircraft_id':id,'aircraft_model':model,'capacity':capacity}])],ignore_index=True)

    return df

if __name__ == "__main__":
    
    conn = psycopg2.connect(
        dbname= dbname,
        user= user,
        password= password,
        host= host,
        port= port 
    )

    cur = conn.cursor()
    db_id = cur.execute("SELECT aircraft_id FROM aircraft")

    if db_id is not None:

        db_id = cur.fetchall()
        db_id = [i[0] for i in db_id]

    else:  
        db_id = []

    cur.close()
    conn.close()


    generate_df().to_csv('./Files/aircraft/aircraft.csv',index=False)
    with open('./Files/aircraft/aircraft.json', 'w') as f:
        f.write(generate_df().to_json(orient='records'))
    