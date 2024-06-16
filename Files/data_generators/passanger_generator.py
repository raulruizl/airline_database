import pandas as pd
import random
import string
import psycopg2

dbname = 'airline_db'
user = 'postgres'
password = 'SxdixCGG0nXAbcc'
host='localhost'
port=5430

def name_generator():
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Olivia']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia']
    random_name = random.choice(first_names)
    random_last_name = random.choice(last_names)

    return random_name, random_last_name

def mail_generator(name, last_name):

    mail = name + '.' + last_name + '@email.com'

    return mail


    
def phone_generator():
    country_codes = ['+1', '+44', '+61', '+86', '+91']
    random_country_code = random.choice(country_codes)
    random_phone = random_country_code + str(random.randint(1000000000, 9999999999))
    
    return random_phone


def passanger_id():

    random_dni = ''.join(random.choices(string.digits, k=8))
    return random_dni



def generate_df(db_id):

    df = pd.DataFrame(columns=['passanger_id','name','last_name','mail','phone'])

    for i in range(1,16):

        id = passanger_id()
        while id in db_id:
            
            id = passanger_id()
        name, last_name = name_generator()
        mail = mail_generator(name, last_name)
        phone = phone_generator()
        print(f"Generating passanger {i} of 15: Generated passanger {id} with name {name} {last_name}, mail {mail} and phone {phone}")
        df = pd.concat([df,pd.DataFrame([{'passanger_id':id,'name':name,'last_name':last_name,'mail':mail,'phone':phone}])],ignore_index=True)

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
    db_id = cur.execute("SELECT id_number FROM passanger")

    if db_id is not None:

        db_id = cur.fetchall()
        db_id = [i[0] for i in db_id]

    else:  
        db_id = []

    cur.close()
    conn.close()


    generate_df(db_id).to_csv('./Files/passangers/passanger.csv',index=False)
    