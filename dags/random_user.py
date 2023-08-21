from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from sqlalchemy import create_engine



def get_data_from_api():
    #VARIABLES DECLARATION
    url = "https://randomuser.me/api/?results=100"

    headers = {}
    response = requests.request("GET",url,headers=headers)

    data = response.json()
    return data



def extract_relevant_records_from_overall_data(data):
    
    First_Name = []
    Last_Name = []
    Gender = []
    Email = []
    Date_of_Birth = []
    Country = []
    Street_Address = []
    City = []
    State = []
    Postcode = []
    Phone = []
    Cell = []


    for i in range(len(data['results'])):
        First_Name.append(data['results'][i]['name']['first'])
        Last_Name.append(data['results'][1]['name']['last'])
        Gender.append(data['results'][i]['gender'])
        Email.append(data['results'][i]['email'])
        Date_of_Birth.append(data['results'][i]['dob']['date'][:10])
        Country.append(data['results'][i]['location']['country'])
        Street_Address.append(data['results'][i]['location']['street']['name'])
        City.append(data['results'][i]['location']['city'])
        State.append(data['results'][i]['location']['state'])
        Postcode.append(data['results'][i]['location']['postcode'])
        Phone.append(data['results'][i]['phone'])
        Cell.append(data['results'][i]['cell'])
    return First_Name,Last_Name,Gender,Email,Date_of_Birth,Country,Street_Address,City,State,Postcode,Phone,Cell


def translate_extractions_to_dataframe_and_transform(First_Name,Last_Name,Gender,Email,Date_of_Birth,Country,Street_Address,City,State,Postcode,Phone,Cell):
    
    
    #placing values into columns
    rapid_dict = {
                    'first':First_Name,
                    'last':Last_Name,
                    'gender':Gender,
                    'email':Email,
                    'dob':Date_of_Birth,
                    'country':Country,
                    'street':Street_Address,
                    'city':City,
                    'state':State,
                    'postcode':Postcode,
                    'phone':Phone,
                    'cell':Cell      

                 }
    
    random_user3 = pd.DataFrame(rapid_dict)#convert to dataframe
    
    #convert dob column datatype from object to date
    random_user3['dob'] = pd.to_datetime(random_user3['dob']).dt.date
    
    return random_user3
    


def to_sql_task(df,table_name):
    engine = create_engine('postgresql://airflow:airflow@host.docker.internal:5434/postgres')
    df.to_sql(table_name, engine)
       


data = get_data_from_api()
First_Name,Last_Name,Gender,Email,Date_of_Birth,Country,Street_Address,City,State,Postcode,Phone,Cell = extract_relevant_records_from_overall_data(data)
df_rapid = translate_extractions_to_dataframe_and_transform(First_Name,Last_Name,Gender,Email,Date_of_Birth,Country,Street_Address,City,State,Postcode,Phone,Cell)


default_args={
    'owner':'Funmi',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
    
}
with DAG(
    dag_id='random_user_v1',
    description='Details of ramdon users',
    start_date=datetime(2023,8,20),
    schedule_interval='@daily',
    default_args = default_args    
    )as dag:
    
    get_data_fromapi = PythonOperator(
        task_id = 'get_data_from_api',
        python_callable = get_data_from_api
        
    )
    
    
    extract_relevant_records = PythonOperator(
        task_id = 'extract_relevant_records',
        python_callable = extract_relevant_records_from_overall_data,
        provide_context = True,
        op_args=[data]
        
    )    

load_db=PythonOperator(
    task_id='load_db',
    python_callable=to_sql_task,
    op_args=[df_rapid,'new_random_user_details']
    
)
    
    # Define the task dependencies

get_data_fromapi >> extract_relevant_records  >> load_db