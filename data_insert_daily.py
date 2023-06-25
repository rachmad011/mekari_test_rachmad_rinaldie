import pandas as pd
import pandas_gbq
from os.path import exists
from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "spheric-gearing-318714-6941c63bfba1.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = "spheric-gearing-318714"

def read_files_csv(file_path:str):
    try:
        file_csv = exists(file_path)
        if file_csv == False:
            raise Exception("Path is not a file or wrong")
        else:
            data = pd.read_csv(file_path)
            
            return(data)
    except Exception as e:
        print("Error :", e)
    finally:
        print("Read File CSV Done!")

def check_existing_id(data_csv:pd.DataFrame, type_data:str):
    try:
        type = {"employees" : "SELECT employee_id FROM `spheric-gearing-318714.rachmadrinaldie_dataset.employees`",
                "timesheets" : "SELECT timesheet_id FROM `spheric-gearing-318714.rachmadrinaldie_dataset.timesheets`"
                }
        
        type_id = {
            "employees" : "employee_id",
            "timesheets" : "timesheet_id"
        }

        if type_data.lower() in type:
            query_production = type[type_data.lower()]
            data_production = pandas_gbq.read_gbq(query_production)
            temp_df = pd.merge(data_csv, data_production, on=type_id[type_data.lower()], how='left', indicator='exists')
            
        else :
            raise Exception("Error : Wrong type of data")
        
        
        if temp_df.loc[temp_df['exists'] != 'both'].empty == True:
            print("There aren't new data to insert")
            return None, None
        else:
            filtered_data = temp_df.loc[temp_df['exists'] != 'both']
            
            if type_data.lower() == 'employees':
                filtered_data['join_date'] = pd.to_datetime(filtered_data['join_date'], format="%Y-%m-%d")
                filtered_data['resign_date'] = pd.to_datetime(filtered_data['resign_date'], format="%Y-%m-%d")
            elif type_data.lower() == 'timesheets':
                filtered_data['date'] = pd.to_datetime(filtered_data['date'], format="%Y-%m-%d")
                filtered_data['checkin'] = pd.to_datetime(filtered_data['checkin'], format="%H:%M:%S").dt.time
                filtered_data['checkout'] = pd.to_datetime(filtered_data['checkout'], format="%H:%M:%S").dt.time
            filtered_data = filtered_data.drop(['exists'], axis=1)
            
            return filtered_data, type_data.lower()
        
    except Exception as e:
        print("Error :", e)
    finally:
        print("Checking existing id - Complete!")
        
def insert_new_data(data:pd.DataFrame, type:str):
    try:
        if data is None or type is None:
            raise Exception("Params Data and Type must not be None")
        
        if type == "employees":
            table_id_fd_employees = "spheric-gearing-318714.rachmadrinaldie_dataset.employees"

            job_config_fd_employees = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("employee_id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("branch_id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("salary", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("join_date", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField("resign_date", "DATE", mode="NULLABLE")
                ],
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )

            job_fd_employees = client.load_table_from_dataframe(
                data, table_id_fd_employees, job_config=job_config_fd_employees
            )
            job_fd_employees.result()
        elif type == "timesheets":
            table_id_fd_timesheets = "spheric-gearing-318714.rachmadrinaldie_dataset.timesheets"

            job_config_fd_timesheets = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("timesheet_id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("employee_id", "INTEGER", mode="NULLABLE"),
                    bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField("checkin", "TIME", mode="NULLABLE"),
                    bigquery.SchemaField("checkout", "TIME", mode="NULLABLE")
                ],
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED"
            )

            job_fd_timesheets = client.load_table_from_dataframe(
                data, table_id_fd_timesheets, job_config=job_config_fd_timesheets
            )
            job_fd_timesheets.result()
            
    except Exception as e:
        print("Error :", e)
    finally:
        print("Job insert new data is completed!")

if __name__ == "__main__":
    fd_employees = pd.read_csv('./employees.csv')
    fd_timesheets = pd.read_csv('./timesheets.csv')
    
    file_path_employees_csv = 'employees.csv'
    file_path_timesheets_csv = 'timesheets.csv'
    
    data_employees_csv = read_files_csv(file_path_employees_csv)
    data_timesheets_csv = read_files_csv(file_path_timesheets_csv)
    
    new_data_employees, type_insert_employees = check_existing_id(data_employees_csv, 'employees')
    new_data_timesheets, type_insert_timesheets = check_existing_id(data_timesheets_csv, 'timesheets')
    
    insert_new_data(new_data_employees, type_insert_employees)
    insert_new_data(new_data_timesheets, type_insert_timesheets)