import pandas as pd
import trino
from trino.auth import BasicAuthentication  # Import BasicAuthentication
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import getpass
import logging
import datetime
import sqlparse
import re
import os  # Import for handling file paths

# Function to set up logging with the desired log file name
def setup_logging(excel_directory, team, env, timestamp):
    log_filename = f"trino_test_log_{team or 'All'}_{env}_{timestamp}.log".replace(' ', '_')
    log_filepath = os.path.join(excel_directory, log_filename)
    logging.basicConfig(
        filename=log_filepath,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return log_filepath

# Function to read the data from the Excel sheets
def read_excel_data(file_path):
    test_cases_df = pd.read_excel(file_path, sheet_name='Test Cases')
    users_df = pd.read_excel(file_path, sheet_name='Users')
    trino_env_df = pd.read_excel(file_path, sheet_name='Trino Env')
    return test_cases_df, users_df, trino_env_df

# Function to get passwords for unique users based on the selected environment
def get_user_passwords(users_df, selected_env):
    user_passwords = {}
    env_specific_users = users_df[users_df['Env'] == selected_env]
    
    for user in env_specific_users['User'].unique():
        password = getpass.getpass(prompt=f"Enter password for user '{user}' in environment '{selected_env}': ")
        user_passwords[user] = password
    return user_passwords

# Dictionary to store reusable connections
connection_pool = {}

# Function to get or reuse a Trino connection using BasicAuthentication
def get_or_create_trino_connection(host_url, user, password):
    connection_key = (host_url, user)
    
    if connection_key not in connection_pool:
        # Create a new connection if it doesn't exist in the pool
        connection_pool[connection_key] = trino.dbapi.connect(
            host=host_url,
            port=443,  # Using HTTPS port 443
            user=user,
            auth=BasicAuthentication(user, password),  # Use BasicAuthentication
            http_scheme='https'  # Use 'https' for secure connections
        )
        
    return connection_pool[connection_key]

# Function to pretty format the SQL query
def format_sql(sql_query):
    return sqlparse.format(sql_query, reindent=True, keyword_case='upper')

# Dictionary to store variable values
variable_values_cache = {}

# Function to collect variable values before starting test case execution
def collect_variable_values(test_cases_df):
    # Extract all unique variables from all SQL queries in the test cases
    all_sql_queries = ' '.join(test_cases_df['SQL Query'].dropna().tolist())
    variables = re.findall(r"##(.*?)##", all_sql_queries)
    
    # Prompt for each unique variable only once
    unique_variables = set(variables)
    for var in unique_variables:
        if var not in variable_values_cache:
            value = input(f"Enter value for variable '{var}': ")
            variable_values_cache[var] = value

# Function to replace variables in a given SQL query using collected values and remove any trailing semicolon
def replace_variables_in_sql(sql_query):
    for var, value in variable_values_cache.items():
        sql_query = sql_query.replace(f"##{var}##", value)
    
    # Remove any trailing semicolon from the query
    sql_query = sql_query.rstrip(';').strip()
    return sql_query

# Function to execute SQL and return status
def execute_sql_with_trino(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if sql_query.strip().lower().startswith(('select', 'with', 'show')):
            results = cursor.fetchall()
            logging.info(f"Query executed successfully: \n{format_sql(sql_query)}")
            return "COMPLETED", results
        else:
            logging.info(f"DDL executed successfully: \n{format_sql(sql_query)}")
            return "COMPLETED", "DDL statement executed"

    except Exception as e:
        logging.error(f"Error executing query: \n{format_sql(sql_query)}. Error: {str(e)}")
        return "ERROR", str(e)

# Function to generate the output filename
def generate_output_filename(excel_directory, team, env, timestamp):
    result_filename = f"trino_test_results_{team or 'All'}_{env}_{timestamp}.xlsx".replace(' ', '_')
    return os.path.join(excel_directory, result_filename)

# Function to save results to a new Excel file
def save_results_to_new_excel(file_path, df, sheet_name='Test Cases'):
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# Function to apply formatting (PASS = Green, FAIL = Red)
def apply_result_formatting(file_path, df, sheet_name='Test Cases'):
    green_fill = PatternFill(start_color="00FF00", end_color="00FF00", fill_type="solid")
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")

    workbook = load_workbook(file_path)
    sheet = workbook[sheet_name]

    for index, row in df.iterrows():
        result_cell = sheet[f'J{index + 2}']  # Column J is the 'Result'
        if row['Result'] == 'PASS':
            result_cell.fill = green_fill
        elif row['Result'] == 'FAIL':
            result_cell.fill = red_fill

    workbook.save(file_path)

# Function to get the team and environment selection
def get_team_and_env_selection(test_cases_df, trino_env_df):
    teams = sorted(test_cases_df['Team'].unique())
    
    print("Select the team for which test cases should be executed:")
    print("0. All")
    for idx, team in enumerate(teams, start=1):
        print(f"{idx}. {team}")
    
    selected_team_option = int(input("Enter the number corresponding to your choice: "))
    selected_team = None if selected_team_option == 0 else teams[selected_team_option - 1]

    envs = sorted(trino_env_df['Env'].unique())
    
    print("\nSelect the environment to be used:")
    for idx, env in enumerate(envs, start=1):
        print(f"{idx}. {env}")

    selected_env_option = int(input("Enter the number corresponding to your choice: "))
    selected_env = envs[selected_env_option - 1]
    
    return selected_team, selected_env

# Main function to process test cases
def process_test_cases(file_path):
    # Read data from Excel sheets
    test_cases_df, users_df, trino_env_df = read_excel_data(file_path)
    
    # Get the directory of the provided Excel file
    excel_directory = os.path.dirname(file_path)

    selected_team, selected_env = get_team_and_env_selection(test_cases_df, trino_env_df)

    # Filter test cases based on Execution Type
    setup_test_cases = test_cases_df[test_cases_df['Execution Type'] == 'Setup']
    cleanup_test_cases = test_cases_df[test_cases_df['Execution Type'] == 'Clean up']
    
    # Select only the 'Test' test cases for the selected team
    test_cases_df = test_cases_df[
        (test_cases_df['Execution Type'] == 'Test') & 
        (test_cases_df['Team'] == selected_team)
    ]

    # Combine all test cases in the correct order: Setup -> Test -> Clean up
    ordered_test_cases_df = pd.concat([setup_test_cases, test_cases_df, cleanup_test_cases], ignore_index=True)

    # Convert 'Actual Status', 'Result', add 'Executed SQL' and 'Error Message' columns
    ordered_test_cases_df['Actual Status'] = ordered_test_cases_df['Actual Status'].astype(object)
    ordered_test_cases_df['Result'] = ordered_test_cases_df['Result'].astype(object)
    ordered_test_cases_df['Executed SQL'] = ""  # Initialize an empty column for Executed SQL
    ordered_test_cases_df['Error Message'] = ""  # Initialize an empty column for Error Messages

    # Collect all SQL variable values before execution
    collect_variable_values(ordered_test_cases_df)

    user_passwords = get_user_passwords(users_df, selected_env)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    log_filepath = setup_logging(excel_directory, selected_team, selected_env, timestamp)
    output_filename = generate_output_filename(excel_directory, selected_team, selected_env, timestamp)

    # Execute each test case in the ordered list
    for index, test_case in ordered_test_cases_df.iterrows():
        test_case_number = test_case['Test Case Number']
        team = test_case['Team']
        instance_type = test_case['Trino Instance Type']
        sql_query = test_case['SQL Query']
        execution_type = test_case['Execution Type']
        expected_status = test_case['Expected Status']
        group = test_case['Group']
        use_case = test_case['Use Case']

        logging.info(f"Executing Test Case Number: {test_case_number} | Execution Type: {execution_type}")
        logging.info(f"Use Case: {use_case}")

        # Replace variables in the SQL query using collected values
        executed_sql = replace_variables_in_sql(sql_query)
        logging.info(f"SQL query after variable replacement: \n{format_sql(executed_sql)}")

        # Save the actual executed SQL to the DataFrame
        ordered_test_cases_df.at[index, 'Executed SQL'] = executed_sql

        trino_env_row = trino_env_df[
            (trino_env_df['Team'] == team) &
            (trino_env_df['Trino Instance Type'] == instance_type) &
            (trino_env_df['Env'] == selected_env)
        ]

        if trino_env_row.empty:
            ordered_test_cases_df.at[index, 'Actual Status'] = 'ERROR'
            ordered_test_cases_df.at[index, 'Result'] = 'FAIL'
            ordered_test_cases_df.at[index, 'Error Message'] = 'Host URL not found'
            logging.info(f"Host URL not found for Team: {team}, Instance Type: {instance_type}, and Env: {selected_env}")
            continue

        host_url = trino_env_row.iloc[0]['Host URL']
        user_row = users_df[(users_df['Env'] == selected_env) & (users_df['Group'] == group)]
        
        if user_row.empty:
            ordered_test_cases_df.at[index, 'Actual Status'] = 'ERROR'
            ordered_test_cases_df.at[index, 'Result'] = 'FAIL'
            ordered_test_cases_df.at[index, 'Error Message'] = 'User not found'
            logging.info(f"User not found for Group: {group} in Environment: {selected_env}")
            continue

        user = user_row.iloc[0]['User']
        password = user_passwords[user]

        try:
            # Attempt to get or reuse an existing connection from the pool
            conn = get_or_create_trino_connection(host_url, user, password)
        except Exception as e:
            # If connection fails, capture the error and proceed with the next test case
            error_message = f"Connection failed: {str(e)}"
            ordered_test_cases_df.at[index, 'Actual Status'] = 'ERROR'
            ordered_test_cases_df.at[index, 'Result'] = 'FAIL'
            ordered_test_cases_df.at[index, 'Error Message'] = error_message
            logging.info(f"Failed to connect for Test Case Number {test_case_number}: {error_message}")
            continue
        
        actual_status, response = execute_sql_with_trino(conn, executed_sql)
        ordered_test_cases_df.at[index, 'Actual Status'] = actual_status

        # Capture the error message in the "Error Message" column if execution failed
        if actual_status == 'ERROR':
            ordered_test_cases_df.at[index, 'Error Message'] = response
        
        logging.info(f"Response for Test Case Number {test_case_number}: {response}")

        if actual_status == expected_status:
            ordered_test_cases_df.at[index, 'Result'] = 'PASS'
        else:
            ordered_test_cases_df.at[index, 'Result'] = 'FAIL'

    save_results_to_new_excel(output_filename, ordered_test_cases_df)
    apply_result_formatting(output_filename, ordered_test_cases_df)

    print(f"Results have been saved to: {output_filename}")
    print(f"Logs have been saved to: {log_filepath}")

# Example usage
if __name__ == "__main__":
    excel_file_path = input("Please provide the Excel file path: ")
    process_test_cases(excel_file_path)
