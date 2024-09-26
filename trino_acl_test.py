import pandas as pd
import trino
from trino.auth import BasicAuthentication
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import getpass
import logging
import datetime
import sqlparse
import re
import os
import time
import threading

# Global flag to indicate when the execution is complete
execution_complete = False
# Dictionary to store reusable connections
connection_pool = {}
# Dictionary to store variable values
variable_values_cache = {}
# Dictionary to keep track of failed connections
failed_connections = set()

# Function to display the execution summary
def display_summary(ordered_test_cases_df, total_test_cases, refresh_frequency):
    while not execution_complete:
        executed_cases = ordered_test_cases_df[ordered_test_cases_df['Actual Status'].notna()]
        passed_cases = executed_cases[executed_cases['Result'] == 'PASS']
        failed_cases = executed_cases[executed_cases['Result'] == 'FAIL']

        print("\n" + "="*50)
        print(f"Total Test Cases: {total_test_cases}")
        print(f"Executed Test Cases: {len(executed_cases)}")
        print(f"Passed Test Cases: {len(passed_cases)}")
        print(f"Failed Test Cases: {len(failed_cases)}")
        print("="*50 + "\n")
        
        time.sleep(refresh_frequency * 60)  # Refresh based on user-defined frequency

    # Print the final summary once all test cases are executed
    executed_cases = ordered_test_cases_df[ordered_test_cases_df['Actual Status'].notna()]
    passed_cases = executed_cases[executed_cases['Result'] == 'PASS']
    failed_cases = executed_cases[executed_cases['Result'] == 'FAIL']

    print("\n" + "="*50)
    print("Final Summary")
    print(f"Total Test Cases: {total_test_cases}")
    print(f"Executed Test Cases: {len(executed_cases)}")
    print(f"Passed Test Cases: {len(passed_cases)}")
    print(f"Failed Test Cases: {len(failed_cases)}")
    print("="*50 + "\n")

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

# Function to collect variable values from the "SQL Variables" sheet and prompt if not found
def collect_variable_values(test_cases_df, sql_variables_df, selected_env):
    # Extract all unique variables from all SQL queries in the test cases
    all_sql_queries = ' '.join(test_cases_df['SQL Query'].dropna().tolist())
    variables = re.findall(r"##(.*?)##", all_sql_queries)
    
    # Get values for the selected environment from the SQL Variables sheet
    env_variable_values = sql_variables_df[sql_variables_df['Env'] == selected_env]

    # Prompt for each unique variable only once if not found in the SQL Variables sheet
    unique_variables = set(variables)
    for var in unique_variables:
        # Check if the variable exists in the SQL Variables sheet for the selected environment
        var_value_row = env_variable_values[env_variable_values['Variable'] == var]
        
        if not var_value_row.empty:
            value = var_value_row.iloc[0]['Value']
            variable_values_cache[var] = value  # Store the value in the cache
        elif var not in variable_values_cache:
            # Prompt user to enter a value for each variable if not already cached
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

# Function to get team selection
def get_selected_teams(test_cases_df):
    # Get a unique list of teams and present them as a numbered list
    teams = sorted(test_cases_df['Team'].unique())
    print("Available teams:")
    for idx, team in enumerate(teams, 1):
        print(f"{idx}. {team}")

    print("0. All Teams")  # Option to select all teams

    # Prompt user to enter team numbers as a comma-separated list
    selected_team_numbers = input("Enter the team numbers to execute (comma-separated, e.g., 1,3,5) or '0' for all: ")

    # Convert the input into a list of selected team indices
    selected_indices = [int(num.strip()) for num in selected_team_numbers.split(",") if num.strip().isdigit()]

    # Determine the actual selected teams
    if 0 in selected_indices:
        return teams  # Return all teams if '0' is selected
    else:
        return [teams[idx - 1] for idx in selected_indices if 1 <= idx <= len(teams)]

# Function to get environment selection
def get_selected_env(trino_env_df):
    envs = sorted(trino_env_df['Env'].unique())
    
    print("\nSelect the environment to be used:")
    for idx, env in enumerate(envs, start=1):
        print(f"{idx}. {env}")

    selected_env_option = int(input("Enter the number corresponding to your choice: "))
    selected_env = envs[selected_env_option - 1]
    
    return selected_env

# Function to execute test cases for a given set of test cases with thread name logging
def execute_test_cases(test_cases_df, trino_env_df, users_df, selected_env, user_passwords, sql_variables_df, log_filepath, execution_type):
    current_thread = threading.current_thread().name  # Get the current thread's name

    for index, test_case in test_cases_df.iterrows():
        test_case_number = test_case['Test Case Number']
        team = test_case['Team']
        instance_type = test_case['Trino Instance Type']
        sql_query = test_case['SQL Query']
        expected_status = test_case['Expected Status']
        group = test_case['Group']
        use_case = test_case['Use Case']

        logging.info(f"[{current_thread}] Executing {execution_type} Test Case Number: {test_case_number} | Team: {team}")
        logging.info(f"[{current_thread}] Use Case: {use_case}")

        # Replace variables in the SQL query using collected values
        executed_sql = replace_variables_in_sql(sql_query)
        logging.info(f"[{current_thread}] SQL query after variable replacement: \n{format_sql(executed_sql)}")

        # Save the actual executed SQL using .loc to avoid SettingWithCopyWarning
        test_cases_df.loc[index, 'Executed SQL'] = executed_sql

        trino_env_row = trino_env_df[
            (trino_env_df['Team'] == team) &
            (trino_env_df['Trino Instance Type'] == instance_type) &
            (trino_env_df['Env'] == selected_env)
        ]

        if trino_env_row.empty:
            test_cases_df.loc[index, 'Actual Status'] = 'ERROR'
            test_cases_df.loc[index, 'Result'] = 'FAIL'
            test_cases_df.loc[index, 'Error Message'] = 'Host URL not found'
            logging.info(f"[{current_thread}] Host URL not found for Team: {team}, Instance Type: {instance_type}, and Env: {selected_env}")
            continue

        host_url = trino_env_row.iloc[0]['Host URL']
        user_row = users_df[(users_df['Env'] == selected_env) & (users_df['Group'] == group)]
        
        if user_row.empty:
            test_cases_df.loc[index, 'Actual Status'] = 'ERROR'
            test_cases_df.loc[index, 'Result'] = 'FAIL'
            test_cases_df.loc[index, 'Error Message'] = 'User not found'
            logging.info(f"[{current_thread}] User not found for Group: {group} in Environment: {selected_env}")
            continue

        user = user_row.iloc[0]['User']
        password = user_passwords[user]

        # Check if this user and host URL combination has already failed
        connection_key = (host_url, user)
        if connection_key in failed_connections:
            test_cases_df.loc[index, 'Actual Status'] = 'ERROR'
            test_cases_df.loc[index, 'Result'] = 'FAIL'
            test_cases_df.loc[index, 'Error Message'] = 'Previous connection attempt failed for this user and host URL'
            logging.info(f"[{current_thread}] Skipping Test Case Number {test_case_number} due to prior connection failure for Host URL: {host_url} and User: {user}")
            continue

        try:
            # Attempt to get or reuse an existing connection from the pool
            conn = get_or_create_trino_connection(host_url, user, password)
        except Exception as e:
            # If connection fails, capture the error, mark this combination as failed, and proceed with the next test case
            error_message = f"Connection failed: {str(e)}"
            test_cases_df.loc[index, 'Actual Status'] = 'ERROR'
            test_cases_df.loc[index, 'Result'] = 'FAIL'
            test_cases_df.loc[index, 'Error Message'] = error_message
            logging.info(f"[{current_thread}] Failed to connect for Test Case Number {test_case_number}: {error_message}")
            
            # Add this host URL and user combination to the failed_connections set
            failed_connections.add(connection_key)
            continue
        
        actual_status, response = execute_sql_with_trino(conn, executed_sql)
        test_cases_df.loc[index, 'Actual Status'] = actual_status

        # Capture the error message in the "Error Message" column if execution failed
        if actual_status == 'ERROR':
            test_cases_df.loc[index, 'Error Message'] = response
        
        logging.info(f"[{current_thread}] Response for Test Case Number {test_case_number}: {response}")

        if actual_status == expected_status:
            test_cases_df.loc[index, 'Result'] = 'PASS'
        else:
            test_cases_df.loc[index, 'Result'] = 'FAIL'

def process_test_cases(file_path):
    global execution_complete  # Use the global execution_complete flag

    # Read data from Excel sheets
    test_cases_df, users_df, trino_env_df = read_excel_data(file_path)
    sql_variables_df = pd.read_excel(file_path, sheet_name='SQL Variables')
    
    # Get the directory of the provided Excel file
    excel_directory = os.path.dirname(file_path)

    selected_env = get_selected_env(trino_env_df)

    # Prompt the user for refresh frequency in minutes
    refresh_frequency = int(input("Enter the refresh frequency in minutes for the summary display: "))

    # Filter test cases based on Execution Type
    setup_test_cases = test_cases_df[test_cases_df['Execution Type'] == 'Setup']
    cleanup_test_cases = test_cases_df[test_cases_df['Execution Type'] == 'Clean up']
    
    # Select teams based on user input
    selected_teams = get_selected_teams(test_cases_df)

    # Select only the 'Test' test cases for the selected teams
    team_test_cases_df = test_cases_df[
        (test_cases_df['Execution Type'] == 'Test') & 
        (test_cases_df['Team'].isin(selected_teams))
    ]

    # Combine all test cases in the correct order: Setup -> Test -> Clean up
    ordered_test_cases_df = pd.concat([setup_test_cases, team_test_cases_df, cleanup_test_cases], ignore_index=True)

    # Convert 'Actual Status', 'Result', add 'Executed SQL' and 'Error Message' columns
    ordered_test_cases_df['Actual Status'] = ordered_test_cases_df['Actual Status'].astype(object)
    ordered_test_cases_df['Result'] = ordered_test_cases_df['Result'].astype(object)
    ordered_test_cases_df['Executed SQL'] = ""  # Initialize an empty column for Executed SQL
    ordered_test_cases_df['Error Message'] = ""  # Initialize an empty column for Error Messages

    total_test_cases = len(ordered_test_cases_df)

    # Collect all SQL variable values before execution
    collect_variable_values(ordered_test_cases_df, sql_variables_df, selected_env)

    user_passwords = get_user_passwords(users_df, selected_env)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    log_filepath = setup_logging(excel_directory, ','.join(selected_teams), selected_env, timestamp)
    output_filename = generate_output_filename(excel_directory, ','.join(selected_teams), selected_env, timestamp)

    # Execute the "Setup" test cases
    execute_test_cases(setup_test_cases, trino_env_df, users_df, selected_env, user_passwords, sql_variables_df, log_filepath, "Setup")

    # Start the summary display thread
    summary_thread = threading.Thread(target=display_summary, args=(ordered_test_cases_df, total_test_cases, refresh_frequency), daemon=True)
    summary_thread.start()

    # Execute test cases for each selected team in parallel
    team_threads = []
    for team in selected_teams:
        team_specific_df = team_test_cases_df[team_test_cases_df['Team'] == team]
        team_thread = threading.Thread(
            target=execute_test_cases, 
            args=(team_specific_df, trino_env_df, users_df, selected_env, user_passwords, sql_variables_df, log_filepath, "Test"),
            name=f"Team-{team}"
        )
        team_threads.append(team_thread)
        team_thread.start()

    # Wait for all team threads to finish
    for thread in team_threads:
        thread.join()

    # Execute the "Clean up" test cases after all team test cases are finished
    execute_test_cases(cleanup_test_cases, trino_env_df, users_df, selected_env, user_passwords, sql_variables_df, log_filepath, "Clean up")

    # Write the results to the Excel file first
    save_results_to_new_excel(output_filename, ordered_test_cases_df)
    apply_result_formatting(output_filename, ordered_test_cases_df)

    # Mark the execution as complete
    execution_complete = True
    # Wait for the summary thread to complete
    summary_thread.join()

    # Print the location of the saved files
    print(f"Results have been saved to: {output_filename}")
    print(f"Logs have been saved to: {log_filepath}")

# Example usage
if __name__ == "__main__":
    excel_file_path = input("Please provide the Excel file path: ")
    process_test_cases(excel_file_path)
