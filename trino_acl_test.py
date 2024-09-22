import pandas as pd
import trino
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import getpass
import logging
import datetime

# Function to set up logging with the desired log file name
def setup_logging(team, env, timestamp):
    log_filename = f"trino_test_log_{team or 'All'}_{env}_{timestamp}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return log_filename

# Function to read the data from the Excel sheets
def read_excel_data(file_path):
    test_cases_df = pd.read_excel(file_path, sheet_name='Test cases')
    users_df = pd.read_excel(file_path, sheet_name='Users')
    trino_env_df = pd.read_excel(file_path, sheet_name='Trino env')
    return test_cases_df, users_df, trino_env_df

# Function to get passwords for unique users
def get_user_passwords(users_df):
    user_passwords = {}
    for user in users_df['user'].unique():
        password = getpass.getpass(prompt=f"Enter password for user '{user}': ")
        user_passwords[user] = password
    return user_passwords

# Function to get Trino connection
def get_trino_connection(host_url, user, password):
    return trino.dbapi.connect(
        host=host_url,
        port=8080,  # Adjust port if necessary
        user=user,
        password=password,
        http_scheme='http'
    )

# Function to execute SQL and return status
def execute_sql_with_trino(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if sql_query.strip().lower().startswith(('select', 'with')):
            results = cursor.fetchall()  # Only fetch results for SELECT queries
            logging.info(f"Query executed successfully: {sql_query}")
            return "COMPLETED", results
        else:
            logging.info(f"DDL executed successfully: {sql_query}")
            return "COMPLETED", "DDL statement executed"

    except Exception as e:
        logging.error(f"Error executing query: {sql_query}. Error: {str(e)}")
        return "ERROR", str(e)

# Function to generate the output filename
def generate_output_filename(team, env, timestamp):
    return f"trino_test_results_{team or 'All'}_{env}_{timestamp}.xlsx"

# Function to save results to a new Excel file
def save_results_to_new_excel(file_path, df, sheet_name='Test cases'):
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# Function to apply formatting (PASS = Green, FAIL = Red)
def apply_result_formatting(file_path, df, sheet_name='Test cases'):
    green_fill = PatternFill(start_color="00FF00", end_color="00FF00", fill_type="solid")
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")

    workbook = load_workbook(file_path)
    sheet = workbook[sheet_name]

    for index, row in df.iterrows():
        result_cell = sheet[f'H{index + 2}']  # Column H is the 'Result'
        if row['result'] == 'PASS':
            result_cell.fill = green_fill
        elif row['result'] == 'FAIL':
            result_cell.fill = red_fill

    workbook.save(file_path)

# Function to get the team and environment selection
def get_team_and_env_selection(test_cases_df, trino_env_df):
    # Get the unique teams in alphabetical order
    teams = sorted(test_cases_df['Team'].unique())
    
    # Display the teams as a numbered list
    print("Select the team for which test cases should be executed:")
    print("0. All")
    for idx, team in enumerate(teams, start=1):
        print(f"{idx}. {team}")
    
    # Prompt user to select the team
    selected_team_option = int(input("Enter the number corresponding to your choice: "))
    selected_team = None if selected_team_option == 0 else teams[selected_team_option - 1]

    # Get distinct environments in alphabetical order
    envs = sorted(trino_env_df['env'].unique())
    
    # Display the environments as a numbered list
    print("\nSelect the environment to be used:")
    for idx, env in enumerate(envs, start=1):
        print(f"{idx}. {env}")

    # Prompt user to select the environment
    selected_env_option = int(input("Enter the number corresponding to your choice: "))
    selected_env = envs[selected_env_option - 1]
    
    return selected_team, selected_env

# Main function to process test cases
def process_test_cases(file_path):
    # Read data from Excel sheets
    test_cases_df, users_df, trino_env_df = read_excel_data(file_path)

    # Prompt user to select the team and environment
    selected_team, selected_env = get_team_and_env_selection(test_cases_df, trino_env_df)

    # Filter test cases based on the selected team
    if selected_team:
        test_cases_df = test_cases_df[test_cases_df['Team'] == selected_team]

    # Get passwords for unique users
    user_passwords = get_user_passwords(users_df)

    # Get the current timestamp for filenames
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Set up logging with the appropriate log filename
    log_filename = setup_logging(selected_team, selected_env, timestamp)
    
    # Generate the output Excel filename
    output_filename = generate_output_filename(selected_team, selected_env, timestamp)

    # Loop through each test case
    for index, test_case in test_cases_df.iterrows():
        team = test_case['Team']
        instance_type = test_case['trino instance type']
        sql_query = test_case['SQL query']
        expected_status = test_case['expected status']
        group = test_case['group']
        use_case = test_case['Use case']

        # Log the current use case and query being executed
        logging.info(f"Executing use case: {use_case}")
        logging.info(f"SQL query: {sql_query}")

        # Get the correct host URL for the team, instance type, and selected environment
        trino_env_row = trino_env_df[
            (trino_env_df['Team'] == team) &
            (trino_env_df['trino instance type'] == instance_type) &
            (trino_env_df['env'] == selected_env)
        ]

        if trino_env_row.empty:
            test_cases_df.at[index, 'actual status'] = 'ERROR'
            test_cases_df.at[index, 'result'] = 'FAIL'
            logging.error(f"Host URL not found for Team: {team}, Instance Type: {instance_type}, and Env: {selected_env}")
            continue

        host_url = trino_env_row.iloc[0]['host URL']

        # Get the user and password for the group
        user_row = users_df[users_df['group'] == group]
        if user_row.empty:
            test_cases_df.at[index, 'actual status'] = 'ERROR'
            test_cases_df.at[index, 'result'] = 'FAIL'
            logging.error(f"User not found for group: {group}")
            continue

        user = user_row.iloc[0]['user']
        password = user_passwords[user]

        # Establish connection to Trino
        conn = get_trino_connection(host_url, user, password)

        # Execute the SQL query and capture actual status
        actual_status, response = execute_sql_with_trino(conn, sql_query)
        test_cases_df.at[index, 'actual status'] = actual_status

        # Log the response from the query execution
        logging.info(f"Response for query execution: {response}")

        # Compare actual status with expected status and determine result
        if actual_status == expected_status:
            test_cases_df.at[index, 'result'] = 'PASS'
        else:
            test_cases_df.at[index, 'result'] = 'FAIL'

    # Save the results to a new Excel file
    save_results_to_new_excel(output_filename, test_cases_df)

    # Apply formatting based on results
    apply_result_formatting(output_filename, test_cases_df)

    print(f"Results have been saved to: {output_filename}")
    print(f"Logs have been saved to: {log_filename}")

# Example usage
if __name__ == "__main__":
    excel_file_path = input("Please provide the Excel file path: ")
    process_test_cases(excel_file_path)
