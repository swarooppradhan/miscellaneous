import pandas as pd
import trino
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import getpass
import logging
import datetime
import sqlparse
import re  # Import for handling regex

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
    test_cases_df = pd.read_excel(file_path, sheet_name='Test Cases')
    users_df = pd.read_excel(file_path, sheet_name='Users')
    trino_env_df = pd.read_excel(file_path, sheet_name='Trino Env')
    return test_cases_df, users_df, trino_env_df

# Function to get passwords for unique users based on the selected environment
def get_user_passwords(users_df, selected_env):
    user_passwords = {}
    # Filter users based on the selected environment
    env_specific_users = users_df[users_df['Env'] == selected_env]
    
    for user in env_specific_users['User'].unique():
        password = getpass.getpass(prompt=f"Enter password for user '{user}' in environment '{selected_env}': ")
        user_passwords[user] = password
    return user_passwords

# Function to get Trino connection with HTTPS and port 443
def get_trino_connection(host_url, user, password):
    return trino.dbapi.connect(
        host=host_url,
        port=443,  # Using HTTPS port 443
        user=user,
        password=password,
        http_scheme='https'  # Use 'https' for secure connections
    )

# Function to pretty format the SQL query
def format_sql(sql_query):
    return sqlparse.format(sql_query, reindent=True, keyword_case='upper')

# Function to replace variables in the SQL query
def replace_variables_in_sql(sql_query):
    # Find all variable placeholders in the format ##variable_name##
    variables = re.findall(r"##(.*?)##", sql_query)
    
    for var in variables:
        # Prompt user to enter a value for each variable
        value = input(f"Enter value for variable '{var}': ")
        # Replace the variable placeholder with the entered value in the query
        sql_query = sql_query.replace(f"##{var}##", value)
    
    return sql_query

# Function to execute SQL and return status
def execute_sql_with_trino(conn, sql_query):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        # Check if the query starts with SELECT, WITH, or SHOW
        if sql_query.strip().lower().startswith(('select', 'with', 'show')):
            results = cursor.fetchall()  # Fetch results for SELECT, WITH, or SHOW queries
            logging.info(f"Query executed successfully: \n{format_sql(sql_query)}")
            return "COMPLETED", results
        else:
            logging.info(f"DDL executed successfully: \n{format_sql(sql_query)}")
            return "COMPLETED", "DDL statement executed"

    except Exception as e:
        logging.error(f"Error executing query: \n{format_sql(sql_query)}. Error: {str(e)}")
        return "ERROR", str(e)

# Function to generate the output filename
def generate_output_filename(team, env, timestamp):
    return f"trino_test_results_{team or 'All'}_{env}_{timestamp}.xlsx"

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
        result_cell = sheet[f'I{index + 2}']  # Column I is the 'Result'
        if row['Result'] == 'PASS':
            result_cell.fill = green_fill
        elif row['Result'] == 'FAIL':
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
    envs = sorted(trino_env_df['Env'].unique())
    
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

    # Get passwords for unique users based on the selected environment
    user_passwords = get_user_passwords(users_df, selected_env)

    # Get the current timestamp for filenames
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Set up logging with the appropriate log filename
    log_filename = setup_logging(selected_team, selected_env, timestamp)
    
    # Generate the output Excel filename
    output_filename = generate_output_filename(selected_team, selected_env, timestamp)

    # Loop through each test case
    for index, test_case in test_cases_df.iterrows():
        test_case_number = test_case['Test Case Number']
        team = test_case['Team']
        instance_type = test_case['Trino Instance Type']
        sql_query = test_case['SQL Query']
        expected_status = test_case['Expected Status']
        group = test_case['Group']
        use_case = test_case['Use Case']

        # Log the current test case number, use case, and query being executed
        logging.info(f"Executing Test Case Number: {test_case_number}")
        logging.info(f"Use Case: {use_case}")

        # Replace variables in the SQL query if present
        sql_query = replace_variables_in_sql(sql_query)
        logging.info(f"SQL query after variable replacement: \n{format_sql(sql_query)}")

        # Get the correct host URL for the team, instance type, and selected environment
        trino_env_row = trino_env_df[
            (trino_env_df['Team'] == team) &
            (trino_env_df['Trino Instance Type'] == instance_type) &
            (trino_env_df['Env'] == selected_env)
        ]

        if trino_env_row.empty:
            test_cases_df.at[index, 'Actual Status'] = 'ERROR'
            test_cases_df.at[index, 'Result'] = 'FAIL'
            logging.error(f"Host URL not found for Team: {team}, Instance Type: {instance_type}, and Env: {selected_env}")
            continue

        host_url = trino_env_row.iloc[0]['Host URL']

        # Identify the user based on the selected environment and group
        user_row = users_df[(users_df['Env'] == selected_env) & (users_df['Group'] == group)]
        if user_row.empty:
            test_cases_df.at[index, 'Actual Status'] = 'ERROR'
            test_cases_df.at[index, 'Result'] = 'FAIL'
            logging.error(f"User not found for Group: {group} in Environment: {selected_env}")
            continue

        user = user_row.iloc[0]['User']
        password = user_passwords[user]

        # Establish connection to Trino
        conn = get_trino_connection(host_url, user, password)

        # Execute the SQL query and capture actual status
        actual_status, response = execute_sql_with_trino(conn, sql_query)
        test_cases_df.at[index, 'Actual Status'] = actual_status

        # Log the response from the query execution
        logging.info(f"Response for Test Case Number {test_case_number}: {response}")

        # Compare actual status with expected status and determine result
        if actual_status == expected_status:
            test_cases_df.at[index, 'Result'] = 'PASS'
        else:
            test_cases_df.at[index, 'Result'] = 'FAIL'

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
