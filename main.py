import os
import pandas as pd
import numpy as np
import pytz


def data_cleaning(directory):
    # Get all csv files on the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]
    dfs = {}
    # Do the read and cleaning stage of each df
    for filename in csv_files:
        df_name = filename.split('.')[0]
        file_path = os.path.join(directory, filename)

        df = pd.read_csv(file_path)
        # print_dataframe(df, df_name)
        df = handle_null_values(df)
        # print_dataframe(df, df_name)
        df = convert_to_datetime(df, df_name)
        # print_dataframe(df, df_name)
        df = convert_numeric_types(df, df_name)
        # print_dataframe(df, df_name)
        df = handle_duplicates(df, df_name)
        # print_dataframe(df, df_name)
        save_dataframe(df, df_name, 'csv_cleaned')

        # for checking dataframe details
        # print_dataframe(df, df_name)
        # print_column_types(df, df_name)

        dfs[df_name] = df
    return dfs


# For checking result of df
def print_dataframe(df, df_name):
    print(f"DataFrame: {df_name}")
    print(df.head())


# For checking data type result of df
def print_column_types(df, df_name):
    print(f"\nDataFrame: {df_name}")
    print("Column types:")
    print(df.dtypes)
    print()


def convert_to_datetime(df, df_name):
    # Dictionary containing the DataFrame names and their corresponding columns that need to be converted to datetime
    timestamp_columns = {
        'referral_rewards': ['created_at'],
        'user_logs': ['membership_expired_date'],
        'lead_log': ['created_at'],
        'paid_transactions': ['transaction_at'],
        'user_referral_logs': ['created_at'],
        'user_referral_statuses': ['created_at'],
        'user_referrals': ['referral_at', 'updated_at'],
    }

    # Check if the given DataFrame name has any columns listed for datetime conversion
    if df_name in timestamp_columns:
        columns_to_convert = timestamp_columns[df_name]
        # Loop through each column that needs to be converted
        for column in columns_to_convert:
            # Check if the column exists in the DataFrame and is of type object (string)
            if column in df.columns and df[column].dtype == 'object':
                # Convert the column to datetime, coerce errors to NaT (Not a Time)
                df[column] = pd.to_datetime(df[column], errors='coerce')
                # If the datetime column does not have a timezone, localize it to UTC
                if df[column].dt.tz is None:
                    df[column] = df[column].dt.tz_localize('UTC')
                # Convert the timezone from UTC to Asia/Jakarta
                df[column] = df[column].dt.tz_convert('Asia/Jakarta')
    return df


def convert_numeric_types(df, df_name):
    # Dictionary containing the DataFrame names and their corresponding columns with target numeric types
    numeric_columns = {
        'user_referrals': {'user_referral_status_id': 'int64', 'referral_reward_id': 'int64'},
        'user_logs': {'id': 'int64'},
        'lead_log': {'id': 'int64'},
        'referral_rewards': {'id': 'int64', 'reward_type': 'int64'},
        'user_referral_logs': {'id': 'int64'},
        'user_referral_statuses': {'id': 'int64'},
    }

    # Check if the given DataFrame name has any columns listed for numeric type conversion
    if df_name in numeric_columns:
        columns_to_convert = numeric_columns[df_name]
        # Loop through each column and its target data type
        for column, dtype in columns_to_convert.items():
            # Check if the column exists in the DataFrame
            if column in df.columns:
                # Convert the column to the specified numeric type
                df[column] = df[column].astype(dtype)
    return df


def handle_null_values(df):
    # Loop through each column in the DataFrame
    for column in df.columns:
        # Check if the column is of numeric type
        if pd.api.types.is_numeric_dtype(df[column]):
            # Fill null values with 0 for numeric columns
            df[column].fillna(0, inplace=True)
        # For string columns, null values are kept as is
    return df


def handle_duplicates(df, df_name):
    # Dictionary containing the DataFrame names and their corresponding unique column for detecting duplicates
    unique_columns = {
        'user_referrals': 'referral_id',
        'lead_logs': 'id',
        'user_referral_logs': 'id',
        'user_logs': 'id',
        'user_referral_statuses': 'id',
        'referral_rewards': 'id',
        'paid_transactions': 'transaction_id',
    }

    # Check if the given DataFrame name has a unique column listed for duplicate detection
    if df_name in unique_columns:
        column_name = unique_columns[df_name]
        # Find all duplicate rows based on the unique column
        duplicates = df[df.duplicated(subset=[column_name], keep=False)]
        # If there are duplicates, drop all but the first occurrence
        if not duplicates.empty:
            df = df.drop_duplicates(subset=[column_name], keep='first')
    return df


def save_dataframe(df, df_name, output_directory):
    # Ensure the output directory exists; create it if it doesn't
    os.makedirs(output_directory, exist_ok=True)

    # Construct the output file path
    output_path = os.path.join(output_directory, f"{df_name}.csv")

    # Save the DataFrame to a CSV file without including the index
    df.to_csv(output_path, index=False)

    # Print the column types of the DataFrame (assuming print_column_types is defined elsewhere)
    print_column_types(df, df_name)

    # Print a message indicating where the cleaned DataFrame has been saved
    print(f"Cleaned DataFrame saved to {output_path}")


def data_processing(dfs):
    user_referrals = dfs.get('user_referrals')

    if user_referrals is not None:
        # Perform joins
        user_referral_statuses = dfs.get('user_referral_statuses')
        if user_referral_statuses is not None:
            user_referral_statuses = user_referral_statuses[['id', 'description']]

        paid_transactions = dfs.get('paid_transactions')
        if paid_transactions is not None:
            paid_transactions = paid_transactions[['transaction_id', 'transaction_status', 'transaction_at', 'transaction_location', 'timezone_transaction', 'transaction_type']]

        lead_log = dfs.get('lead_log')
        if lead_log is not None:
            lead_log = lead_log[['lead_id', 'source_category', 'preferred_location', 'timezone_location', 'current_status']]

        user_referral_logs = dfs.get('user_referral_logs')
        if user_referral_logs is not None:
            user_referral_logs = user_referral_logs[['user_referral_id', 'source_transaction_id', 'is_reward_granted']]

        referral_rewards = dfs.get('referral_rewards')
        if referral_rewards is not None:
            referral_rewards = referral_rewards[['id', 'reward_value', 'reward_type']]

        user_logs = dfs.get('user_logs')
        if user_logs is not None:
            user_logs = user_logs[['user_id', 'name', 'phone_number', 'homeclub', 'timezone_homeclub', 'membership_expired_date', 'is_deleted']]

        # Left joins on user_referrals
        if user_referral_statuses is not None:
            user_referrals = user_referrals.merge(
                user_referral_statuses,
                left_on='user_referral_status_id',
                right_on='id',
                how='left'
            ).drop(columns=['id'])  # Drop the primary key column

        if paid_transactions is not None:
            user_referrals = user_referrals.merge(
                paid_transactions,
                left_on='transaction_id',
                right_on='transaction_id',
                how='left'
            )

        if lead_log is not None:
            user_referrals = user_referrals.merge(
                lead_log,
                left_on='referee_id',
                right_on='lead_id',
                how='left'
            ).drop(columns=['lead_id'])  # Drop the primary key column

        if user_referral_logs is not None:
            user_referrals = user_referrals.merge(
                user_referral_logs,
                left_on='referral_id',
                right_on='user_referral_id',
                how='left'
            ).drop(columns=['user_referral_id'])  # Drop the primary key column

        if referral_rewards is not None:
            user_referrals = user_referrals.merge(
                referral_rewards,
                left_on='referral_reward_id',
                right_on='id',
                how='left'
            ).drop(columns=['id'])  # Drop the primary key column

        if user_logs is not None:
            user_referrals = user_referrals.merge(
                user_logs,
                left_on='referrer_id',
                right_on='user_id',
                how='left'
            ).drop(columns=['user_id'])  # Drop the primary key column

        # Handle any NaN values after joining
        user_referrals.fillna('', inplace=True)
        user_referrals = apply_initcap(user_referrals)
        user_referrals = create_referral_source_category(user_referrals)
        user_referrals = evaluate_business_logic(user_referrals)
        save_dataframe(user_referrals, 'main_table', 'csv_joined')
        return user_referrals
    else:
        raise ValueError("Main DataFrame 'user_referrals' not found")


def apply_initcap(df):
    # Loop through each column in the DataFrame
    for column in df.columns:
        # Check if the column is of object type (string)
        if pd.api.types.is_object_dtype(df[column]):
            # Apply the title() method to each string in the column
            df[column] = df[column].apply(lambda x: x.title() if isinstance(x, str) else x)
    return df


def create_referral_source_category(df):
    # Create a new column 'referral_source_category' based on the values in 'referral_source' and 'source_category'
    df['referral_source_category'] = np.where(
        df['referral_source'] == 'User Sign Up', 'Online',  # If 'referral_source' is 'User Sign Up', set to 'Online'
        np.where(
            df['referral_source'] == 'Draft Transaction', 'Offline',  # If 'referral_source' is 'Draft Transaction', set to 'Offline'
            np.where(
                df['referral_source'] == 'Lead', df['source_category'],  # If 'referral_source' is 'Lead', use the value from 'source_category'
                ''  # Otherwise, set to an empty string
            )
        )
    )
    return df


def evaluate_business_logic(df):
    # Ensure date columns are in datetime format
    df['transaction_at'] = pd.to_datetime(df['transaction_at'], errors='coerce')
    df['referral_at'] = pd.to_datetime(df['referral_at'], errors='coerce')
    df['membership_expired_date'] = pd.to_datetime(df['membership_expired_date'], errors='coerce')

    df['is_deleted'] = df['is_deleted'].astype(bool)
    df['is_reward_granted'] = df['is_reward_granted'].astype(bool)

    # Define today's date
    jakarta_timezone = pytz.timezone('Asia/Jakarta')
    today = pd.Timestamp.now(jakarta_timezone)

    # Conditions for Valid Referral Rewards
    condition1_valid = (
        (df['reward_value'].notnull()) &
        (df['description'] == 'Berhasil') &
        (df['transaction_id'].notnull()) &
        (df['transaction_status'] == 'Paid') &
        (df['transaction_type'] == 'New') &
        (df['transaction_at'] > df['referral_at']) &
        (df['transaction_at'].dt.to_period('M') == df['referral_at'].dt.to_period('M')) &
        (df['membership_expired_date'] >= today) &
        (~df['is_deleted']) &
        (df['is_reward_granted'])
    )

    condition2_valid = (
        (df['description'].isin(['Menunggu', 'Tidak Berhasil'])) &
        (df['reward_value'].isnull())
    )

    # Conditions for Invalid Referral Rewards
    condition1_invalid = (
        (df['reward_value'].notnull()) &
        (df['description'] != 'Berhasil')
    )

    condition2_invalid = (
        (df['reward_value'].notnull()) &
        (df['transaction_id'].isnull())
    )

    condition3_invalid = (
        (df['reward_value'].isnull()) &
        (df['transaction_id'].notnull()) &
        (df['transaction_status'] == 'Paid') &
        (df['transaction_at'] < df['referral_at'])
    )

    condition4_invalid = (
        (df['description'] == 'Berhasil') &
        (df['reward_value'].isnull())
    )

    condition5_invalid = (
        (df['transaction_at'] < df['referral_at'])
    )

    # Determine validity
    df['is_business_logic_valid'] = (
        condition1_valid |
        condition2_valid
    ) & ~(
        condition1_invalid |
        condition2_invalid |
        condition3_invalid |
        condition4_invalid |
        condition5_invalid
    )

    return df


if __name__ == "__main__":
    dfs = data_cleaning('csv_sources')
    joined_df = data_processing(dfs)
