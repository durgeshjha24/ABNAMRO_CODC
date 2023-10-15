"""
This file contains parameters used for dataset generation and data processing.

Parameters:
- `column_mapping`: A dictionary that maps old column names to new column names for data column renaming. This is used to improve readability for business users.
    Example: {"id": "client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}

- `client_column_need_to_drop`: A list of column names that need to be dropped from the client dataset to remove personal information. This list should include columns such as first_name and last_name.
    Example: ['first_name', 'last_name']

- `finance_column_need_to_drop`: A list of column names that need to be dropped from the financial dataset to remove sensitive information, e.g., credit card numbers.
    Example: ['cc_n']

- `log_file_path_name`: A path and file name of logs where we want to store the logging of whole processing

- `target_file_path_name`: A path and file name of target dataset where we want to store the final outcome

These parameters are used in the dataset generation and data processing code to ensure the data is processed according to the specified requirements. Modify these parameters as needed for your specific dataset and data processing tasks.
"""



column_mapping = {
    "id": "client_identifier",
    "btc_a": "bitcoin_address",
    "cc_t": "credit_card_type"
}

client_column_need_to_drop=['first_name','last_name']

finance_column_need_to_drop=['cc_n']

log_file_path_name="logs/kommatipara_dataset.log"

target_file_path_name="client_data/"

client_file="data/dataset_one.csv"

finance_file="data/dataset_two.csv"

countries='"Netherlands" "United kingdom"'