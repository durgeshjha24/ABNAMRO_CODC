# Import necessary libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,trim
import argparse
import logging
from logging.handlers import RotatingFileHandler

from config import(
   column_mapping,
   client_column_need_to_drop,
   finance_column_need_to_drop,
   log_file_path_name,
   target_file_path_name,
)

def parsing_arguments():
    
    """
    Parse command-line arguments for the data processing.

    This function sets up an argument parser to capture the necessary input parameters for data processing. It expects the following arguments:
    - 'client_file': Path to the client dataset.
    - 'financial_file': Path to the financial dataset.
    - 'countries': A list of countries used to filter the client data.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.

    """

    parser = argparse.ArgumentParser(description="Process client data")
    parser.add_argument("client_file", help="Path to the client dataset")
    parser.add_argument("financial_file", help="Path to the financial dataset")
    parser.add_argument("countries", help="Countries to filter", nargs="+")
    args = parser.parse_args()
    
    return args

def read_dataset(logger,client_file_path, finance_file_path):
    
    """
    Read and process dataset files.

    This function reads and processes client and finance datasets from the provided file paths. It performs the following steps:
    1. Creates a Spark session for data processing.
    2. Reads the client dataset in CSV format, and trims and converts the 'country' column to lowercase for consistency.
    3. Reads the finance dataset in CSV format.

    Args:
        client_file_path (str): Path to the client dataset file.
        finance_file_path (str): Path to the financial dataset file.

    Returns:
        two DataFrames - one for the client dataset and one for the finance dataset.

    Example:
    df_client, df_finance = read_dataset("/data/client.csv", "/data/financial.csv")
    """

    spark = SparkSession.builder.master("local[*]").appName("Read_data").getOrCreate()
    logger.info(f"Spark session created successfully")

    logger.info(f"Started reading Client dataset")
    df_client = spark.read.format("csv").option("header","true").load(client_file_path)
    df_client= df_client.withColumn('country',lower(trim(df_client['country'])))
    logger.info(f"Finished reading Client dataset including triming of country column")

    logger.info(f"Started reading Finance dataset")
    df_finance = spark.read.format("csv").option("header","true").load(finance_file_path)
    logger.info(f"Started reading Finance dataset")
    return df_client, df_finance

def process_files_data(logger,df_client,df_finance,list_of_countries):
   
   """
    Process client and finance data to generate a final dataset.

    This function takes two DataFrames, 'df_client' and 'df_finance', and a list of countries as input. It performs several data processing tasks, including:
    1. Filtering clients based on the specified list of countries and dropping personal identifiable information columns from the client dataset.
    2. Removing credit card numbers from the financial dataset.
    3. Renaming columns in both datasets as per the assignment instructions.
    4. Joining the two datasets on the 'client_identifier' field to generate the final client data.

    Args:
        df_client (DataFrame): Client dataset to be processed.
        df_finance (DataFrame): Financial dataset to be processed.
        list_of_countries (list): List of countries used to filter the client data.

    Returns:
        DataFrame: The final client dataset with processed data.

    Example:
    final_data = process_files_data(df_client, df_finance, ["united kingdom", "netherlands"])
    """
   
   # Filter clients from specified countries and dropping personal identifiable information from the client dataset
   logger.info(f"Started filtering clients from specified countries and dropping personal identifiable information columns {client_column_need_to_drop} from the client dataset")
   list_of_countries= [element.lower() for element  in list_of_countries]
   df_client = df_client.filter(col("country").isin(list_of_countries)).drop(*client_column_need_to_drop)
   logger.info(f"Finished filtering clients from specified countries and dropping personal identifiable information columns {client_column_need_to_drop}  from the client dataset")
   
   # Remove credit card numbers from the financial dataset
   logger.info(f"Started dropping {finance_column_need_to_drop} columns from the financial dataset")
   df_finance = df_finance.drop(*finance_column_need_to_drop)
   logger.info(f"Finished dropping {finance_column_need_to_drop} columns from the financial dataset")
   
   # Rename columns of datasets as per instruction given in assignment 
   logger.info(f"Started renaming columns for better understanding")
   for old_col, new_col in column_mapping.items():
      df_finance = df_finance.withColumnRenamed(old_col, new_col)
      df_client= df_client.withColumnRenamed(old_col, new_col)
   logger.info(f"Finished renaming columns")
   
   # Join the two datasets on the "client_identifier" field to get final outcome 
   logger.info(f"Performing  join on client and finance dataset")
   df_final_client_data = df_client.join(df_finance, "client_identifier", "inner")
   logger.info(f"Finished join on client and finance dataset")
   return df_final_client_data

def rotating_logger(log_file_path_name):
   
   # Set up logging
   logger = logging.getLogger("my_logger")
   logger.setLevel(logging.INFO)

   # Define log file path and maximum log file size (in bytes)
   max_log_file_size = 1024 * 1024  # 1 MB

   # Create a RotatingFileHandler with log file rotation
   file_handler = RotatingFileHandler(log_file_path_name, maxBytes=max_log_file_size, backupCount=5)

   # Configure the log format
   formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
   file_handler.setFormatter(formatter)

   # Create a console handler for displaying log messages on the console
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setLevel(logging.INFO)
   console_handler.setFormatter(formatter)

   # Add the file handler to the logger
   logger.addHandler(file_handler)
   logger.addHandler(console_handler)

   # Parse command-line arguments
   logger.info(f"Started parsing arguments")
   args = parsing_arguments()
   logger.info(f"Parsing arguments completed")

   return logger
   

def main():
   """
    Main function for executing data processing and logging workflow.

    This function serves as the entry point for the data processing and logging workflow. It performs the following tasks:
    1. Sets up logging to capture informative messages and potential errors, both to a log file and console.
    2. Parses command-line arguments to obtain necessary input parameters.
    3. Reads client and finance datasets using the provided file paths.
    4. Processes the datasets to generate the final client data.
    5. Writes the final dataset to the target directory.
    
    Logging:
    - A log file is created to record detailed information about the data processing.
    - Console output is configured to display real-time progress.

    Command-Line Arguments:
    - Parses command-line arguments to provide input parameters to the data processing function.
    - Arguments include the paths to client and financial datasets and the countries to filter.

    Data Processing:
    - Invokes the 'read_dataset' and 'process_files_data' functions for data processing.
    - Captures successful data processing and logs any exceptions encountered during the process.

    Example:
    main()
   """

#    logger=rotating_logger(log_file_path_name)

#    # Parse command-line arguments
#    logger.info(f"Started parsing arguments")
#    args = parsing_arguments()
#    logger.info(f"Parsing arguments completed")
   
   try:
      
      #Invoke logger function

      logger=rotating_logger(log_file_path_name)

      # Parse command-line arguments
      logger.info(f"Started parsing arguments")
      args = parsing_arguments()
      logger.info(f"Parsing arguments completed")
      
      #Reading client & finance data
      df_client,df_finance=read_dataset(logger, args.client_file, args.financial_file)

      #Performing data processing to create dataset
      df_final_client_data=process_files_data(logger,df_client,df_finance, args.countries)
      
      #Wrting final outcome dataset
      logger.info(f"Started writing the final outcome of datasets")
      df_final_client_data.coalesce(1).write.csv(target_file_path_name, header=True, mode="overwrite")
      logger.info(f"Dataset is ready for end user")
      logger.info(f"===============================End=================================================")

   except Exception as e:
    logger.error(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main()

