# Import necessary libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,trim
import argparse
import logging
from config import(
   column_mapping,
   client_column_need_to_drop,
   finance_column_need_to_drop,
   log_file_path_name,
   target_file_path_name,
)

def process_files_data(client_file_path, finance_file_path, list_of_countries):
   
   """
    Process client and finance data, including filtering, cleaning, and joining.

    Args:
        client_file_path (str): Path to the client dataset file.
        finance_file_path (str): Path to the financial dataset file.
        list_of_countries (list): List of countries to filter client data.

    Returns:
        None

    This function reads client and financial datasets, applies data transformations and filters based on the provided list of countries. 
    It also drops specific columns from both datasets, renames columns, and performs a join operation to generate a final dataset. 
    The final dataset is written to a target directory.

    Note:
    - The client dataset's 'country' column is trimmed and converted to lowercase for consistency.
    - Columns specified in 'client_column_need_to_drop' are dropped to remove personal identifiable information.
    - Columns specified in 'finance_column_need_to_drop' are dropped to remove sensitive financial information.
    - Columns are renamed according to the 'column_mapping' dictionary.

    Examples:
    process_files_data("client.csv", "financial.csv", ["united kingdom", "netherlands"])
   """
   
   spark = SparkSession.builder.master("local[*]").appName("Read_data").getOrCreate()
   logging.info(f"Spark session created successfully")

   logging.info(f"Started reading Client dataset")
   df_client = spark.read.format("csv").option("header","true").load(client_file_path)
   df_client= df_client.withColumn('country',lower(trim(df_client['country'])))
   logging.info(f"Finished reading Client dataset including triming of country column")

   logging.info(f"Started reading Finance dataset")
   df_finance = spark.read.format("csv").option("header","true").load(finance_file_path)
   logging.info(f"Started reading Finance dataset")

   # Filter clients from specified countries and dropping personal identifiable information from the client dataset
   logging.info(f"Started filtering clients from specified countries and dropping personal identifiable information columns {client_column_need_to_drop} from the client dataset")
   list_of_countries= [element.lower() for element  in list_of_countries]
   df_client = df_client.filter(col("country").isin(list_of_countries)).drop(*client_column_need_to_drop)
   logging.info(f"Finished filtering clients from specified countries and dropping personal identifiable information columns {client_column_need_to_drop}  from the client dataset")

   # Remove credit card numbers from the financial dataset
   logging.info(f"Started dropping {finance_column_need_to_drop} columns from the financial dataset")
   df_finance = df_finance.drop(*finance_column_need_to_drop)
   logging.info(f"Finished dropping {finance_column_need_to_drop} columns from the financial dataset")

   # Rename columns of datasets as per instruction given in assignment 
   logging.info(f"Started renaming columns for better understanding")
   for old_col, new_col in column_mapping.items():
      df_finance = df_finance.withColumnRenamed(old_col, new_col)
      df_client= df_client.withColumnRenamed(old_col, new_col)
   logging.info(f"Finished renaming columns")
   
   
   # Join the two datasets on the "client_identifier" field to get final outcome 
   logging.info(f"Performing  join on client and finance dataset")
   df_final_client_data = df_client.join(df_finance, "client_identifier", "inner")
   logging.info(f"Finished join on client and finance dataset")


   logging.info(f"Started wrting the final outcome of datasets")
   df_final_client_data.coalesce(1).write.csv(target_file_path_name, header=True, mode="overwrite")
   logging.info(f"Dataset is ready for end user")

def main():
   """
    Main function for data processing and logging.

    This function serves as the entry point for the application. It sets up logging to capture informative messages and 
    potential errors. It also handles command-line argument parsing and invokes the data processing function.

    Logging:
    - A log file is created to record detailed information about the data processing.
    - Console output is configured to display real-time progress.
    
    Command-Line Arguments:
    - Parses command-line arguments to provide input parameters to the data processing function.
    - Arguments include the paths to client and financial datasets and the countries to filter.

    Data Processing:
    - Invokes the `process_files_data` function to perform data transformations and generate the final dataset.
    - Captures successful data processing and logs any exceptions encountered during the process.

    """
    # Set up logging
   logging.basicConfig(level=logging.INFO, filename=log_file_path_name, filemode="a")
   console = logging.StreamHandler()
   console.setLevel(logging.INFO)
   formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
   console.setFormatter(formatter)
   logging.getLogger("").addHandler(console)
   
   # Parse command-line arguments
   logging.info(f"Started parsing arguments")
   parser = argparse.ArgumentParser(description="Process client data")
   parser.add_argument("client_file", help="Path to the client dataset")
   parser.add_argument("financial_file", help="Path to the financial dataset")
   parser.add_argument("countries", help="Countries to filter", nargs="+")
   args = parser.parse_args()
   logging.info(f"Parsing arguments completed")
   
   try:
      # Process the data
      process_files_data(args.client_file, args.financial_file, args.countries)
      logging.info(f"Data processing completed successfully.")
   except Exception as e:
        logging.error(f"Error processing data: {str(e)}")

if __name__ == "__main__":
    main()


# python src/kommatipara_dataset.py "../data/dataset_one.csv" "../data/dataset_two.csv" "Netherlands" "United kingdom"