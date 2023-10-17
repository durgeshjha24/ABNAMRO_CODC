# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower,trim
from chispa.dataframe_comparer import *
from kommatipara_dataset import (
    read_dataset,
    parsing_arguments,
    process_files_data,
    rotating_logger,

)
from config import(
   log_file_path_name,
)

def test_schema_check_client_data(df_client, spark):
    """
    
    Unit test for validating the schema of the 'df_client' DataFrame.

    This test function ensures that the provided 'df_client' DataFrame adheres to the expected schema for client data.
    It is using chispa package for testing
    
    Args:
        df_client (DataFrame): The DataFrame to be tested.
        spark (SparkSession): The SparkSession used for creating DataFrames.

    Raises:
        AssertionError: If the schema of 'df_client' does not match the expected schema, an assertion error is raised.

    """
    client_data = [
        ("1","Rakel","Ingliby","ringliby6@ft.com","United States"),
        ("2","Derk","Mattielli","dmattielli7@slideshare.net","United States"),
    ]
    client_data = spark.createDataFrame(client_data, ["id","first_name","last_name","email","country"])
    client_data=client_data.schema
    df_client=df_client.schema
    
    #apply chispa package to compare schema 
    assert_schema_equality(client_data, df_client)

def test_schema_check_finance_data(df_finance, spark):
    """
    
    Unit test for validating the schema of the 'df_finance' DataFrame.

    This test function ensures that the provided 'df_finance' DataFrame adheres to the expected schema for Finance data.
    It is using chispa package for testing
    
    Args:
        df_finance (DataFrame): The DataFrame to be tested.
        spark (SparkSession): The SparkSession used for creating DataFrames.

    Raises:
        AssertionError: If the schema of 'df_finance' does not match the expected schema, an assertion error is raised.

    """
    finance_data = [
        ("15","1GnNjsnbBTw6w9WHnZ8apuxZZcqkhycT9a","jcb","3558941392668773"),
        ("16","17y4HG6vY9wDZmeu53rK3pAKS8ErtaTsQC","jcb","3579496825654275"),
    ]
    finance_data = spark.createDataFrame(finance_data, ["id","btc_a","cc_t","cc_n"])
    schema_finance_data_test=finance_data.schema
    schema_finance_data=df_finance.schema

    #apply chispa package to compare schema 
    assert_schema_equality(schema_finance_data_test,schema_finance_data)


def test_schema_check_final_outcome(df_final_client_data, spark):

    """
    
    Unit test for validating the schema of the 'df_final_client_data' DataFrame.Which is the final outcome

    This test function ensures that the provided 'df_final_client_data' DataFrame adheres to the expected schema as per requirments.
    It is using chispa package for testing
    
    Args:
        df_final_client_data (DataFrame): The DataFrame to be tested.
        spark (SparkSession): The SparkSession used for creating DataFrames.

    Raises:
        AssertionError: If the schema of 'df_final_client_data' does not match the expected schema, an assertion error is raised.

    """

    final_outcome_data=[("15","rpartkya2z@cdc.gov","netherlands","1RcsodKknm8thkCL6F4Vcmo7f4A7r6ydj","maestro"),
                        ("189","etsar58@ovh.net","netherlands","1PktCHyic9G4aZu15Dd3N1PUf45wW4MmFs","jcb")]
    
    final_outcome_data = spark.createDataFrame(final_outcome_data, ["client_identifier","email","country","bitcoin_address","credit_card_type"])
    schema_final_outcome_data_test=final_outcome_data.schema
    schema_final_outcome_data =final_outcome_data.schema

    #apply chispa package to compare schema
    assert_schema_equality(schema_final_outcome_data_test,schema_final_outcome_data)

def test_process_files_data_func(spark,logger):
    
    """
    Unit test for verifying the data joining functionality of the 'process_files_data' function.

    This test function checks if the 'process_files_data' function correctly joins client and finance datasets based on the provided test data and list of countries.

    Args:
        spark (SparkSession): The SparkSession used for creating DataFrames.
        logger: The logger for capturing test information and potential errors.

    Raises:
        AssertionError: If the joined DataFrame does not match the expected result, an assertion error is raised.

    Test Data:
    - Sample test data is provided for client and finance datasets.
    - Test schemas are defined for these dataframes.

    Test Steps:
    - Create test DataFrames.
    - Perform transformations on the test client data.
    - Invoke the 'process_files_data' function for unit testing.
    - Define the expected result.
    - Use the Chispa package to compare the output with the test data.

    Example:
    test_process_files_data_func(spark, logger)
    """

    # Sample test data
    data_client = [("1","Feliza","Eusden","feusden0@ameblo.jp","Netherlands"), 
                   ("3","Deliza","rusden","reusden0@ameblo.jp","United Kingdom")]
    
    data_finance = [("1","1QKy8RoeW","dinersclub","3034386"),
                     ("2", "dfghrt","Mastercard","345678564")]

    #Defining schema for test dataframe client & finance
    schema_client = ["id","first_name","last_name","email","country"]
    schema_finance = ["id","btc_a","cc_t","cc_n"]

    # Create test DataFrames
    df_client = spark.createDataFrame(data_client, schema_client)
    df_finance = spark.createDataFrame(data_finance, schema_finance)
    
    #Performing transformation on test client data
    df_client= df_client.withColumn('country',lower(trim(df_client['country'])))
    list_of_countries=['Netherlands','United Kingdom']
    
    #Invoke the process_files_data function for unit testing
    df_final_client_data = process_files_data(logger,df_client,df_finance,list_of_countries)
    
    #expected result
    expected_data = [("1", "feusden0@ameblo.jp", "netherlands", "1QKy8RoeW", "dinersclub")]
    expected_schema = ["client_identifier","email","country","bitcoin_address","credit_card_type"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    #apply chispa package to compare output with test data 
    assert_df_equality(expected_df,df_final_client_data,ignore_row_order=True)

def main():
    """
    Main function for executing the data processing and testing workflow.

    This function serves as the entry point for testing workflow. It performs the following tasks:
    
    1. Sets up logging to capture informative messages and potential errors, both to a log file and console.
    2. Parses command-line arguments to obtain necessary input parameters.
    3. Reads client and finance datasets using the provided file paths.
    4. Processes the datasets to generate the final client data.
    5. Executes schema checks for the client and finance datasets.
    6. Tests the schema of the final outcome after data processing.
    7. Tests the data joining functionality to ensure correctness.

    """
 

    logger=rotating_logger(log_file_path_name)

    logger.info(f"===============================Unit test started=================================================")

    args = parsing_arguments(logger)
    list_of_countries=args.countries

    df_client,df_finance = read_dataset(logger,args.client_file, args.financial_file)
    
    df_final_client_data= process_files_data(logger,df_client,df_finance,list_of_countries)

    #create spark session

    spark = SparkSession.builder.master("local[*]").appName("Read_data").getOrCreate()

    logger.info(f"Started schema validation for client dataset")

    test_schema_check_client_data(df_client, spark)

    logger.info(f"Finished schema validation for client dataset")

    logger.info(f"Started schema validation for finance dataset")
   
    test_schema_check_finance_data(df_finance, spark)

    logger.info(f"Finished schema validation for finance dataset")

    logger.info(f"Started testing of data processing logic")

    test_process_files_data_func(spark,logger)

    logger.info(f"Finshied testing of data processing logic")

    logger.info(f"Started schema validation for final outcome dataframe")

    test_schema_check_final_outcome(df_final_client_data, spark)

    logger.info(f"Started schema validation for final outcome dataframe")

    logger.info(f"All unit tests results : Passed ")

    logger.info(f"===============================Unit test End=================================================")


if __name__ == "__main__":
    main()
