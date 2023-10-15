# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lower,trim
from chispa.dataframe_comparer import *
from chispa.column_comparer import assert_column_equality
from kommatipara_dataset import (
    read_dataset,
    parsing_arguments,
    process_files_data,

)

def test_schema_check_client_data(df_client, spark):
    """
    
    Unit test for validating the schema of the 'df_client' DataFrame.

    This test function ensures that the provided 'df_client' DataFrame adheres to the expected schema for client data.
    It is using chispa package for testing
    
    Args:
        df_client (DataFrame): The DataFrame to be tested.

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

def test_process_files_data_func(list_of_countries, spark):
    """
    Unit test for verifying the data joining functionality of the 'process_files_data' function.

    This test function checks if the 'process_files_data' function correctly joins client and finance datasets based on the provided test data and list of countries.

    Args:
        list_of_countries (list): List of countries to filter client data during the join.

    Raises:
        AssertionError: If the joined DataFrame does not match the expected result, an assertion error is raised.
    
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
    
    #Invoke the process_files_data function for unit testing
    df_final_client_data = process_files_data(df_client,df_finance,list_of_countries)
    
    #expected result
    expected_data = [("1", "feusden0@ameblo.jp", "netherlands", "1QKy8RoeW", "dinersclub")]
    expected_schema = ["client_identifier","email","country","bitcoin_address","credit_card_type"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    #apply chispa package to compare output with test data 
    assert_df_equality(expected_df,df_final_client_data,ignore_row_order=True)

def main():
    """
    Main function for executing the data processing and testing workflow.

    This function serves as the entry point for the data processing and testing workflow. It performs the following tasks:
    1. Parses command-line arguments to obtain necessary input parameters.
    2. Reads client and finance datasets using the provided file paths.
    3. Processes the datasets to generate the final client data.
    4. Executes schema checks for the client and finance datasets.
    5. Tests the schema of the final outcome after data processing.
    6. Tests the data joining functionality to ensure correctness.

    """

    args = parsing_arguments()
    list_of_countries=args.countries

    df_client,df_finance = read_dataset(args.client_file, args.financial_file)
    
    df_final_client_data= process_files_data(df_client,df_finance,list_of_countries)

    spark = SparkSession.builder.master("local[*]").appName("Read_data").getOrCreate()

    test_schema_check_client_data(df_client, spark)

    test_schema_check_finance_data(df_finance, spark)

    test_schema_check_final_outcome(df_final_client_data, spark)

    test_process_files_data_func(list_of_countries, spark)

if __name__ == "__main__":
    main()