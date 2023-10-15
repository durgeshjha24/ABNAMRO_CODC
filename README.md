# How to run the application:

##### Clone the repository to your local machine.
##### Set up a Python environment with Python 3.8.
##### Install the required packages using the requirements.txt file.
##### Ensure you have a Spark environment set up.
##### Run the kommatipara_dataset_test.py  script for unit testing.Please pass three arguments(path of dataset1, path of dataset2 and countries need to filter)
##### Run the kommatipara_dataset.py  script to process the data.Please pass three arguments(path of dataset1, path of dataset2 and countries need to filter)
##### Output of processed data is  stored at client_data directory
##### Logs of data processing are stored at logs directory
##### A basic automated build pipeline using GitHub Actions is set up to build the project and run tests whenever changes are pushed to the repository.workflows are present in .github/workflows.
###### Project can be packaged into a source distribution file. wheel package is also present in dist directory. If you want to make any changes in code then you can also create the wheel packages on your changes by running this command(python setup.py bdist_wheel)

