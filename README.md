# codc interview assignment

> Programming Exercise using PySpark

 A pyspark assignment which takes two dataset and list of countries as input and performs several data processing tasks, including:
  1. Filtering clients based on the specified list of countries and dropping personal identifiable information columns from the client dataset.
  2. Generic solution to removing credit card numbers from the financial dataset.
  3. Generic solution to Renaming columns in both datasets as per the assignment instructions.
  4. Joining the two datasets on id to generate the final client data.
  5. Unit testing using chispa packages
  6. Logging with rotating logger
  7. Basic CI/CD implementation using GITHUB Action


## Follow the below mentioned instruction to understand/run this code in local environment

> Steps
  
  1. Clone the repository to your local machine.
  2. Set up a Python virtual environment with Python 3.8.
  3. Install the required packages using the requirements.txt file.
     ```python
     pip install -r requirements.txt
 >    
  4. Ensure that you have a Spark environment configured and ready.
  5. To perform unit testing, run the kommatipara_dataset_test.py script, and make sure to provide three arguments: the paths of dataset 1, dataset 2, and the countries to filter. e.g
     ```python
     python src/kommatipara_dataset_test.py --client_file "data/dataset_one.csv" --financial_file "data/dataset_two.csv" "Netherland" "United kingdom"
> 
   6. To process the data, execute the kommatipara_dataset.py script, and provide the same three arguments: the paths of dataset 1, dataset 2, and the countries to filter.e.g
      ```python
      python src/kommatipara_dataset_test.py --client_file "data/dataset_one.csv" --financial_file "data/dataset_two.csv" "Netherland" "United kingdom"
> 
  7. The processed data will be saved in the client_data directory.
  8. Logs generated during data processing will be stored in the logs directory.A rotating logger has been put into operation, and a fresh file will be created when the current file 
     reaches a size limit of 1MB
  9. An automated build pipeline using GitHub Actions is in place. It will build the project and run tests whenever changes are pushed to the dev branch using pull request. The 
     workflow configurations can be found in the .github/workflows directory.
  10. The project can be packaged into a source distribution file. A Wheel package is also available in the dist directory. If you make changes to the code, you can create Wheel 
     packages for your changes by running the command
      ```python
      python setup.py bdist_wheel
>
  


## How to do CI/CD using GIThub action
   
> steps
 
 1. Retrieve the most recent updates from the GIT repository.
 2. Establish a new branch dedicated to your specific feature.
 3. Implement your modifications and enhancements.
 4. Record and upload your changes to the feature branch.
 5. Initiate a pull request to integrate your alterations into the dev branch.
 6. Automatically, upon any push to the dev branch, the kommatipara_dataset_CICD.yml will be triggered. It will facilitate continuous integration (CI) across multiple phases and subsequently deploy your adjustments to the main branch as part of continuous delivery (CD).
 7. For this particular task, the main branch serves as the ultimate result, and all the outcomes of your code will be accessible in their respective directories.

## Description of CICD Implementation(kommatipara_dataset_CICD.yml)
>
  1. This workflow is activated upon any push event occurring in the 'dev' branch
  2. It configures and installs all the required Python dependencies
  3. It conducts a thorough examination of Python code to identify syntax errors and undefined variables
  4. Following that, it performs unit testing
  5. It then executes the code
  6. The results are published as an artifact in the 'dev' branch, containing the newly generated code output
  7. Eventually, the code is deployed to the 'main' branch

