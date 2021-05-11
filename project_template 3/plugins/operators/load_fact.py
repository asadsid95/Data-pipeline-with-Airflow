from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

'''
This class creates a custom operator, to be used for creating fact table from Sparkify's staged data.
It's parent class is 'BaseOperator' whose methods are leveraged by using super(). Note that this syntax for super() is of Python 2.

This class has a constructor and an 'execute' methods (required by Airflow): 
'''

class LoadFactOperator(BaseOperator):

    # Class attributes
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials='',
                 redshift_conn_id='',
                 table='',
                 sql_statement='',
                 *args, **kwargs):

        # Constructor defines instance attributes (these will be specified in the DAG file) as well as leverages the parent class's methods

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials=aws_credentials
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_statement=sql_statement

    def execute(self, context):
        
        '''
        This function is executed when Airflow's runner calls the operator. It has the following roles:
            - Creates neccessary hook to interact with external systems.
            - Conditionally retrieve Sparkify's data from staged tables, create and populate fact table with it.
        '''
        
        self.log.info("Creating Redshift Hook by getting parameters to interact with Redshift.")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Hook is now created.")
        
        self.log.info(f"Running SQl query to load fact table '{self.table}' with data from staging tables")
        redshift.run(self.sql_statement)
              