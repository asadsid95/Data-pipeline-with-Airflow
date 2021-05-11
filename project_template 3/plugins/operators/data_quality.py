from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

'''
This class creates a custom operator, to be used for ensuring data quality in fact and dimensions tables.
It's parent class is 'BaseOperator' whose methods are leveraged by using super(). Note that this syntax for super() is of Python 2.

This class has a constructor and an 'execute' methods (required by Airflow): 
'''

class DataQualityOperator(BaseOperator):
    
    # Class attributes
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table_names='',
                 *args, **kwargs):

        # Constructor defines instance attributes (these will be specified in the DAG file) as well as leverages the parent class's methods        

        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_names=table_names

    def execute(self, context):

        '''
        This function is executed when Airflow's runner calls the operator. It has the following roles:
            - Creates neccessary hook to interact with external systems.
            - Checks whether fact and dimensions tables were populated with data.
        '''
        
        self.log.info("Creating Redshift Hook by getting parameters to interact with Redshift.")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Hook is now created.")

        for table in self.table_names:
            self.log.info(f"To ensure data quality for Sparkify, check to see Table {table} is populated.")
            records = redshift.get_records(f"SELECT count(*) FROM {table}")
            total_records = records[0][0]
            
            if total_records == 0:
                raise ValueError (f"Data quality check failed!")
            else:
                self.log.info(f"Data quality check passed!\nThere are {total_records} in {table}.")
           
        
        
        