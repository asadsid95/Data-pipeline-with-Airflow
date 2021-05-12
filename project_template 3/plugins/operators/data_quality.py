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
                 quality_checks=[],
                 *args, **kwargs):

        # Constructor defines instance attributes (these will be specified in the DAG file) as well as leverages the parent class's methods        
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_names=table_names
        self.quality_checks=quality_checks
             

    def execute(self, context):

        '''
        This function is executed when Airflow's runner calls the operator. It has the following roles:
            - Creates neccessary hook to interact with external systems.
            - Checks whether fact and dimensions tables were populated with data.
            
            The way this quality check works is by using the SQL statements for checking if specific attribute of each table has 'null' value. The expected value is 0.
            After receiving the number of records that containing 'null'value, count of error is increased by 1 if number of records containig 'null' value is not 0. 
            The SQL statement is also pushed into a list. Lastly, if error count > 0, quality check will fail and ValueError will be raised. 
        '''
        
        self.log.info("Creating Redshift Hook by getting parameters to interact with Redshift.")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift Hook is now created.")
        
        error_count = 0
        failing_tests = []
        
        self.log.info("Data quality checks will now begin.")        
        for check in self.quality_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')
           
            records = redshift.get_records(sql)
            self.log.info(records[0][0])
            
            if expected_result != records[0][0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')

        self.log.info('Data Quality checks passed!')
            
        # Old version of quality check
        '''
        for table in self.table_names:
            self.log.info(f"To ensure data quality for Sparkify, check to see Table {table} is populated.")
            records = redshift.get_records(f"SELECT count(*) FROM {table}")
            total_records = records[0][0]
            
            if total_records == 0:
                raise ValueError (f"Data quality check failed!")
            else:
                self.log.info(f"Data quality check passed!\nThere are {total_records} in {table}.")
           
        '''
        
        