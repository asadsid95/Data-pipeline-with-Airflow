from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

'''
This class creates a custom operator, to be used for staging Sparkify's data (located in AWS S3) in Data Warehouse on AWS ReshifT.
It's parent class is 'BaseOperator' whose methods are leveraged by using super(). Note that this syntax for super() is of Python 2.

This class has a constructor and an 'execute' methods (required by Airflow): 
'''

class StageToRedshiftOperator(BaseOperator):

    # Class attributes
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):

        # Constructor defines instance attributes (these will be specified in the DAG file) as well as leverages the parent class's methods
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key

    def execute(self, context):
        
        '''
        This function is executed when Airflow's runner calls the operator. It has the following roles:
            - Creates neccessary hooks to interact with external systems.
            - Conditionally retrieve data from specific S3 buckets and stage them into Data Warehouse
        '''
        self.log.info("Creating AWS and Redshift Hooks by getting AWS Access credentials to connect to S3, and parameters to interact with Redshift.")
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("AWS & Redshift Hook is now created.")        
        
        if self.table=="staging_songs":
            self.log.info(f"Clearing data from {self.table} table")
            #redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift into {}".format(self.table))
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                'auto'
            )
            redshift.run(formatted_sql)
                    
        else:
            self.log.info(f"Clearing data from {self.table} table")
            #redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift into {}".format(self.table))
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                's3://udacity-dend/log_json_path.json'
            )
            redshift.run(formatted_sql)   