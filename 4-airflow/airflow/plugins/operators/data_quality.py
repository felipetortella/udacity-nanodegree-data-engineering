from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tables=tuple(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables 
        self.redshift_conn_id = redshift_conn_id        

    def execute(self, context):
        self.log.info('Start of DataQualityOperator')    
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info('Checking Data quality for {0}'.format(table))

            records = redshift.get_records("SELECT COUNT(*) FROM {0}".format(table))
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality has failed. {0} is empty".format(table))
                
        self.log.info('End of DataQualityOperator')            