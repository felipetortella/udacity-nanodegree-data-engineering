from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 query,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clean FACT table" + self.table)
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info("Table" + self.table + "is clean...")
                      
        self.log.info("Populating FACT table {}... ".format(self.table))
        self.log.info("Executing QUERY ... " + self.query)
        redshift.run(self.query, False)
        self.log.info("Done, table {} is populated.".format(self.table))
        
