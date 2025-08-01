from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id = "",
                table = "",
                select_statement = "",
                append_only = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table 
        self.select_statement = select_statement
        self.appennd_only  = append_only

    def execute(self, context):
        self.log.info(f'LoadFactOperator loading to {self.table}. Append-only: {self.appennd_only}')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.appennd_only is False:
            self.log.info(f"Clearing data from {self.table}")
            redshift.run(f"truncate {self.table};")

        self.log.info(f"Inserting data to {self.table}")
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.select_statement}
        """
        redshift.run(insert_sql)

