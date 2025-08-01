from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_user="",
                 aws_pw="",
                 table="",
                 s3_path="",
                 json = "",
                 region = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.json = json
        self.aws_user = aws_user
        self.aws_pw = aws_pw
        self.region = region

    def execute(self, context):        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table}")
        redshift.run("truncate {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        formatted_sql = f"""
            COPY {self.table}
            FROM '{self.s3_path}'
            ACCESS_KEY_ID '{self.aws_user}'
            SECRET_ACCESS_KEY '{self.aws_pw}'
            REGION '{self.region}'
            JSON '{self.json}'
        """
        redshift.run(formatted_sql)



