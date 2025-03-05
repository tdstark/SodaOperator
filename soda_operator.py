import logging

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from soda.scan import Scan

# The link to the soda check language (SodaCL) can be found here:
# https://docs.soda.io/soda-cl/soda-cl-overview.html

# Programmatic scan (Python SDK) documentation can be found here:
# https://docs.soda.io/soda-core/programmatic.html

logger = logging.getLogger(__name__)


class SodaSqlOperator(BaseOperator):
    """
    Executes a Soda SQL scan using the Soda Python SDK.

    :param airflow_conn_id: The Airflow connection ID to use for the scan.
    :type airflow_conn_id: str
    :param soda_cl_path: The path to the SodaCL file to use for the scan.
    :type soda_cl_path: str
    :param soda_vars: A dictionary of SodaCL variables to use for the scan.
    :type soda_vars: dict
    :param test_mode: Bool to indicate if test should be run in test mode. This will
                      currently only send a Slack message with the scan results.
                      The Airflow jobs will always succeed while in test mode.
    :type test_mode: bool
    :param *args: Variable length argument list.
    :param **kwargs: Arbitrary keyword arguments.
    """

    template_fields = ("soda_vars",)

    def __init__(
        self,
        airflow_conn_id: str,
        soda_cl_path: str,
        soda_vars: dict = {},
        test_mode: bool = False,
        *args,
        **kwargs,
    ):
        self.scan = Scan()
        self.soda_cl_path = soda_cl_path
        self.airflow_conn_id = airflow_conn_id
        self.soda_vars = soda_vars
        self.test_mode = test_mode

        self.scan.disable_telemetry()
        super().__init__(*args, **kwargs)

    def _fetch_postgres_yaml_config(self, redshift_conn):
        self.scan.add_configuration_yaml_str(
            f"""
            data_source {self.airflow_conn_id}:
                type: redshift
                connection:
                    host: {redshift_conn.host}
                    username: {redshift_conn.login}
                    password: {redshift_conn.password}
                    database: {redshift_conn.schema}
                    access_key_id: {redshift_conn.extra_dejson.get('aws_access_key_id')}
                    secret_access_key: {redshift_conn.extra_dejson.get('aws_secret_access_key')}
                    role_arn: {redshift_conn.extra_dejson.get('role_arn', None)}
                    region: us-east-1
                    schema:
        """
        )

    def _fetch_oracle_yaml_config(self, oracle_conn):
        self.scan.add_configuration_yaml_str(
            f"""
            data_source {self.airflow_conn_id}:
                type: oracle
                username: {oracle_conn.login}
                password: {oracle_conn.password}
                connectstring: {oracle_conn.host}:{oracle_conn.port}/{oracle_conn.schema}
        """
        )

    def _fetch_mysql_yaml_config(self, mysql_conn):
        self.scan.add_configuration_yaml_str(
            f"""
            data_source {self.airflow_conn_id}:
                type: mysql
                host: {mysql_conn.host}
                username: {mysql_conn.login}
                password: {mysql_conn.password}
                database: {mysql_conn.schema}
        """
        )

    def execute(self, context):
        db_conn = BaseHook.get_connection(self.airflow_conn_id)
        getattr(self, f"_fetch_{db_conn.conn_type}_yaml_config")(db_conn)
        self.scan.set_data_source_name(self.airflow_conn_id)
        self.scan.add_sodacl_yaml_files(self.soda_cl_path)
        self.scan.add_variables(self.soda_vars)
        self.scan.execute()
        self.scan.assert_no_error_logs()
        results = self.scan.get_all_checks_text()
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        try:
            self.scan.assert_no_checks_fail()
        except AssertionError as error:
            if self.test_mode:
                msg = f"SODA TESTING DISREGARD: {dag_id}.{task_id}:\n\n{results}"
                logger.error(msg)
                # Add callback here
            else:
                raise error

        if self.scan.has_check_warns():
            msg = f":warning: SODA WARNING :warning:: {dag_id}.{task_id}:\n\n{results}"
            logger.warning(msg)
            # Add callback here
