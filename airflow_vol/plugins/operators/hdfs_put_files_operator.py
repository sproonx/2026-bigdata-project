from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.hdfs_hook import HdfsHook

import ast

class HdfsPutFilesOperator(BaseOperator):

    template_fields = ('local_remote_pairs', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            local_remote_pairs,
            hdfs_conn_id,
            *args, **kwargs):
        """
        :param local_remote_pairs: list of (local_file, remote_file) tuples to upload to HDFS
        :type local_remote_pairs: list of tuples
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsPutFilesOperator, self).__init__(*args, **kwargs)
        self.local_remote_pairs = local_remote_pairs
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsPutFilesOperator execution started.")
        
        self.local_remote_pairs = ast.literal_eval(self.local_remote_pairs)

        if not isinstance(self.local_remote_pairs, list) or not all(isinstance(p, tuple) and len(p) == 2 for p in self.local_remote_pairs):
            self.log.error(f"'local_remote_pairs': {self.local_remote_pairs} is type {type(self.local_remote_pairs)}")
            raise AirflowException("'local_remote_pairs' must be a list of (local_file, remote_file) tuples.")

        self.log.info(f"Uploading {len(self.local_remote_pairs)} files to HDFS.")
        self.log.info(f"local_remote_pairs: {self.local_remote_pairs}")
        for local_file, remote_file in self.local_remote_pairs:
            self.log.info("Upload file '" + local_file + "' to HDFS '" + remote_file + "'.")
            hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
            hh.putFile(local_file, remote_file)

        self.log.info("HdfsPutFilesOperator done.")
