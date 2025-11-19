from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from hooks.hdfs_hook import HdfsHook
import posixpath
import ast

class HdfsMkdirsFileOperator(BaseOperator):

    template_fields = ('parent_directory', 'folder_names', 'hdfs_conn_id')
    ui_color = '#fcdb03'

    @apply_defaults
    def __init__(
            self,
            parent_directory,
            folder_names,
            hdfs_conn_id,
            *args, **kwargs):
        """
        :param parent_directory: shared parent directory where folder_names will be created
        :type parent_directory: string
        :param folder_names: list of folder names to create under parent_directory
        :type folder_names: list[string]
        :param hdfs_conn_id: airflow connection id of HDFS connection to be used
        :type hdfs_conn_id: string
        """

        super(HdfsMkdirsFileOperator, self).__init__(*args, **kwargs)
        self.parent_directory = parent_directory
        self.folder_names = folder_names
        self.hdfs_conn_id = hdfs_conn_id

    def execute(self, context):

        self.log.info("HdfsMkdirsFileOperator execution started.")
        
        self.log.info("Mkdir HDFS directory'" + self.parent_directory + "'.")

        hh = HdfsHook(hdfs_conn_id=self.hdfs_conn_id)
        hh.mkdir(self.parent_directory)

        self.folder_names = ast.literal_eval(self.folder_names)
           
        self.log.info("Iterating over folder names to create directories.")

        if not isinstance(self.folder_names, list):
            self.log.error(f"'folder_names': {self.folder_names} is type {type(self.folder_names)}")
            raise AirflowException("'folder_names' must be a list of folder name strings.")

        for name in self.folder_names:
            if not isinstance(name, str) or not name:
                raise AirflowException("Each folder name must be a non-empty string.")
            path = posixpath.join(self.parent_directory, name)
            self.log.info("Mkdir HDFS directory '" + path + "'.")
            hh.mkdir(path)

        self.log.info("HdfsMkdirsFileOperator done.")
