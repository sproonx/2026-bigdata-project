from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

import os
import zipfile

class UnzipFolderOperator(BaseOperator):

    template_fields = ('zip_file', 'extract_to')
    ui_color = '#b05b27'

    @apply_defaults
    def __init__(
            self,
            zip_file,
            extract_to,
            *args, **kwargs):
        """
        :param zip_file: file to unzip (including path to file)
        :type zip_file: string
        :param extract_to: where to extract zip folder to
        :type extract_to: string
        """

        super(UnzipFolderOperator, self).__init__(*args, **kwargs)
        self.zip_file = zip_file
        self.extract_to = extract_to

    def execute(self, context):

        self.log.info("UnzipFolderOperator execution started.")

        self.log.info("Unzipping '" + self.zip_file + "' to '" + self.extract_to + "'.")

        with zipfile.ZipFile(self.zip_file, 'r') as zip_ref:
            zip_ref.extractall(self.extract_to)

        self.log.info("UnzipFolderOperator done.")
