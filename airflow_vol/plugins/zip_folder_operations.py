from airflow.plugins_manager import AirflowPlugin
from operators.zip_folder_operator import *

class ZipFolderPlugin(AirflowPlugin):
    name = "zip_folder_operations"
    operators = [UnzipFolderOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
