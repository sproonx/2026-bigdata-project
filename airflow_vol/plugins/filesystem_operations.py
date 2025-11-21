from airflow.plugins_manager import AirflowPlugin
from operators.create_directory_operator import *
from operators.clear_directory_operator import *

class FileSystemPlugin(AirflowPlugin):
    name = "filesystem_operations"
    operators = [CreateDirectoryOperator, ClearDirectoryOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
