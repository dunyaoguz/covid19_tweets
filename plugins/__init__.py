from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

class CustomPlugins(AirflowPlugin):
    name = "custom_plugins"

    operators = [
        operators.S3ToRedshiftOperator,
        operators.HasRowsOperator,
        operators.ContextCheckOperator
    ]
