from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults

class RaizenOperator(PythonOperator):
    template_fields = ["start_time", "end_time"]

    @apply_defaults
    def __init__(self, python_script, start_time, end_time, *args, **kwargs):
        super(RaizenOperator, self).__init__(*args, **kwargs)
        self.python_script = python_script
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context):
        self.python_callable(*self.op_args, **self.op_kwargs)