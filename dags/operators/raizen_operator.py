from airflow.operators.python import PythonOperator

class RaizenOperator(PythonOperator):
    template_fields = ["start_time", "end_time"]
    
    def __init__(self, start_time, end_time, *args, **kwargs):
        super(RaizenOperator, self).__init__(*args, **kwargs)
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context):
        self.python_callable(*self.op_args, **self.op_kwargs)