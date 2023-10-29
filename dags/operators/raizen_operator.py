from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import subprocess

class RaizenOperator(BaseOperator):
    template_fields = ["start_time", "end_time"]

    @apply_defaults
    def __init__(self, python_script, start_time, end_time, *args, **kwargs):
        super(RaizenOperator, self).__init__(*args, **kwargs)
        self.python_script = python_script
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context):
        try:
            self.log.info("Executando script Python: %s", self.python_script)
            result = subprocess.run(["python", self.python_script], capture_output=True, text=True)
            if result.returncode == 0:
                self.log.info("Script Python executado com sucesso")
            else:
                self.log.error("A execução do script Python falhou com erro: %s", result.stderr)
        except Exception as e:
            self.log.error("Ocorreu um erro ao executar o script Python: %s", str(e))
            raise e