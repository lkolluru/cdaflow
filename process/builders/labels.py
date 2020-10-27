from airflow.operators.dummy_operator import DummyOperator

from cda.cdaflow.utils.validation import validate_flow_id_name


class CdaLabelBuilder:
	"""
	    Prepares the dummy operators required to orchestrate the execution.
	    Each operation requires a start and end labels with flow based values.
	    As all the values being passed have
	    #TODO create a process to accept a list of phases used for orchestration and pre create the labels
	"""
	template_fields = 'name'
	
	def __init__(self,
	             name=None):
		self._name = name
		validate_flow_id_name(str(self._name))
	
	def get_workflowlabel_start(self):
		_task_id = 'start_' + str(self._name).lower()
		return DummyOperator(
			task_id=_task_id
		)
	
	def get_workflowlabel_end(self):
		_task_id = 'end_' + str(self._name).lower()
		return DummyOperator(
			task_id=_task_id
		)
