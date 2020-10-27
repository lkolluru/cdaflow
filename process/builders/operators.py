import re

from cda.cdaflow.exceptions import InvalidCdaOperatorException

"""
Module required to get the operator data required to build dynamic airflow dags.
"""

HIVE_PREFIX = "hive"
SHELL_PREFIX = "shell"
BIGQUERY_PREFIX = "bigquery"
PROCEDURE_PREFIX = "bigqueryprocedure"
SPARK_PREFIX = "spark"
BEAM_PREFIX = "beam"

"""
Module required to name of tasks getting passed to the .
"""

HIVE_REGEX = re.compile("^%s" % re.escape(HIVE_PREFIX))
SHELL_REGEX = re.compile("^%s" % re.escape(SHELL_PREFIX))
BEAM_REGEX = re.compile("^%s" % re.escape(BEAM_PREFIX))
BIGQUERY_REGEX = re.compile("^%s" % re.escape(BIGQUERY_PREFIX))
PROCEDURE_REGEX = re.compile("^%s" % re.escape(PROCEDURE_PREFIX))
SPARK_REGEX = re.compile("^%s" % re.escape(SPARK_PREFIX))


def _get_hive_operator(_info):
	from cda_plugins.cda_metadata_plugin.operators.cda_hive_operator import CdaHiveOperator
	"""
		Use the pyhive operator to execute the queries getting populated
	"""
	name = _info.get_name
	query = _info.get_query
	conf = _info.get_query_parameters
	
	return CdaHiveOperator(task_id=name,
	                       hql=query,
	                       hive_conf=conf
	                       )


def _get_shell_operator(_info):
	raise NotImplementedError


def _get_bigquery_operator(_info):
	raise NotImplementedError


def _get_procedure_operator(_info):
	raise NotImplementedError


def _get_spark_operator(_info):
	raise NotImplementedError


def _get_beam_operator(_info):
	raise NotImplementedError


def fetch_flow_operator(rw_step_info):
	info = StepInfo(rw_step_info)
	
	if HIVE_REGEX.match(info.get_query_type):
		_get_hive_operator(info)
	elif SHELL_REGEX.match(info.get_query_type):
		_get_hive_operator(rw_step_info)
	elif BEAM_REGEX.match(info.get_query_type):
		_get_hive_operator(rw_step_info)
	elif BIGQUERY_REGEX.match(info.get_query_type):
		_get_hive_operator(rw_step_info)
	elif PROCEDURE_REGEX.match(info.get_query_type):
		_get_hive_operator(rw_step_info)
	elif SPARK_REGEX.match(info.get_query_type):
		_get_hive_operator(rw_step_info)
	else:
		raise InvalidCdaOperatorException(
			"`Step operator must be a HIVE_PREFIX (%s), SHELL_PREFIX (%s), BIGQUERY_PREFIX (%s),PROCEDURE_PREFIX (%s),"
			"SPARK_PREFIX (%s),BEAM_PREFIX (%s) "
			"Location, got %s"
			% (HIVE_PREFIX, SHELL_PREFIX, BIGQUERY_PREFIX, PROCEDURE_PREFIX, SPARK_REGEX, BEAM_PREFIX, rw_step_info)
		)


class StepInfo:
	
	"""
	    prepares an object style implementation from the information obtained from the database.
	    #TODO add validations to raise appropriate exceptions for incorrect configurations in the database.
	"""
	
	def __init__(self,
	             step_info=None):
		self._step_info = step_info
	
	@property
	def get_name(self):
		return self._step_info["variableworkflowstepname"]
	
	@property
	def get_query(self):
		return self._step_info["variableworkflowstepquerytype"]
	
	@property
	def get_query_type(self):
		return self._step_info["variableworkflowstepquerytype"]
	
	@property
	def get_query_parameters(self):
		return self._step_info["workflowstepqueryparameters"]
