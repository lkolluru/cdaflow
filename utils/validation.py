"""
Utilities for validating user inputs such as
	* airflow task id names
	* job id parameter names
	* application variable names
"""
import re

from airflow.exceptions import AirflowException

# Regex for valid task names: must be an alphanumeric string of length 1 to 256.
# TODO expand regex to exclude names with ":" in string to prevent the Airflow exception.

_CDA_FLOW_ID_REGEX = re.compile(r"^[a-zA-Z0-9][\w\-]{0,255}$")

_MAPR_HIVE_JOB_ID_REGEX = re.compile(r"^[a-zA-Z0-9][\w\-]{0,63}$")

_CLOUD_STORAGE_JOB_ID_REGEX = re.compile(r"^[a-zA-Z0-9][\w\-]{0,255}$")

_BIGQUERY_JOB_ID_REGEX = re.compile(r"^[a-zA-Z0-9][\w\-]{0,255}$")

_BAD_CHARACTERS_MESSAGE = (
	"Names may only contain alphanumerics, underscores (_), dashes (-), periods (.),"
	" spaces ( ), and slashes (/)."
)


def _validate_length_limit(name, limit, value):
	if len(value) > limit:
		raise AirflowException(
			"%s '%s' had length %s, which exceeded length limit of %s"
			% (name, value[:250], len(value), limit)
		)


def validate_flow_id_name(name):
	if not _CDA_FLOW_ID_REGEX.match(name):
		raise AirflowException("%s task name is not a acceptable convention" % name)


def validate_mapr_job_id_name(name):
	if not _MAPR_HIVE_JOB_ID_REGEX.match(name):
		raise AirflowException("%s hive job name is not a acceptable convention" % name)


def validate_gcs_job_id_name(name):
	if not _CLOUD_STORAGE_JOB_ID_REGEX.match(name):
		raise AirflowException("%s gcs job name is not a acceptable convention" % name)
	

def validate_gcp_job_id_name(name):
	if not _BIGQUERY_JOB_ID_REGEX.match(name):
		raise AirflowException("%s gcp job name is not a acceptable convention" % name)
