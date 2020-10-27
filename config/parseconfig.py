from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os

import pandas as pd

from cda.cdaflow.exceptions import MissingConfigException
from cda.cdaflow.utils.file_utils import read_yaml, is_directory

log = logging.getLogger(__name__)

"""
	Yml to config converter process
"""


def expand_env_var(env_var):
	"""
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
	if not env_var:
		return env_var
	while True:
		interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
		if interpolated == env_var:
			return interpolated
		else:
			env_var = interpolated


def _load_flow_config(config_dir, config_name):
	"""
    Read flow configs from YAML file

    :return: Python dictionary containing configs & their info
    """
	_file_config = read_yaml(config_dir, config_name)
	_file_path = os.path.join(config_dir, config_name)
	return _file_config, _file_path


class _CdaConfigParser(object):
	def __init__(self):
		self._flow_conn_config = FLOW_CONN_CONFIG
		self._flow_core_config = FLOW_CORE_CONFIG
		self._flow_daily_config = FLOW_DAILY_CONFIG
		self._flow_pit_config = FLOW_PIT_CONFIG
	
	def _validate(self):
		raise NotImplementedError
	
	def get_flow_conn_config(self):
		return self._flow_conn_config
	
	def get_flow_core_config(self):
		return self._flow_core_config
	
	def get_flow_daily_config(self):
		return self._flow_daily_config
	
	def get_flow_pit_config(self):
		return self._flow_pit_config



"""
	Core configuration settings
"""


def _get_config_root():
	"""
	 Return the root configuration folder location based on the AIRFLOW environment variable
	 it would be one level above the core config location
	"""
	_airflow_home = expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))
	# to move one level above to obtain the configuration root this is in our current setup
	# _config_root = get_parent_dir(_airflow_home)
	_config_root = os.path.join(_airflow_home, 'config')
	if not is_directory(_config_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_config_root))
	return _config_root


def _get_flow_root(config_root):
	"""
	 Return the flow configuration folder
	"""
	_flow_root = os.path.join(config_root, FLOW_NAME)
	if not is_directory(_flow_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_flow_root))
	return _flow_root


def _get_flow_conn_config(flow_root):
	"""
	 Return the connection configuration folder
	"""
	_flow_conn_root = os.path.join(flow_root, 'conn')
	if not is_directory(_flow_conn_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_flow_conn_root))
	return _flow_conn_root


def _get_flow_core_config(flow_root):
	"""
	 Return the core configuration folder
	"""
	_flow_core_root = os.path.join(flow_root, 'core')
	if not is_directory(_flow_core_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_flow_core_root))
	return _flow_core_root


def _get_flow_daily_config(flow_root):
	"""
	 Return the daily configuration folder
	"""
	_flow_daily_root = os.path.join(flow_root, 'daily')
	if not is_directory(_flow_daily_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_flow_daily_root))
	return _flow_daily_root


def _get_flow_pit_config(flow_root):
	"""
	 Return the point in time configuration folder
	"""
	_flow_pit_root = os.path.join(flow_root, 'pit')
	if not is_directory(_flow_pit_root):
		raise MissingConfigException('{0} is not a valid directory'.format(_flow_pit_root))
	return _flow_pit_root


# TODO need to convert this to a global parameter or pull this from the config
FLOW_NAME = 'omnicorenested'
CONFIG_ROOT = _get_config_root()
FLOW_CONFIG_ROOT = _get_flow_root(CONFIG_ROOT)
FLOW_CONN_ROOT = _get_flow_conn_config(FLOW_CONFIG_ROOT)
FLOW_CORE_ROOT = _get_flow_core_config(FLOW_CONFIG_ROOT)
FLOW_DAILY_ROOT = _get_flow_daily_config(FLOW_CONFIG_ROOT)
FLOW_PIT_ROOT = _get_flow_pit_config(FLOW_CONFIG_ROOT)

FLOW_CONN_CONFIG, FLOW_CONN_FILE_PATH = _load_flow_config(FLOW_CONN_ROOT, 'flow_connections.yml')
FLOW_CORE_CONFIG, FLOW_CORE_FILE_PATH = _load_flow_config(FLOW_CORE_ROOT, 'core.yml')
FLOW_DAILY_CONFIG, FLOW_DAILY_FILE_PATH = _load_flow_config(FLOW_DAILY_ROOT, 'daily.yml')
FLOW_PIT_CONFIG, FLOW_PIT_FILE_PATH = _load_flow_config(FLOW_PIT_ROOT, 'flow_pit.yml')