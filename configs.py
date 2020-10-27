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
	Workflow Core configurations including env, group,dynamic conn
"""


class CoreConfig(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def env(self):
		return self._flow_core_config['environment']
	
	@property
	def functional_group(self):
		return self._flow_core_config['functionalgroup']
	
	@property
	def metadata_conn(self):
		return self._flow_core_config['repositoryconnectionid']
	
	@property
	def project_name(self):
		return self._flow_core_config['projectname']
	
	@property
	def workflow_root_dir(self):
		return self._flow_core_config['paths']['workflow_root_dir']
	
	@property
	def workflow_config_root_dir(self):
		return self._flow_core_config['paths']['workflow_config_root_dir']
	
	@property
	def workflow_resources_root_dir(self):
		return self._flow_core_config['paths']['workflow_resources_root_dir']


class PitConfig(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def pit_obj_list(self):
		_pit_obj_list = []
		for key, value in enumerate(self._flow_pit_config['bigdatasnapshots']):
			_pit_obj_list.append(value)
		return _pit_obj_list
	
	@property
	def pit_snapshot_list(self):
		_pit_obj_list = []
		for key, value in enumerate(self._flow_pit_config['bigdatasnapshots']):
			_pit_obj_list.append(value)
		return _pit_obj_list


"""
	Point in Time Configuration Objects
"""

PITDETAILS = pd.DataFrame(columns=['bd_tablename', 'bd_snapshotdate',
                                   'bd_snapshotlocation', 'cda_tablename',
                                   'flow_script_location', 'bd_snapshotid'])

PITPLACEHOLDER = pd.DataFrame(columns=['bd_tablename', 'bd_snapshotid',
                                       'bd_snapshotlocation', 'bd_datalocation',
                                       'bd_volume', 'cda_tablename', 'flow_script_location'])

PITADDPARTITIONSCRIPT = "/gen_sql/omnicorenested/spartitions/add_snapshot_partitions.sql"

"""
Customer Snapshot Objects
"""


class Tdmcust(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcust']['ddlscriptlocation']


class Tdmordq(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordq']['ddlscriptlocation']


class Tdmordr(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmordr']['ddlscriptlocation']


class Tdmorit(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorit']['ddlscriptlocation']


class Tdmcusu(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcusu']['ddlscriptlocation']


class Tdmefcu(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmefcu']['ddlscriptlocation']


class Tdmnono(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmnono']['ddlscriptlocation']


class Tdmorak(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmorak']['ddlscriptlocation']


class Tdmcudq(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmcudq']['ddlscriptlocation']


class Tdmtran(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tdmtran']['ddlscriptlocation']


"""
Prospect Snapshot Objects
"""


class Tnbefhh(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbefhh']['ddlscriptlocation']


class Tnbxref(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['tnbxref']['ddlscriptlocation']


"""
Online Snapshot Objects
"""


class Gmtattributes(PitConfig):
	@property
	def bd_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['bigdatatablename']
	
	@property
	def bd_snapshotlocation(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['snapshotlocation']
	
	@property
	def bd_datalocation(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['datalocation']
	
	@property
	def bd_volume(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['bigdatavolumename']
	
	@property
	def cda_tablename(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['cdatablename']
	
	@property
	def flow_script_location(self):
		return self._flow_pit_config['bigdatasnapshots']['gmtattributes']['ddlscriptlocation']


"""
	Connection Configuration Objects
"""


class ConnConfig(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def flow_conn_list(self):
		_conn_list = []
		for key, value in enumerate(self._flow_conn_config['connections']):
			_conn_list.append(value)
		return _conn_list


class OcnGcp(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def conn_id(self):
		return self._flow_conn_config['connections']['ocn_gcp']['setup']['conn_id']
	
	@property
	def setup(self):
		return self._flow_conn_config['connections']['ocn_gcp']['setup']
	
	@property
	def test(self):
		return self._flow_conn_config['connections']['ocn_gcp']['test']


class OcnMssql(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def conn_id(self):
		return self._flow_conn_config['connections']['ocn_mssql']['setup']['conn_id']
	
	@property
	def setup(self):
		return self._flow_conn_config['connections']['ocn_mssql']['setup']
	
	@property
	def test(self):
		return self._flow_conn_config['connections']['ocn_mssql']['test']


class OcnSshEdge2(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def conn_id(self):
		return self._flow_conn_config['connections']['ocn_ssh_edge2']['setup']['conn_id']
	
	@property
	def setup(self):
		return self._flow_conn_config['connections']['ocn_ssh_edge2']['setup']
	
	@property
	def test(self):
		return self._flow_conn_config['connections']['ocn_ssh_edge2']['test']


class OcnHiveEdge2(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def conn_id(self):
		return self._flow_conn_config['connections']['ocn_hive_edge2']['setup']['conn_id']
	
	@property
	def setup(self):
		return self._flow_conn_config['connections']['ocn_hive_edge2']['setup']
	
	@property
	def test(self):
		return self._flow_conn_config['connections']['ocn_hive_edge2']['test']


class OcnPyHiveEdge2(_CdaConfigParser):
	def _validate(self):
		pass
	
	@property
	def conn_id(self):
		return self._flow_conn_config['connections']['ocn_pyhive_edge2']['setup']['conn_id']
	
	@property
	def setup(self):
		return self._flow_conn_config['connections']['ocn_pyhive_edge2']['setup']
	
	@property
	def test(self):
		return self._flow_conn_config['connections']['ocn_pyhive_edge2']['test']


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

log.info("Reading the config from %s", FLOW_CORE_CONFIG)
