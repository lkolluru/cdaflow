import re

import pandasql as pdsql
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults
from airflow_fs.hooks import SftpHook

from cda.cdaflow.configs import Tdmcust, Tdmordq, Tdmordr, Tdmorit, Tdmcusu, Tdmefcu, Tdmtran, Tdmnono, Tdmorak, \
	Tdmcudq, Tnbxref, Tnbefhh, PITDETAILS, PITPLACEHOLDER, PITADDPARTITIONSCRIPT, Gmtattributes
from cda.cdaflow.exceptions import MissingPitDataException, InvalidPitObjectException, MissingSnapshotDataException
from cda_plugins.cda_metadata_plugin.operators.cda_hive_operator import CdaHiveOperator
from cda.cdaflow.fsdata import fetch_dataframe

"""
Isolate the configured objects for pits
"""

TDMCUST_PREFIX = "tdmcust"
TDMORDQ_PREFIX = "tdmordq"
TDMORDR_PREFIX = "tdmordr"
TDMORIT_PREFIX = "tdmorit"
TDMCUSU_PREFIX = "tdmcusu"
TDMEFCU_PREFIX = "tdmefcu"
TDMTRAN_PREFIX = "tdmtran"
TDMNONO_PREFIX = "tdmnono"
TDMORAK_PREFIX = "tdmorak"
TDMCUDQ_PREFIX = "tdmcudq"
TNBXREF_PREFIX = "tnbxref"
TNBEFHH_PREFIX = "tnbefhh"
GMTATRB_PREFIX = "gmtattributes"

"""
Reg-ex for switch operations
"""

TDMCUST_REGEX = re.compile("^%s" % re.escape(TDMCUST_PREFIX))
TDMORDQ_REGEX = re.compile("^%s" % re.escape(TDMORDQ_PREFIX))
TDMORDR_REGEX = re.compile("^%s" % re.escape(TDMORDR_PREFIX))
TDMORIT_REGEX = re.compile("^%s" % re.escape(TDMORIT_PREFIX))
TDMCUSU_REGEX = re.compile("^%s" % re.escape(TDMCUSU_PREFIX))
TDMEFCU_REGEX = re.compile("^%s" % re.escape(TDMEFCU_PREFIX))
TDMTRAN_REGEX = re.compile("^%s" % re.escape(TDMTRAN_PREFIX))
TDMNONO_REGEX = re.compile("^%s" % re.escape(TDMNONO_PREFIX))
TDMORAK_REGEX = re.compile("^%s" % re.escape(TDMORAK_PREFIX))
TDMCUDQ_REGEX = re.compile("^%s" % re.escape(TDMCUDQ_PREFIX))
TNBXREF_REGEX = re.compile("^%s" % re.escape(TNBXREF_PREFIX))
TNBEFHH_REGEX = re.compile("^%s" % re.escape(TNBEFHH_PREFIX))
GMTATRB_REGEX = re.compile("^%s" % re.escape(GMTATRB_PREFIX))


class CdaPitTableInfo:
	"""
	Module for parsing out the information related to a pit configuration
	"""
	
	@apply_defaults
	def __init__(self, pit_tablename=None, pit_fs_conn_id='dev_ocn_ssh_edge2', *args, **kwargs):
		self._pit_tablename = pit_tablename
		self._pit_fs_conn_id = pit_fs_conn_id
		self.tdmordr = Tdmordr()
		self.tdmnono = Tdmnono()
		self.tdmtran = Tdmtran()
		self.tdmcudq = Tdmcudq()
		self.tdmcusu = Tdmcusu()
		self.tdmcust = Tdmcust()
		self.tdmordq = Tdmordq()
		self.tdmefcu = Tdmefcu()
		self.tdmorit = Tdmorit()
		self.tdmorak = Tdmorak()
		self.tnbxref = Tnbxref()
		self.tnbefhh = Tnbefhh()
		self.gmtatrb = Gmtattributes()
		self._pit_info = self._fetch_pit_table_info()
		self._df_pit_details = PITDETAILS
		self._df_pit_placeholder = PITPLACEHOLDER
		self._load_pit_snapshot_details()
	
	def _fetch_pit_table_info(self):
		_pit_tablename = str(self._pit_tablename)
		if TDMORDR_REGEX.match(_pit_tablename):
			return self.tdmordr
		elif TDMNONO_REGEX.match(_pit_tablename):
			return self.tdmnono
		elif TDMTRAN_REGEX.match(_pit_tablename):
			return self.tdmtran
		elif TDMCUDQ_REGEX.match(_pit_tablename):
			return self.tdmcudq
		elif TDMCUSU_REGEX.match(_pit_tablename):
			return self.tdmcusu
		elif TDMCUST_REGEX.match(_pit_tablename):
			return self.tdmcust
		elif TDMORDQ_REGEX.match(_pit_tablename):
			return self.tdmordq
		elif TDMEFCU_REGEX.match(_pit_tablename):
			return self.tdmefcu
		elif TDMORIT_REGEX.match(_pit_tablename):
			return self.tdmorit
		elif TDMORAK_REGEX.match(_pit_tablename):
			return self.tdmorak
		elif TNBXREF_REGEX.match(_pit_tablename):
			return self.tnbxref
		elif TNBEFHH_REGEX.match(_pit_tablename):
			return self.tnbefhh
		elif GMTATRB_REGEX.match(_pit_tablename):
			return self.gmtatrb
		else:
			raise InvalidPitObjectException(
				"`Pit Configured tables are TDMORDR_PREFIX (%s), TDMNONO_PREFIX (%s), TDMTRAN_PREFIX (%s), "
				"TDMCUDQ_PREFIX (%s), TDMCUSU_PREFIX (%s), TDMCUST_PREFIX (%s), TDMORDQ_PREFIX (%s), TDMEFCU_PREFIX ("
				"%s), TDMORIT_PREFIX (%s), TDMORAK_PREFIX (%s), TNBXREF_PREFIX (%s), TNBEFHH_PREFIX (%s),"
				"GMTATRB_PREFIX (%s)"
				"but, got %s"
				% (TDMORDR_PREFIX, TDMNONO_PREFIX, TDMTRAN_PREFIX, TDMCUDQ_PREFIX, TDMCUSU_PREFIX, TDMCUST_PREFIX,
				   TDMORDQ_PREFIX, TDMEFCU_PREFIX, TDMORIT_PREFIX, TDMORAK_PREFIX, TNBXREF_PREFIX, TNBEFHH_PREFIX,
				   GMTATRB_PREFIX,
				   self._pit_tablename))
	
	def _load_pit_snapshot_placeholder(self):
		"""
			Use the Bigdata Volume snapshot location to obtain the existing snapshotid information
			Note: this is necessary to accommodate the regular cleanup being implemented
			_snapshot_list = DataFrame(_sftp_hook.listdir(_snapshotlocation), columns=['snapshotid'])
		"""
		# TODO move the snapshot data pull to common fs-data post local uri conversion implementation
		_sftp_hook = SftpHook(conn_id=self._pit_fs_conn_id)
		_snapshotlocation = self._pit_info.bd_snapshotlocation
		self._df_pit_placeholder['bd_snapshotid'] = _sftp_hook.listdir(_snapshotlocation)
		self._df_pit_placeholder['bd_tablename'] = self._pit_info.bd_tablename
		self._df_pit_placeholder['bd_snapshotlocation'] = self._pit_info.bd_snapshotlocation
		self._df_pit_placeholder['bd_datalocation'] = self._pit_info.bd_datalocation
		self._df_pit_placeholder['bd_volume'] = self._pit_info.bd_volume
		self._df_pit_placeholder['cda_tablename'] = self._pit_info.cda_tablename
		self._df_pit_placeholder['flow_script_location'] = self._pit_info.flow_script_location
		if self._df_pit_placeholder.empty:
			raise MissingSnapshotDataException(
				"Snapshot data location {0} for object {1} is empty ".format(self._pit_tablename, _snapshotlocation)
			)
	
	def _load_pit_snapshot_details(self):
		"""
			Using SQL Lite processing to determine the snapshot date and location required for alter table
			command this also excludes all the mirror snap locations used for dedicated snapshots
		"""
		self._load_pit_snapshot_placeholder()
		_df_pit_placeholder = self._df_pit_placeholder
		self._df_pit_details = pdsql.sqldf("select bd_tablename, "
		                                   "REPLACE(SUBSTR(REPLACE(bd_snapshotid,bd_volume,''),0,11),'_',"
		                                   "'-') bd_snapshotdate, "
		                                   "(bd_snapshotlocation||bd_snapshotid||bd_datalocation) bd_snapshotlocation,"
		                                   "cda_tablename,flow_script_location,bd_snapshotid "
		                                   "from _df_pit_placeholder",
		                                   locals())
		if self._df_pit_details.empty:
			raise MissingPitDataException
	
	def get_pit_details(self):
		_df_pit_details = self._df_pit_details
		return _df_pit_details
	
	@property
	def get_pit_tablename(self):
		return self._pit_info.bd_tablename
	
	@property
	def get_pit_ddl_script(self):
		return self._pit_info.flow_script_location


def get_ss_table_builder(pit_obj_name, pit_hdp_conn_id):
	_pits = CdaPitTableInfo(pit_tablename=pit_obj_name)
	_pit_bd_tablename = _pits.get_pit_tablename
	_pit_bd_script = _pits.get_pit_ddl_script
	return CdaHiveOperator(task_id=_pit_bd_tablename,
	                       hiveserver2_conn_id=pit_hdp_conn_id,
	                       hql=_pit_bd_script)


def get_ss_partition_builder(pit_obj_name, pit_hdp_conn_id, args, _sd_id, **kwargs):
	_sdname = _sd_id
	_subdag = DAG(dag_id=_sdname, default_args=args)
	_pits = CdaPitTableInfo(pit_tablename=pit_obj_name)
	_df_pit_details = _pits.get_pit_details()
	for _df_pit_detail_index, _df_pit_detail in _df_pit_details.iterrows():
		_cdatablename = _df_pit_detail["cda_tablename"]
		_snapshotdate = _df_pit_detail["bd_snapshotdate"]
		_snapshotlocation = _df_pit_detail["bd_snapshotlocation"]
		_snapshotid = _df_pit_detail["bd_snapshotid"]
		_partition_builder_params = {"tablename": _cdatablename, "snapshotdate": _snapshotdate,
		                             "snapshotlocation": _snapshotlocation}
		_taskid = _snapshotid + _cdatablename
		CdaHiveOperator(task_id=_taskid,
		                hiveserver2_conn_id=pit_hdp_conn_id,
		                hql=PITADDPARTITIONSCRIPT,
		                params=_partition_builder_params,
		                dag=_subdag
		                )
	return _subdag
