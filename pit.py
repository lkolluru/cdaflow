import pandasql as pdsql
import yaml
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults
from airflow_fs.hooks import SftpHook
from pandas import DataFrame

from cda_plugins.cda_metadata_plugin.operators.cda_hive_operator import CdaHiveOperator


def cda_dataprocessing_error(exception):
	# TODO duplicate the exception handling from generic JAR.
	pass


def load_pit_configuration():
	# TODO obtain this information from a global env variable
	with open('/c/Users/lkolluru/AirflowHome/config/omnicorenested/pit/pit.yml',
	          encoding='UTF-8') as conf_file:
		cda_pit_configs = yaml.load(conf_file, Loader=yaml.FullLoader)
		return dict(cda_pit_configs)


class CdaPointInTimeBuilder:
	"""
			Connects and pulls the necessary information required to run the guided parallel
			processing required for the hadoop operations
			#TODO can be converted to linux subsystem operator to leverage the necessary information
			:param Location: Full linux path required to operate the controlled loop. (templated)
			:type  Location: str
			:param sftp_conn_id: Connects to the required host to pull the information . (templated)
			:type  sftp_conn_id: str
	"""
	template_fields = (
		'bigdatatablename', 'snapshotlocation', 'datalocation', 'bigdatavolumename', 'cdatablename')
	
	@apply_defaults
	def __init__(self,
	             bigdatatablename=None,
	             conn_id='dev_ocn_ssh_edge2',
	             snapshotlocation=None,
	             datalocation=None,
	             bigdatavolumename=None,
	             cdatablename=None,
	             *args, **kwargs
	             ):
		self.ddlscriptlocation = None
		self.bigdatatablename = bigdatatablename
		self.conn_id = conn_id
		self.snapshotlocation = snapshotlocation
		self.datalocation = datalocation
		self.bigdatavolumename = bigdatavolumename
		self.cdatablename = cdatablename
	
	def _get_snapshot_list(self):
		"""
			Use the Bigdata Volume snapshot location to obtain the existing snapshotid information
			Note: this is necessary to accommodate the regular cleanup being implemented
			# TODO post the inital load add a method to check the partitions in existing table and determine delta
		"""
		_sftp_hook = SftpHook(conn_id=self.conn_id)
		snapshot_list = DataFrame(_sftp_hook.listdir(self.snapshotlocation), columns=['snapshotid'])
		if snapshot_list.empty:
			raise cda_dataprocessing_error
		return snapshot_list
	
	def _get_snapshotdetails(self):
		"""
			Extension to the previous method to append all the auxiliary information required for post processing
		"""
		_snapshot_list = self._get_snapshot_list()
		_snapshot_list['bigdatatablename'] = self.bigdatatablename
		_snapshot_list['snapshotlocation'] = self.snapshotlocation
		_snapshot_list['datalocation'] = self.datalocation
		_snapshot_list['bigdatavolumename'] = self.bigdatavolumename
		_snapshot_list['cdatablename'] = self.cdatablename
		_snapshot_list['ddlscriptlocation'] = self.ddlscriptlocation
		snapshotdetails = _snapshot_list
		if snapshotdetails.empty:
			raise cda_dataprocessing_error
		return snapshotdetails
	
	def get_snapshotpartitionlocation(self):
		"""
			Using SQL Lite processing to determine the snapshot date and location required for alter table
			command
		"""
		_snapshotdetails = self._get_snapshotdetails()
		snapshotpartitiondetails = pdsql.sqldf("select bigdatatablename, "
		                                       "REPLACE(SUBSTR(REPLACE(snapshotid,bigdatavolumename,''),0,11),'_',"
		                                       "'-') snapshotdate, "
		                                       "(snapshotlocation||snapshotid||datalocation) snapshotlocation,"
		                                       "cdatablename,ddlscriptlocation,snapshotid "
		                                       "from _snapshotdetails",
		                                       locals())
		# TODO add additional validations as this is the core step in the whole process of execution
		if snapshotpartitiondetails.empty:
			raise cda_dataprocessing_error
		return snapshotpartitiondetails
	
	@staticmethod
	def get_snapshottablebuilder(_bigdatatablename, _ddlscriptlocation):
		return CdaHiveOperator(task_id=_bigdatatablename,
		                       hiveserver2_conn_id='hs2_test',
		                       hql=_ddlscriptlocation)
	
	@staticmethod
	def get_snapshottablepartitionbuilder(_cda_pit_builders, args, _sd_id, **kwargs):
		
		_sdname = _sd_id
		_subdag = DAG(dag_id=_sdname, default_args=args)
		for _cda_pit_builder_index, _cda_pit_builder in _cda_pit_builders.iterrows():
			_cdatablename = _cda_pit_builder["cdatablename"]
			_snapshotdate = _cda_pit_builder["snapshotdate"]
			_snapshotlocation = _cda_pit_builder["snapshotlocation"]
			_snapshotid = _cda_pit_builder["snapshotid"]
			_partition_builder_params = {"tablename": _cdatablename, "snapshotdate": _snapshotdate,
			                             "snapshotlocation": _snapshotlocation}
			_taskid = _snapshotid + _cdatablename
			CdaHiveOperator(task_id=_taskid,
			                hiveserver2_conn_id='hs2_test',
			                hql="/gen_sql/omnicorenested/spartitions/add_snapshot_partitions.sql",
			                params=_partition_builder_params,
			                dag=_subdag
			                )
		return _subdag


class CdaExternalTableBuilder:
	"""
		Process used to check and prepare the external tables required for the process
		it is required the portion of the location is a match so it would be comparable.
		#Todo based on the used case the same object to be initiated multiple times for sa,cm,im
	"""
	template_fields = ('attributes', 'conn_id')
	
	@apply_defaults
	def __init__(self,
	             attributes=None,
	             gcp_conn_id='dev_ocn_gcp',
	             hdfs_conn_id=''):
		self.attributes = attributes
		self.gcp_conn_id = gcp_conn_id
		self.bucketname = attributes.get("bucketname")
		self.cloudprefixname = attributes.get("cloudprefixname")
		self.maprprefixname = attributes.get("maprprefixname")
		self.maprlocationname = attributes.get("maprlocationname")
		self._gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcp_conn_id)
		self._sftp_hook = SftpHook(conn_id=hdfs_conn_id)
	
	def _get_gcp_folders(self):
		_cloud_folder = DataFrame(self._gcs_hook.list(bucket=self.bucketname, delimiter='/',
		                                              prefix=self.cloudprefixname), columns=['gcs_foldername'])
		return _cloud_folder
	
	def _get_hdfs_folders(self):
		_hdfs_folder = DataFrame(self._sftp_hook.listdir(self.maprlocationname), columns=['hdp_foldername'])
		return _hdfs_folder
