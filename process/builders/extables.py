import pandasql as pdsql
import yaml
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import DAG
from airflow.utils.decorators import apply_defaults
from airflow_fs.hooks import SftpHook
from pandas import DataFrame


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
