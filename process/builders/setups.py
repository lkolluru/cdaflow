import json
import logging
import re

from airflow.models import Connection
from airflow.settings import Session
from airflow.utils.decorators import apply_defaults

from cda.cdaflow.configs import OcnGcp, OcnMssql, OcnSshEdge2, OcnHiveEdge2, OcnPyHiveEdge2
from cda.cdaflow.exceptions import InvalidConnConfigException, InvalidJdbcConnectionException, \
	InvalidGcpConnectionException, InvalidSshConnectionException, InvalidMssqlConnectionException

"""
Module required to get the connection data required to prepare the connections for airflow dags.
"""

JDBC_PREFIX = "ocn_hive_edge2"
SHELL_PREFIX = "ocn_ssh_edge2"
BIGQUERY_PREFIX = "ocn_gcp"
MSSQL_PREFIX = "ocn_mssql"
HIVE2_PREFIX = "ocn_pyhive_edge2"

"""
 Regular expression module required for switch operations.
"""

JDBC_REGEX = re.compile("^{}".format(re.escape(JDBC_PREFIX)))
SHELL_REGEX = re.compile("^%s" % re.escape(SHELL_PREFIX))
BIGQUERY_REGEX = re.compile("^%s" % re.escape(BIGQUERY_PREFIX))
MSSQL_REGEX = re.compile("^%s" % re.escape(MSSQL_PREFIX))
HIVE2_REGEX = re.compile("^%s" % re.escape(HIVE2_PREFIX))


class CdaFlowConnectionSetup:
	"""
	    Prepares the objects and functions required to setup and test the connections in airflow DB
	    by executing the configurations this is required to determine if the environment configurations are
	    loaded correctly.
	    #TODO create a process to make the methods in the class callable so the python operator can be leveraged.
	"""
	
	template_fields = 'cda_conn'
	
	@apply_defaults
	def __init__(self,
	             cda_conn=None,
	             default=None,
	             *args, **kwargs
	             ):
		self.cda_conn = cda_conn
		self.default = default
		self.ocn_gcp = OcnGcp()
		self.ocn_mssql = OcnMssql()
		self.ocn_ssh_edge2 = OcnSshEdge2()
		self.ocn_hive_edge2 = OcnHiveEdge2()
		self.ocn_pyhive_edge2 = OcnPyHiveEdge2()
		self.session = Session()
		self.orm_conn = Connection()
	
	def _test_ocn_hive_edge2(self):
		from airflow.hooks.jdbc_hook import JdbcHook
		_id = self.ocn_hive_edge2.conn_id
		_test = self.ocn_hive_edge2.test
		try:
			_sql = _test.get("sql")
			_jdbc_hook = JdbcHook(_id)
			_jdbc_hook.get_pandas_df(sql=_sql)
			global IdOcnHiveEdge2
			IdOcnHiveEdge2 = _id
		
		except Exception:
			raise InvalidJdbcConnectionException("unable to connect to the jdbc {0} using connection id".format(_id))
	
	def _test_ocn_gcp(self):
		from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
		_id = self.ocn_gcp.conn_id
		_test = self.ocn_gcp.test
		try:
			_bucketname = _test.get("bucketname")
			_prefixname = _test.get("prefixname")
			_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=_id)
			_hook.list(bucket=_bucketname, delimiter='/', prefix=_prefixname)
		except Exception:
			raise InvalidGcpConnectionException("unable to connect to the gcp {0} using connection id".format(_id))
	
	def _test_ocn_ssh_edge2(self):
		from airflow_fs.hooks import SftpHook
		_id = self.ocn_ssh_edge2.conn_id
		_test = self.ocn_ssh_edge2.test
		try:
			_sftp_location = _test.get("dirname")
			_sftp_hook = SftpHook(conn_id=_id)
			_test = _sftp_hook.listdir(_sftp_location)
		except Exception:
			raise InvalidSshConnectionException("unable to connect to the ssh {0} using connection id".format(_id))
	
	def _test_ocn_mssql(self):
		from airflow.hooks.mssql_hook import MsSqlHook
		_id = self.ocn_mssql.conn_id
		_test = self.ocn_mssql.test
		try:
			_sql = _test.get("sql")
			_hook = MsSqlHook(_id)
			_test = _hook.get_pandas_df(_sql)
		except Exception:
			raise InvalidMssqlConnectionException("unable to connect to the mssql {0} using connection id".format(_id))
	
	def _test_ocn_pyhive_edge2(self):
		from cda_plugins.cda_metadata_plugin.hooks.cda_hive_hook import CdaHiveHook
		_id = self.ocn_pyhive_edge2.conn_id
		_test = self.ocn_pyhive_edge2.test
		try:
			_sql = _test.get("sql")
			_pyhive_hook = CdaHiveHook(_id)
			_pyhive_hook.run_query(hql=_sql)
		except Exception:
			raise InvalidJdbcConnectionException("unable to connect to the pyhive {0} using connection id".format(_id))
	
	def _test_default(self):
		raise NotImplementedError
	
	test_control = {
		"_test_ocn_hive_edge2": _test_ocn_hive_edge2,
		"_test_ocn_gcp": _test_ocn_gcp,
		"_test_ocn_ssh_edge2": _test_ocn_ssh_edge2,
		"_test_ocn_mssql": _test_ocn_mssql,
		"_test_ocn_pyhive_edge2": _test_ocn_pyhive_edge2
	}
	
	def test_conn(self):
		_test_name = '_test_' + str(self.cda_conn)
		_test = self.test_control[_test_name]
		try:
			_test(self)
			return True
		except Exception as e:
			raise NotImplemented
	
	def _fetch_connection(self):
		if JDBC_REGEX.match(self.cda_conn):
			return self.ocn_hive_edge2, self.ocn_hive_edge2.conn_id, self.ocn_hive_edge2.setup
		elif SHELL_REGEX.match(self.cda_conn):
			return self.ocn_ssh_edge2, self.ocn_ssh_edge2.conn_id, self.ocn_ssh_edge2.setup
		elif BIGQUERY_REGEX.match(self.cda_conn):
			return self.ocn_gcp, self.ocn_gcp.conn_id, self.ocn_gcp.setup
		elif MSSQL_REGEX.match(self.cda_conn):
			return self.ocn_mssql, self.ocn_mssql.conn_id, self.ocn_gcp.setup
		elif HIVE2_REGEX.match(self.cda_conn):
			return self.ocn_pyhive_edge2, self.ocn_pyhive_edge2.conn_id, self.ocn_pyhive_edge2.setup
		else:
			raise InvalidConnConfigException(
				"`Connections configured for the workflow` must be a JDBC (%s), HIVE2 (%s), or SHELL (%s)  or "
				"BIGQUERY (%s), or MSSQL (%s) got "
				"%s" % (JDBC_PREFIX, HIVE2_PREFIX, SHELL_PREFIX, BIGQUERY_PREFIX, MSSQL_PREFIX, self.cda_conn)
			)
	
	def setup_connection(self):
		
		_gen_conn_info, _gen_conn_id, _gen_conn_setup = self._fetch_connection()
		check_new_conn = self.session.query(Connection).filter(Connection.conn_id == _gen_conn_id).first()
		
		if not check_new_conn:
			self._create_new_connection(_gen_conn_id, _gen_conn_setup)
			return True
	
	def _create_new_connection(self, new_conn_id, new_conn_setup):
		
		conn_id = new_conn_id
		logging.info("adding new connection.{0}".format(conn_id))
		self.orm_conn.conn_id = conn_id
		self.orm_conn.conn_type = new_conn_setup.get('conn_type')
		self.orm_conn.host = new_conn_setup.get('host')
		self.orm_conn.port = new_conn_setup.get('port')
		self.orm_conn.schema = new_conn_setup.get('schema')
		self.orm_conn.login = new_conn_setup.get('login')
		self.orm_conn.set_password(new_conn_setup.get('password'))
		self.orm_conn.set_extra(json.dumps(new_conn_setup.get('extra')))
		self.session.merge(self.orm_conn)
		self.session.commit()
		logging.info("Cda connection" + new_conn_id + " created successfully")
		self.session.close()
	
	def _update_new_connection(self):
		# check if the hash value of all the info
		raise NotImplemented


def setup_connections(_cda_connection):
	cda_flow_connection = CdaFlowConnectionSetup(cda_conn=str(_cda_connection))
	try:
		cda_flow_connection.setup_connection()
	except Exception as e:
		raise NotImplemented


def test_connections(_cda_connection):
	cda_flow_connection = CdaFlowConnectionSetup(cda_conn=str(_cda_connection))
	try:
		cda_flow_connection.test_conn()
	except Exception as e:
		raise NotImplemented
