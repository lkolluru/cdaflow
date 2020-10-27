import pandas as pd
import pandasql as pdsql
from pandas import DataFrame
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.dummy_operator import DummyOperator
import logging  # TODO convert the connection to the google STAQ driver logging and integrate accordingly
from airflow.settings import Session
from airflow.models import Connection
from airflow_fs.hooks import SftpHook
import yaml
import json

'''
def load_connection_configuration():
	# TODO obtain this information from a global env variable
	with open('/c/Users/lkolluru/AirflowHome/config/omnicorenested/conn/connections.yml',
	          encoding='UTF-8') as conf_file:
		cda_configs = yaml.load(conf_file, Loader=yaml.FullLoader)
		return dict(cda_configs)


'''


def setup_new_connection(attributes):
	session = Session()
	conn_id = attributes.get("conn_id")
	new_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
	# TODO Refactor to a dedicated class and include a update process process to check and update conn
	if not new_conn:
		logging.info("adding new connection")
		new_conn = Connection()
		new_conn.conn_id = conn_id
		new_conn.conn_type = attributes.get('conn_type')
		new_conn.host = attributes.get('host')
		new_conn.port = attributes.get('port')
		new_conn.schema = attributes.get('schema')
		new_conn.login = attributes.get('login')
		new_conn.set_password(attributes.get('password'))
		new_conn.set_extra(json.dumps(attributes.get('extra')))
		session.merge(new_conn)
		session.commit()
		logging.info("Cda connection" + new_conn.conn_id + " created successfully")
		session.close()
	logging.info("Cda connection already in the repository")


'''


def test_connection(conn_id, conn_type, attributes):
	if conn_type == 'google_cloud_platform':
		_bucketname = attributes.get("bucketname")
		_prefixname = attributes.get("prefixname")
		_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=conn_id)
		_test = _hook.list(bucket=_bucketname, delimiter='/', prefix=_prefixname)
		print(_test)
	if conn_type == 'msql':
		_sql = attributes.get("sql")
		_hook = MsSqlHook(conn_id)
		_test = _hook.get_pandas_df(_sql)
		print(_test)
	if conn_type == 'ssh':
		_sftp_location = attributes.get("dirname")
		_sftp_hook = SftpHook(conn_id=conn_id)
		_test = _sftp_hook.listdir(_sftp_location)
		print(_test)
	if conn_type == 'jdbc':
		_sql = attributes.get("sql")
		_jdbc_hook = JdbcHook(conn_id)
		_test = _jdbc_hook.get_pandas_df(sql=_sql)
		print(_test)
'''


class CdaDagBuilder:
	"""
	    Connects and pulls data from the operational metadata repository.
		#TODO Currently the get_steps() is making 15 calls to the database to get the info to enhance the init
		#to get information only once and pass it to downstream
		would need to make the call only once.
	    Operational metadata repository consists of functional groups,tablegroups,
	    and workflow steps.
	    :param functional_group: Overall Functional Group Information required for the operations. (templated)
	    :type functional_group: str
	    :param mssql_conn_id: Connects to the . (templated)
	    :type mssql_conn_id: str
	"""
	template_fields = ('functional_group', 'mssql_conn_id')
	
	@apply_defaults
	def __init__(self,
	             functional_group='omnicorenested',
	             mssql_conn_id='dev_ocn_mssql',
	             *args, **kwargs):
		self.functional_group = functional_group
		self.mssql_conn_id = mssql_conn_id
		self.execution_procedure = 'EXEC appinfo.airflow_get_workflow_execution_info ' \
		                           '@s_functionalgroupcode={0}'.format(functional_group)
		_hook = MsSqlHook(self.mssql_conn_id)
		self.df = _hook.get_pandas_df(self.execution_procedure)
	
	def get_functionalgroup(self):
		_allsteps = self.df
		df_functionalgroup = pd.DataFrame(data=_allsteps)
		fg = pdsql.sqldf("select distinct variablefunctionalgroupcode from df_functionalgroup", locals())
		return fg
	
	def get_tablegroup(self):
		_allsteps = self.df
		df_tablegroup = pd.DataFrame(data=_allsteps)
		tg = pdsql.sqldf(
			"select distinct variabletablegroupname,variabletablegroupbuildorder from df_tablegroup ORDER BY  "
			"variabletablegroupbuildorder ASC",
			locals())
		return tg
	
	def get_workflowstep(self):
		_allsteps = self.df
		df_workflowstep = pd.DataFrame(data=_allsteps)
		ws = pdsql.sqldf(
			"select distinct variabletablegroupname,variableworkflowstepname,variableworkflowstepquerytype,"
			"variableworkflowstepexecutionorder,variableworkflowstepquery,"
			"workflowstepqueryparameters from df_workflowstep",
			locals())
		return ws
	
	def get_functionalgroupname(self):
		fg = self.get_functionalgroup()
		fg_name = fg['variablefunctionalgroupcode'].values[0].lower()
		return fg_name
	
	def get_tablegroupbuildorder(self):
		tg = self.get_tablegroup()
		tg_buildorder = tg.variabletablegroupbuildorder.unique()
		return tg_buildorder
	
	def get_filtered_tablegroupnames(self, _group_execution_order):
		tg = self.get_tablegroup()
		filtered_group_names = pdsql.sqldf(
			"select distinct variabletablegroupname from tg where variabletablegroupbuildorder = {0}".format(
				str(_group_execution_order)),
			locals())
		return filtered_group_names
	
	def get_filtered_workflowstepnames(self, _table_group_name):
		ws = self.get_workflowstep()
		stepnames = pdsql.sqldf(
			"select replace(variableworkflowstepname,':','_') variableworkflowstepname, "
			"variableworkflowstepexecutionorder,variableworkflowstepquery,"
			"workflowstepqueryparameters,variableworkflowstepquerytype "
			"from ws where variabletablegroupname =" + "'" + _table_group_name + "'" +
			" order by variableworkflowstepexecutionorder ASC",
			locals())
		return stepnames
	
	@staticmethod
	def get_workflow_operator(_stepname):
		_step_name = _stepname["variableworkflowstepname"]
		_step_query_type = _stepname["variableworkflowstepquerytype"].lower()
		
		_step_query = _stepname["variableworkflowstepquery"]
		_step_query_parameters = _stepname["workflowstepqueryparameters"]
		
		if _step_query_type == 'hive':
			_operator_task_name = _step_name + '_' + _step_query_type
			return JdbcOperator(task_id=_operator_task_name,
			                    jdbc_conn_id='dev_ocn_hive_edge2',
			                    sql=_step_query,
			                    parameters=_step_query_parameters)
		if _step_query_type != 'hive':
			return DummyOperator(task_id='notimplemented' + _step_name)


class CdaLabelBuilder:
	"""
	    Prepares the dummy operators required to orchestrate the execution.
	    Each operation requires a start and end with flow based values.
	    #TODO to connect and pull from db all the information at the time of initiation and
	    #TODO to apply custom colours to the dummy operators for clear identification of labels
	    #prepare the labels
	"""
	
	@staticmethod
	def get_workflowlabel_start(_name):
		_task_id = 'start_' + str(_name).lower()
		return DummyOperator(
			task_id=_task_id
		)
	
	@staticmethod
	def get_workflowlabel_end(_name):
		_task_id = 'end_' + str(_name).lower()
		return DummyOperator(
			task_id=_task_id
		)
