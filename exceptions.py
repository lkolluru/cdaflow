from airflow.exceptions import AirflowException


class MissingConfigException(AirflowException):
	"""Exception thrown when expected configuration file/directory not found"""
	pass


class MissingWorkflowStepsException(AirflowException):
	"""Exception thrown when functional group did not return any steps"""
	pass


class BlankDataStepsException(AirflowException):
	"""Exception thrown when data steps are blank"""
	pass


class MissingSnapshotDataException(AirflowException):
	"""Exception thrown when no snapshots are found in the destination location"""
	pass


class InvalidLocationException(AirflowException):
	"""Exception thrown when invalid snapshot location configured"""
	pass


class MissingGcsDataException(AirflowException):
	"""Exception thrown when no data is found in the buckets in GCP"""
	pass


class InvalidDataLocationFormatException(AirflowException):
	"""Exception thrown when incorrect URI is configured in the configuration"""
	pass


class InvalidCdaOperatorException(AirflowException):
	"""Exception thrown when no matching data is found in the metadata database"""
	pass


class InvalidJdbcConnectionException(AirflowException):
	"""Exception thrown when jdbc connection is configured incorrectly"""
	pass


class InvalidGcpConnectionException(AirflowException):
	"""Exception thrown when GCP connection is configured incorrectly"""
	pass


class InvalidSshConnectionException(AirflowException):
	"""Exception thrown when ssh connection is configured incorrectly"""
	pass


class InvalidMssqlConnectionException(AirflowException):
	"""Exception thrown when mssql connection is configured incorrectly"""
	pass


class InvalidConnTypeException(AirflowException):
	"""Exception thrown when connection type is configured incorrectly"""
	pass


class InvalidConnConfigException(AirflowException):
	"""Exception thrown when connection config is configured incorrectly"""
	pass


class InvalidPitObjectException(AirflowException):
	"""Exception thrown when invalid pit object is being passed"""
	pass


class MissingPitDataException(AirflowException):
	"""Exception thrown when no snapshot data is found post filtering the data"""
	pass
