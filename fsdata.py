"""
Module required to get the file system data required to build dynamic airflow dags.
"""

import os
from cda.cdaflow.exceptions import MissingSnapshotDataException, InvalidLocationException, \
	InvalidDataLocationFormatException
from cda.cdaflow.utils.file_utils import is_directory, path_to_local_file_uri
import re
import urllib.parse
from pandas import DataFrame

S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
MAPR_PREFIX = "/mapr/JMAPRCLUP01.CLASSIC.PCHAD.COM/"
S3_REGEX = re.compile("^%s" % re.escape(S3_PREFIX))
GS_REGEX = re.compile("^%s" % re.escape(GS_PREFIX))
MAPR_REGEX = re.compile("^%s" % re.escape(MAPR_PREFIX))

'''
def _fetch_dbfs(uri, local_path):
	print("=== Downloading DBFS file %s to local path %s ===" % (uri, os.path.abspath(local_path)))
	process.exec_cmd(cmd=["databricks", "fs", "cp", "-r", uri, local_path])


def _get_hdfs_data(conn_id, location_id):
	from airflow_fs.hooks import SftpHook
	"""
		Use the Bigdata Volume snapshot location to obtain the existing snapshotid information
		Note: this is necessary to accommodate the regular cleanup being implemented
	"""
	_sftp_hook = SftpHook(conn_id)
	try:
		_data = DataFrame(_sftp_hook.listdir(location_id))
	except:
		raise InvalidLocationException
	
	if _data.empty:
		raise MissingSnapshotDataException
	return _data


def _fetch_gs(uri, local_path):
	from google.cloud import storage
	
	print("=== Downloading GCS file %s to local path %s ===" % (uri, os.path.abspath(local_path)))
	(bucket, gs_path) = parse_gs_uri(uri)
	storage.Client().bucket(bucket).blob(gs_path).download_to_filename(local_path)
'''


def _get_hdfs_dataframe(conn_id, location_id):
	from airflow_fs.hooks import SftpHook
	"""
		Use the Bigdata Volume snapshot location to obtain the existing snapshotid information
		Note: this is necessary to accommodate the regular cleanup being implemented
	"""
	_sftp_hook = SftpHook(conn_id)
	try:
		_data = DataFrame(_sftp_hook.listdir(location_id))
	except Exception:
		raise InvalidLocationException
	
	if _data.empty:
		raise MissingSnapshotDataException
	return _data


def parse_s3_uri(uri):
	"""Parse an S3 URI, returning (bucket, path)"""
	parsed = urllib.parse.urlparse(uri)
	if parsed.scheme != "s3":
		raise Exception("Not an S3 URI: %s" % uri)
	path = parsed.path
	if path.startswith("/"):
		path = path[1:]
	return parsed.netloc, path


def parse_gs_uri(uri):
	"""Parse an GCS URI, returning (bucket, path)"""
	parsed = urllib.parse.urlparse(uri)
	if parsed.scheme != "gs":
		raise Exception("Not a GCS URI: %s" % uri)
	path = parsed.path
	if path.startswith("/"):
		path = path[1:]
	return parsed.netloc, path


def parse_maprfs_uri(uri):
	"""Parse an MAPRFS URI, returning (mfsroot, volume, path)"""
	pass


def is_uri(string):
	parsed_uri = urllib.parse.urlparse(string)
	return len(parsed_uri.scheme) > 0


def is_location(string):
	if not is_directory(string):
		raise Exception("Invalid parent directory '%s'" % string)


def fetch_dataframe(conn_id, location_id):
	if MAPR_REGEX.match(location_id):
		_get_hdfs_dataframe(conn_id, location_id)
	elif S3_REGEX.match(location_id):
		_get_hdfs_dataframe(conn_id, location_id)
	elif GS_REGEX.match(location_id):
		_get_hdfs_dataframe(conn_id, location_id)
	else:
		raise InvalidDataLocationFormatException(
			"`Location` must be a MAPRFS (%s), S3 (%s), or GCS (%s) Location, got "
			"%s" % (MAPR_PREFIX, S3_PREFIX, GS_PREFIX, location_id)
		)
