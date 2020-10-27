import codecs
import errno
import os
import posixpath
import shutil
import sys
import tempfile

import urllib.parse
import urllib.request
from urllib.request import pathname2url

import yaml

try:
	from yaml import CSafeLoader as YamlSafeLoader, CSafeDumper as YamlSafeDumper
except ImportError:
	from yaml import SafeLoader as YamlSafeLoader, SafeDumper as YamlSafeDumper

# from mlflow.entities import FileInfo
from cda.cdaflow.exceptions import MissingConfigException

ENCODING = "utf-8"


def is_directory(name):
	return os.path.isdir(name)


def is_file(name):
	return os.path.isfile(name)


def exists(name):
	return os.path.exists(name)


def list_all(root, filter_func=lambda x: True, full_path=False):
	"""
    List all entities directly under 'dir_name' that satisfy 'filter_func'
    :param root: Name of directory to start search
    :param filter_func: function or lambda that takes path
    :param full_path: If True will return results as full path including `root`
    :return: list of all files or directories that satisfy the criteria.
    """
	if not is_directory(root):
		raise Exception("Invalid parent directory '%s'" % root)
	matches = [x for x in os.listdir(root) if filter_func(os.path.join(root, x))]
	return [os.path.join(root, m) for m in matches] if full_path else matches


def list_subdirs(dir_name, full_path=False):
	"""
    Equivalent to UNIX command:
      ``find $dir_name -depth 1 -type d``
    :param dir_name: Name of directory to start search
    :param full_path: If True will return results as full path including `root`
    :return: list of all directories directly under 'dir_name'
    """
	return list_all(dir_name, os.path.isdir, full_path)


def list_files(dir_name, full_path=False):
	"""
    Equivalent to UNIX command:
      ``find $dir_name -depth 1 -type f``
    :param dir_name: Name of directory to start search
    :param full_path: If True will return results as full path including `root`
    :return: list of all files directly under 'dir_name'
    """
	return list_all(dir_name, os.path.isfile, full_path)


def find(root, name, full_path=False):
	"""
    Search for a file in a root directory. Equivalent to:
      ``find $root -name "$name" -depth 1``
    :param root: Name of root directory for find
    :param name: Name of file or directory to find directly under root directory
    :param full_path: If True will return results as full path including `root`
    :return: list of matching files or directories
    """
	path_name = os.path.join(root, name)
	return list_all(root, lambda x: x == path_name, full_path)


def mkdir(root, name=None):  # noqa
	"""
    Make directory with name "root/name", or just "root" if name is None.
    :param root: Name of parent directory
    :param name: Optional name of leaf directory
    :return: Path to created directory
    """
	target = os.path.join(root, name) if name is not None else root
	try:
		os.makedirs(target)
	except OSError as e:
		if e.errno != errno.EEXIST or not os.path.isdir(target):
			raise e
	return target


def make_containing_dirs(path):
	"""
    Create the base directory for a given file path if it does not exist; also creates parent
    directories.
    """
	dir_name = os.path.dirname(path)
	if not os.path.exists(dir_name):
		os.makedirs(dir_name)


def read_yaml(root, file_name):
	"""
    Read data from yaml file and return as dictionary
    :param root: Directory name
    :param file_name: File name. Expects to have '.yaml' extension
    :return: Data in yaml file as dictionary
    """
	if not exists(root):
		raise MissingConfigException(
			"Cannot read '%s'. Parent dir '%s' does not exist." % (file_name, root)
		)
	
	file_path = os.path.join(root, file_name)
	if not exists(file_path):
		raise MissingConfigException("Yaml file '%s' does not exist." % file_path)
	try:
		with codecs.open(file_path, mode="r", encoding=ENCODING) as yaml_file:
			return yaml.load(yaml_file, Loader=YamlSafeLoader)
	except Exception as e:
		raise e


class TempDir(object):
	def __init__(self, chdr=False, remove_on_exit=True):
		self._dir = None
		self._path = None
		self._chdr = chdr
		self._remove = remove_on_exit
	
	def __enter__(self):
		self._path = os.path.abspath(tempfile.mkdtemp())
		assert os.path.exists(self._path)
		if self._chdr:
			self._dir = os.path.abspath(os.getcwd())
			os.chdir(self._path)
		return self
	
	def __exit__(self, tp, val, traceback):
		if self._chdr and self._dir:
			os.chdir(self._dir)
			self._dir = None
		if self._remove and os.path.exists(self._path):
			shutil.rmtree(self._path)
		
		assert not self._remove or not os.path.exists(self._path)
		assert os.path.exists(os.getcwd())
	
	def path(self, *path):
		return os.path.join("./", *path) if self._chdr else os.path.join(self._path, *path)


def read_file_lines(parent_path, file_name):
	"""
    Return the contents of the file as an array where each element is a separate line.
    :param parent_path: Full path to the directory that contains the file.
    :param file_name: Leaf file name.
    :return: All lines in the file as an array.
    """
	file_path = os.path.join(parent_path, file_name)
	with codecs.open(file_path, mode="r", encoding=ENCODING) as f:
		return f.readlines()


def read_file(parent_path, file_name):
	"""
    Return the contents of the file.
    :param parent_path: Full path to the directory that contains the file.
    :param file_name: Leaf file name.
    :return: The contents of the file.
    """
	file_path = os.path.join(parent_path, file_name)
	with codecs.open(file_path, mode="r", encoding=ENCODING) as f:
		return f.read()


def get_relative_path(root_path, target_path):
	"""
    Remove root path common prefix and return part of `path` relative to `root_path`.
    :param root_path: Root path
    :param target_path: Desired path for common prefix removal
    :return: Path relative to root_path
    """
	if len(root_path) > len(target_path):
		raise Exception("Root path '%s' longer than target path '%s'" % (root_path, target_path))
	common_prefix = os.path.commonprefix([root_path, target_path])
	return os.path.relpath(target_path, common_prefix)


def mv(target, new_parent):
	shutil.move(target, new_parent)


def write_to(filename, data):
	with codecs.open(filename, mode="w", encoding=ENCODING) as handle:
		handle.write(data)


def append_to(filename, data):
	with open(filename, "a") as handle:
		handle.write(data)


def get_parent_dir(path):
	return os.path.abspath(os.path.join(path, os.pardir))


def path_to_local_file_uri(path):
	"""
    Convert local filesystem path to local file uri.
    """
	path = pathname2url(path)
	if path == posixpath.abspath(path):
		return "file://{path}".format(path=path)
	else:
		return "file:{path}".format(path=path)


def path_to_local_sqlite_uri(path):
	"""
    Convert local filesystem path to sqlite uri.
    """
	path = posixpath.abspath(pathname2url(os.path.abspath(path)))
	prefix = "sqlite://" if sys.platform == "win32" else "sqlite:///"
	return prefix + path


def local_file_uri_to_path(uri):
	"""
    Convert URI to local filesystem path.
    No-op if the uri does not have the expected scheme.
    """
	path = urllib.parse.urlparse(uri).path if uri.startswith("file:") else uri
	return urllib.request.url2pathname(path)


def get_local_path_or_none(path_or_uri):
	"""Check if the argument is a local path (no scheme or file:///) and return local path if true,
    None otherwise.
    """
	parsed_uri = urllib.parse.urlparse(path_or_uri)
	if len(parsed_uri.scheme) == 0 or parsed_uri.scheme == "file" and len(parsed_uri.netloc) == 0:
		return local_file_uri_to_path(path_or_uri)
	else:
		return None