"""
Backend workflow step repositories types currently supported.
"""

HIVE = "hive"
BIGQUERY = "bigquery"
PROCEDURE = "bigqueryprocedure"
SHELL = "shell"
SPARK = "spark"
BEAM = "beam"

CDA_FLOW_OPERATORS = [HIVE, BIGQUERY, PROCEDURE, SHELL, SPARK, BEAM]
