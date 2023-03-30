
import os
import decimal
import json
import logging
import requests
import datetime
import uuid
import six


import apache_beam as beam
from apache_beam.runners.dataflow.native_io import iobase as dataflow_io
from apache_beam.transforms.display import DisplayDataItem
from apache_beam import coders
from apache_beam.options import value_provider
from apache_beam.transforms import Reshuffle

__version__ = '0.1'

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass

class ExceptionNoColumns(Exception):
    pass


class ExceptionInvalidWrapper(Exception):
    pass


class ExceptionBadRow(Exception):
    pass


from apache_beam.utils import retry
from psycopg2.extensions import Column

try:
    from apitools.base.py.exceptions import HttpError
except ImportError:
    pass



JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
MAX_RETRIES = 5
TABLE_TRUNCATE_SLEEP = 150
DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
CONNECTION_INIT_TIMEOUT = 60
AUTO_COMMIT = False
READ_BATCH = 5000


class SQLDisposition(object):
    WRITE_TRUNCATE = 'WRITE_TRUNCATE'
    WRITE_APPEND = 'WRITE_APPEND'
    WRITE_EMPTY = 'WRITE_EMPTY'
    CREATE_NEVER = 'CREATE_NEVER'
    CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'

    @staticmethod
    def validate_create(disposition):
        values = (SQLDisposition.CREATE_NEVER, SQLDisposition.CREATE_IF_NEEDED)
        if disposition not in values:
            raise ValueError('Invalid write disposition {}. Expecting {}'.format(disposition, values))
        return disposition

    @staticmethod
    def validate_write(disposition):
        values = (SQLDisposition.WRITE_TRUNCATE,
                  SQLDisposition.WRITE_APPEND,
                  SQLDisposition.WRITE_EMPTY)
        if disposition not in values:
            raise ValueError('Invalid write disposition {}. Expecting {}'.format(disposition, values))
        return disposition


class TableSchema(object):
    fields = None
    pass


class TableFieldSchema(object):
    pass


class BaseWrapper(object):
    """
    This eventually need to be replaced with integration with SQLAlchemy
    """

    TEMP_TABLE = 'temp_table_'
    TEMP_database = 'temp_database_'

    def __init__(self, connection):
        self.connection = connection
        self._unique_row_id = 0
        # For testing scenarios where we pass in a client we do not want a
        # randomized prefix for row IDs.
        self._row_id_prefix = '' if connection else uuid.uuid4()
        self._temporary_table_suffix = uuid.uuid4().hex

    def escape_name(self, name):
        """Escape name to avoid SQL injection and keyword clashes.
        Doubles embedded backticks, surrounds the whole in backticks.
        Note: not security hardened, caveat emptor.

        """
        return '`{}`'.format(name.replace('`', '``'))

    @property
    def unique_row_id(self):
        """Returns a unique row ID (str) used to avoid multiple insertions.

        If the row ID is provided, we will make a best effort to not insert
        the same row multiple times for fail and retry scenarios in which the insert
        request may be issued several times. This comes into play for sinks executed
        in a local runner.

        Returns:
          a unique row ID string
        """
        self._unique_row_id += 1
        return '%s_%d' % (self._row_id_prefix, self._unique_row_id)

    @staticmethod
    def row_as_dict(row, schema):
        """
        postgres cursor object contains the description in this format
        (Column(name='id', type_code=23), Column(name='name', type_code=25))

        pymysql cursor description has below format
        ((col1, 123,123,1,23), (col2, 23,123,1,23))
        :param row: database row, tuple/list of objects
        :param schema:
        :return:
        """
        row_dict = {}
        for index, column in enumerate(schema):
            if isinstance(column, Column):
                row_dict[column.name] = row[index]
            else:
                row_dict[column[0]] = row[index]
        return row_dict

    def _get_temp_table(self):
        return "{}.{}".format(self.TEMP_database, self.TEMP_TABLE)

    def _create_table(self, database, table, schema):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_or_create_database(self, database):
        """
        :param database: Check if database already exists otherwise create it
        :return:
        """
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _is_table_empty(self, database, table):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _delete_table(self, database, table):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _delete_database(self, project_id, database, delete_contents=True):
        raise NotImplementedError

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def _get_query_results(self, project_id, job_id,
                         page_token=None, max_results=10000):
        raise NotImplementedError

    @retry.with_exponential_backoff(num_retries=MAX_RETRIES,
                                    retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_table(self, database, table):
        """Lookup a table's metadata object.

        Args:
          database, table: table lookup parameters

        Returns:
          SQL.Table instance
        Raises:
          HttpError if lookup failed.
        """
        raise NotImplementedError

    def _get_cols(self, row, lst_only=False):
        """
        return a sting of columns
        :param row: can be either dict or schema from cursor.description
        :return: string to be placed in insert command of sql
        """
        names = []
        if isinstance(row, dict):
            names = list(row.keys())
        elif isinstance(row, tuple):
            for column in row:
                if isinstance(column, tuple):
                    names.append(column[0])  # columns name is the first attribute in cursor.description
                else:
                    raise ExceptionNoColumns("Not a valid column object")
        if len(names):
            if lst_only:
                return names
            else:
                cols = ', '.join(map(self.escape_name, names))
            return cols
        else:
            raise ExceptionNoColumns("No columns to make")

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def count(self, query):
        with self.connection.cursor() as cursor:
            logging.info("Estimating size for query")
            logging.info(cursor.mogrify(query))
            cursor.execute(query)
            row_count = cursor.rowcount
            return row_count

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def read(self, query, batch=READ_BATCH):
        """
        Execute the query and return the result in batch

        or read in batch

        # for i in range((size//batch)+1):
        #     records = cursor.fetchmany(size=batch)
        #     yield records, schema
        TODO://
        1. Add batch read

        :param query: query to execute
        :param batch: size of batch to read
        :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.debug("Executing Read query")
            #logging.debug(cursor.mogrify(query))
            paginated_query, status = self.paginated_query(query, limit=batch, offset=0)
            if status:
                size = batch
                offset = 0
                while size >= batch:
                    logging.debug("Paginated query")
                    logging.debug(paginated_query)
                    print(paginated_query)
                    cursor.execute(paginated_query)
                    schema = cursor.description
                    size = cursor.rowcount
                    records = cursor.fetchall()
                    yield records, schema
                    offset = offset + batch
                    print("second pagination?", query)
                    paginated_query, status = self.paginated_query(query, limit=batch, offset=offset)
            else:
                print(paginated_query)
                cursor.execute(paginated_query)
                schema = cursor.description
                size = cursor.rowcount
                if " limit " in paginated_query:
                    records = cursor.fetchall()
                    yield records, schema
                else:
                    for i in range((size//batch)+1):
                        records = cursor.fetchmany(size=batch)
                        yield records, schema
            # records = cursor.fetchall()
            # yield records, schema

    @staticmethod
    def paginated_query(query, limit, offset=0):
        if " limit " in query.lower():
            return query, False
        else:
            print("======= paginated_query")
            query = query.strip(";")
            return "{query} LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset), True

    @staticmethod
    def _convert_to_str(value):
        if isinstance(value, six.string_types):
            return value.replace("'", "''")
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return str(value)
        else:
            return value

    @staticmethod
    def _get_data_row(cols, rows):
        data_rows = []
        row_format = ("'{}',"*(len(cols))).rstrip(',')
        for row in rows:
            data = []
            for col in cols:
                _value = row[col]
                _value = BaseWrapper._convert_to_str(_value)
                data.append(_value)
            data_rows.append(row_format.format(*data))
        return tuple(data_rows)

    @staticmethod
    def format_data_rows_query(data_rows):
        rows_format = ("({}),"*len(data_rows)).rstrip(',')
        formatted_rows = rows_format.format(*data_rows)
        return formatted_rows

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        """Inserts rows into the specified table.

        Args:
          table: The table id. either {database}.{table} format or {table} format
          rows: A list of plain Python dictionaries. Each dictionary is a row and
            each key in it is the name of a field.
          skip_invalid_rows: If there are rows with insertion errors, whether they
            should be skipped, and all others should be inserted successfully.

        """
        if not len(rows):
            return None
        with self.connection.cursor() as cursor:
            cols = self._get_cols(rows[0], lst_only=True)
            data_rows = self._get_data_row(cols, rows)
            cols_str = self._get_cols(rows[0], lst_only=False)
            format_data_rows = self.format_data_rows_query(data_rows)

            insert_query = "INSERT INTO {table} ({cols_str}) VALUES {values}  ;".format(
                table=table,
                cols_str=cols_str,
                values=format_data_rows)

            logging.info("Executing insert query")
            logging.debug(cursor.mogrify(insert_query))
            inserted_row_count = cursor.execute(insert_query)
            if hasattr(cursor, 'rowcount'):
                inserted_row_count = cursor.rowcount
            logging.info("Inserted {}".format(inserted_row_count))

            return inserted_row_count

    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        """Gets or creates a table based on create and write dispositions.

        The function mimics the behavior of mysql import jobs when using the
        same create and write dispositions.

        Args:
          database: The database id owning the table.
          table: The table id.
          schema: A TableSchema instance or None.
          create_disposition: CREATE_NEVER or CREATE_IF_NEEDED.
          write_disposition: WRITE_APPEND, WRITE_EMPTY or WRITE_TRUNCATE.

        Returns:
          A mysql.Table instance if table was found or created.

        Raises:
          RuntimeError: For various mismatches between the state of the table and
            the create/write dispositions passed in. For example if the table is not
            empty and WRITE_EMPTY was specified then an error will be raised since
            the table was expected to be empty.
        """

        found_table = None
        try:
            found_table = self.get_table(database, table)
        except HttpError as exn:
            if exn.status_code == 404:
                if create_disposition == SQLDisposition.CREATE_NEVER:
                    raise RuntimeError(
                        'Table %s.%s not found but create disposition is CREATE_NEVER.' % (database, table))
            else:
                raise

        # If table exists already then handle the semantics for WRITE_EMPTY and
        # WRITE_TRUNCATE write dispositions.
        if found_table:
            table_empty = self._is_table_empty(database, table)
            if write_disposition == SQLDisposition.WRITE_TRUNCATE:
                self._delete_table(database, table)

        # Create a new table potentially reusing the schema from a previously
        # found table in case the schema was not specified.
        if schema is None and found_table is None:
            raise RuntimeError(
                'Table %s.%s requires a schema. None can be inferred because the '
                'table does not exist.' % (database, table))
        if found_table and write_disposition != SQLDisposition.WRITE_TRUNCATE:
            return found_table
        else:
            created_table = self._create_table(database=database,
                                               table=table,
                                               schema=schema or found_table.schema)
            logging.info('Created table %s.%s with schema %s. Result: %s.', database, table,
                         schema or found_table.schema,
                         created_table)
            # if write_disposition == mysqlDisposition.WRITE_TRUNCATE we delete
            # the table before this point.
            if write_disposition == SQLDisposition.WRITE_TRUNCATE:
                # mysql can route data to the old table for TABLE_TRUNCATE_SLEEP seconds max so wait
                # that much time before creating the table and writing it
                logging.warning('Sleeping for {} seconds before the write as ' +
                                'mysql inserts can be routed to deleted table ' +
                                'for {} seconds after the delete and create.'.format(TABLE_TRUNCATE_SLEEP))
                return created_table
            else:
                return created_table


class MySQLWrapper(BaseWrapper):
    """mysql client wrapper with utilities for querying.

    The wrapper is used to organize all the mysql integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(MySQLWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(MySQLWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)

class MSSQLWrapper(BaseWrapper):
    """mssql client wrapper with utilities for querying.

    The wrapper is used to organize all the mssql integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(MSSQLWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(MSSQLWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)
    
    @retry.with_exponential_backoff(
        num_retries=MAX_RETRIES,
        retry_filter=retry.retry_on_server_errors_and_timeout_filter)
    def read(self, query, batch=READ_BATCH):
        """
        Execute the query and return the result in batch

        or read in batch

        # for i in range((size//batch)+1):
        #     records = cursor.fetchmany(size=batch)
        #     yield records, schema
        TODO://
        1. Add batch read

        :param query: query to execute
        :param batch: size of batch to read
        :return: iterator of records in batch
        """
        with self.connection.cursor() as cursor:
            logging.debug("Executing Read query")
            #logging.debug(cursor.mogrify(query))
            paginated_query, status = self.paginated_query(query, limit=batch, offset=0)
            if status:
                size = batch
                offset = 0
                while size >= batch:
                    logging.debug("Paginated query")
                    logging.debug(paginated_query)
                    print(paginated_query)
                    cursor.execute(paginated_query)
                    schema = cursor.description
                    size = cursor.rowcount
                    records = cursor.fetchall()
                    yield records, schema
                    offset = offset + batch
                    print("second pagination in the MSSQL world?", query)
                    paginated_query, status = self.paginated_query(query, limit=batch, offset=offset)
            else:
                print(paginated_query)
                cursor.execute(paginated_query)
                schema = cursor.description
                size = cursor.rowcount
                if " fetch " in paginated_query.lower():
                    records = cursor.fetchall()
                    yield records, schema
                else:
                    for i in range((size//batch)+1):
                        records = cursor.fetchmany(size=batch)
                        yield records, schema
            # records = cursor.fetchall()
            # yield records, schema
    
    @staticmethod
    def paginated_query(query, limit, offset=0):
        if " fetch " in query.lower() or  " limit " in query.lower():
            print("closing the query now", query)
            return query, False
        else:
            query = query.strip(";")
            if 'LIMIT' in query:
                indx=query.index('LIMIT')
                query= query[:indx-1]
                print(indx, "HERE", query[:indx-1])
            return "{query} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY;".format(query=query, limit=limit, offset=offset), True


    
class PostgresWrapper(BaseWrapper):
    """postgres client wrapper with utilities for querying.

    The wrapper is used to organize all the postgres integration points and
    offer a common place where retry logic for failures can be controlled.
    In addition it offers various functions used both in sources and sinks
    (e.g., find and create tables, query a table, etc.).
    """

    def get_or_create_table(self, database, table, schema, create_disposition, write_disposition):
        return super(PostgresWrapper, self).get_or_create_table(database, table, schema, create_disposition, write_disposition)

    def insert_rows(self, table, rows, skip_invalid_rows=False):
        return super(PostgresWrapper, self).insert_rows(table, rows, skip_invalid_rows=skip_invalid_rows)

    def escape_name(self, name):
        """Escape name to avoid SQL injection and keyword clashes.
        Doubles embedded backticks, surrounds the whole in backticks.
        Note: not security hardened, caveat emptor.

        """
        return self.connection.cursor().mogrify(name.replace('`', '``')).decode('utf-8')


class SQLWriteDoFn(beam.DoFn):
    """Takes in a set of elements, and inserts them to Mysql/Postgres via batch loads.
    """

    def __init__(self,
                 host,
                 port,
                 database,
                 username,
                 password,
                 destination,
                 batch_size,
                 autocommit,
                 wrapper,
                 schema=None,
                 create_disposition=None,
                 write_disposition=None,
                 validate=True):

        super(SQLWriteDoFn, self).__init__()

        self.wrapper = wrapper
        self.destination = destination
        self.schema = schema
        self.create_disposition = create_disposition
        self.write_disposition = write_disposition
        self.batch_size = batch_size
        self._validate = validate
        if self._validate:
            self.verify()

        self._elements = None

        self.client = None
        self.connection = None

        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.autocommit = autocommit

    def verify(self):
        pass

    def _build_connection_mysql(self):
        """
        Create connection object with mysql or postgres based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        # if self.connection is not None and hasattr(self.connection, 'close'):
        #     return self.connection

        if self.wrapper == MySQLWrapper:
            import pymysql
            connection = pymysql.connect(host=self.host, port=int(self.port),
                                         user=self.username, password=self.password,
                                         database=self.database,
                                         connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.wrapper == PostgresWrapper:
            import psycopg2
            connection = psycopg2.connect(host=self.host, port=int(self.port),
                                          user=self.username, password=self.password,
                                          database=self.database)
            connection.autocommit = self.autocommit or AUTO_COMMIT
        elif self.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")
        self.connection = connection
        return connection

    def _build_value(self, keys):
        for key in keys:
            setattr(self, key, SQLSource.get_value(getattr(self, key)))

    def start_bundle(self):

        self._build_value(['host', 'port', 'username', 'password', 'database',
                           'destination', 'schema', 'batch_size', 'autocommit'])

        if '.' in self.destination:
            self.destination = self.destination
        else:
            self.destination = "{}.{}".format(self.database, self.destination)

        self.connection = self._build_connection_mysql()
        self.client = self.wrapper(connection=self.connection)
        self._elements = []

    def process(self, element, *args, **kwargs):
        if not isinstance(element, dict):
            raise ExceptionBadRow("{} should be dict type instead of {}".format(element, type(element)))
        if self._elements and (len(self._elements) > self.batch_size):
            self._flush_batch()

        self._elements.append(element)
        if len(self._elements) >= self.batch_size:
            self._flush_batch()

    def finish_bundle(self):
        if len(self._elements):
            self._flush_batch()
        if hasattr(self.connection, 'close'):
            self.connection.close()

    def _flush_batch(self):
        logging.debug("Writing %d records ", len(self._elements))
        if len(self._elements):
            try:
                self.client.insert_rows(self.destination, self._elements)
            except Exception as ex:
                logging.error("Exception when inserting into SQL Database")
                logging.error(ex)
                self.connection.rollback()
                raise ex
            finally:
                self.connection.commit()
        self._elements = []



JSON_COMPLIANCE_ERROR = 'NAN, INF and -INF values are not JSON compliant.'
MAX_RETRIES = 5
TABLE_TRUNCATE_SLEEP = 150
DATETIME_FMT = '%Y-%m-%d %H:%M:%S.%f UTC'
CONNECTION_INIT_TIMEOUT = 60
READ_COMMITTED = True
AUTO_COMMIT = False
READ_BATCH = 1000
WRITE_BATCH = 1000
MAX_QUERY_SPLIT = 50000

def default_encoder(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(
        "Object of type '%s' is not JSON serializable" % type(obj).__name__)


class RowAsDictJsonCoder(coders.Coder):
    """A coder for a table row (represented as a dict) to/from a JSON string.

    This is the default coder for sources and sinks if the coder argument is not
    specified.
    """

    def encode(self, table_row):
        # The normal error when dumping NAN/INF values is:
        # ValueError: Out of range float values are not JSON compliant
        # This code will catch this error to emit an error that explains
        # to the programmer that they have used NAN/INF values.
        try:
            return json.dumps(
                table_row, allow_nan=False, default=default_encoder).encode('utf-8')
        except ValueError as e:
            raise ValueError('%s. %s' % (e, JSON_COMPLIANCE_ERROR))

    def decode(self, encoded_table_row):
        return json.loads(encoded_table_row.decode('utf-8'))


class SQLReader(dataflow_io.NativeSourceReader):
    """A reader for a mysql Database source."""

    def __init__(self, source, use_legacy_sql=True, flatten_results=True, kms_key=None):
        self.source = source
        self.connection = None

        self.row_as_dict = isinstance(self.source.coder, RowAsDictJsonCoder)
        # Schema for the rows being read by the reader. It is initialized the
        # first time something gets read from the table. It is not required
        # for reading the field values in each row but could be useful for
        # getting additional details.
        self.schema = None
        self.use_legacy_sql = use_legacy_sql
        self.flatten_results = flatten_results
        self.kms_key = kms_key

        if self.source.table is not None:
            self.query = """SELECT * FROM {};""".format(self.source.table)
        elif self.source.query is not None:
            self.query = self.source.query
        else:
            raise ValueError("mysqlSource must have either a table or query")

    def _build_connection_mysql(self):
        """
        Create connection object with mysql, mssql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        # if self.connection is not None and hasattr(self.connection, 'close'):
        #     return self.connection

        if self.source.wrapper == MySQLWrapper:
            import pymysql
            connection = pymysql.connect(host=self.source.host, port=int(self.source.port),
                                         user=self.source.username, password=self.source.password,
                                         database=self.source.database,
                                         connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.source.wrapper == PostgresWrapper:
            import psycopg2
            connection = psycopg2.connect(host=self.source.host, port=int(self.source.port),
                                          user=self.source.username, password=self.source.password,
                                          database=self.source.database)
            connection.autocommit = self.source.autocommit or AUTO_COMMIT
        elif self.source.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")
        self.connection = connection
        return connection

    def __enter__(self):
        self._build_connection_mysql()
        self.client = self.source.wrapper(connection=self.connection)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        """
        Perform clean up tasks like delete tmp file/bucket/table/database
        :param exception_type:
        :param exception_value:
        :param traceback:
        :return:
        """
        if hasattr(self.connection, 'close'):
            self.connection.close()

    def __iter__(self):
        for records, schema in self.client.read(self.query, batch=self.source.batch):
            if self.schema is None:
                self.schema = schema
            for row in records:
                yield self.client.row_as_dict(row, schema)


class SQLSouceInput(object):
    def __init__(self, host=None, port=None, username=None, password=None,
                 database=None, table=None, query=None, sql_url=None, sql_url_auth_header=None,
                 validate=False, coder=None, batch=READ_BATCH, autocommit=AUTO_COMMIT, wrapper=MySQLWrapper, schema_only=False, *args, **kwargs):
        """

        :param host: db host ip address or domain
        :param port: db port 
        :param username: db username
        :param password: db password
        :param database: db connecting database
        :param table: table to fetch data, all data will be fetched in cursor
        :param query: query sting to fetch. either query or table can be passed
        :param sql_url: url of sql file to download and use the sql url as query
        :param sql_url_auth_header: auth header to download from sql_url, should be json string, which will be decoded at calling time, default to no header
        :param validate: validation ? not used as of now
        :param coder: default coder to use
        :param batch: size of match to read the records default to READ_BATCH, not used
        :param autocommit: connection autocommit
        :param wrapper: which wrapper to use, mysql or postgres
        :param schema_only: return schema or data
        """

        self.database = database
        self.validate = validate
        self.coder = coder or RowAsDictJsonCoder()

        # connection
        self.table = table
        self.query = query
        self.sql_url = sql_url
        self.sql_url_auth_header = sql_url_auth_header
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.batch = batch
        self.autocommit = autocommit
        if wrapper in [BaseWrapper, MySQLWrapper, MSSQLWrapper, PostgresWrapper]:
            self.wrapper = wrapper
        else:
            raise ExceptionInvalidWrapper("Wrapper can be [BaseWrapper, MySQLWrapper, MSSQLWrapper,  PostgresWrapper]")

        self._connection = None
        self._client = None
        self.schema = None
        self.schema_only = schema_only

        self.runtime_params = ['host', 'port', 'username', 'password', 'database',
                               'table', 'query', 'sql_url', 'sql_url_auth_header',
                               'batch', 'schema_only']

    @staticmethod
    def _build_value(source, keys):
        for key in keys:
            setattr(source, key, SQLSource.get_value(getattr(source, key, None)))


class ReadFromSQL(beam.PTransform):
    def __init__(self, *args, **kwargs):
        self.source = SQLSouceInput(*args, **kwargs)
        logging.info(f"from inside the readFromSQL class, source: {self.source.wrapper}")
        self.args = args
        self.kwargs = kwargs

    def expand(self, pcoll):
        "now expanding!!!"
        return (pcoll.pipeline
                | 'UserQuery' >> beam.Create([1])
                | 'SplitQuery' >> beam.ParDo(PaginateQueryDoFn(*self.args, **self.kwargs))
                | "reshuffle" >> Reshuffle()
                | 'Read' >> beam.ParDo(SQLSourceDoFn(*self.args, **self.kwargs))
                )


class PaginateQueryDoFn(beam.DoFn):
        def __init__(self, *args, **kwargs):
            self.args = args
            logging.info(f"pagination query do fn wrapper:{kwargs['wrapper']}")
            
            self.kwargs = kwargs

        def process(self, query, *args):
            
            source = SQLSource(*self.args, **self.kwargs)
            SQLSouceInput._build_value(source, source.runtime_params)
            logging.info(f"we're in the process method now; source.client: {source.client}")
            if query != 1:
                source.query = query
            else:
                source.query = source.query.strip(";")
            query = source.query
            row_count_query = "select count(1) as row_count from ({}) as subq".format(query)
            row_count = 0
            queries = []
            try:
                batch = source.batch
                for records, schema in source.client.read(row_count_query, batch=batch):
                    row_count = records[0][0]
                offsets = list(range(0, row_count, batch))
                for offset in offsets:
                    paginated_query, status = source.client.paginated_query(query, batch, offset)
                    queries.append(paginated_query)
                    logging.debug(("paginated query", paginated_query))
                    if not status:
                        break
            except Exception as ex:
                logging.error(ex)
                queries.append(query)
            return list(set(queries))

        @staticmethod
        def paginated_query(query, limit, offset=0):
            query = query.strip(";")
            if " limit " in query.lower():
                query = "SELECT * from ({query}) as sbq LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset)
                return query, True
                # return query, False
            else:
                query = query.strip(";")
                return "{query} LIMIT {limit} OFFSET {offset}".format(query=query, limit=limit, offset=offset), True


class SQLSourceDoFn(beam.DoFn):
    """
    This needs to be called as beam.io.Read(SQLSource(*arguments))

    TODO://
    1. To accept dsn or connection string

    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    @property
    def client(self):
        """
        Create connection object with mysql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """

        if self.source._connection is not None and hasattr(self._connection, 'close'):
            self._client = self.source.wrapper(connection=self._connection)
            return self._client

        self._connection = self._create_connection()
        self._client = self.source.wrapper(connection=self._connection)
        return self._client

    def _create_connection(self):
        if self.source.wrapper == MySQLWrapper:
            import pymysql
            _connection = pymysql.connect(host=self.source.host, port=int(self.source.port),
                                          user=self.source.username, password=self.source.password,
                                          database=self.source.database,
                                          connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.source.wrapper == PostgresWrapper:
            import psycopg2
            _connection = psycopg2.connect(host=self.source.host, port=int(self.source.port),
                                           user=self.source.username, password=self.source.password,
                                           database=self.source.database)
            _connection.autocommit = self.source.autocommit or AUTO_COMMIT
        elif self.source.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            _connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")

        return _connection

    def process(self, query, *args, **kwargs):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        source = SQLSource(*self.args, **self.kwargs)
        SQLSouceInput._build_value(source, source.runtime_params)
        self.source = source
        # records_schema = source.client.read(query)
        for records, schema in source.client.read(query):
            for row in records:
                
                yield source.client.row_as_dict(row, schema)

class SQLSource(SQLSouceInput, beam.io.iobase.BoundedSource):
    """
    This needs to be called as beam.io.Read(SQLSource(*arguments))

    TODO://
    1. To accept dsn or connection string

    """

    @property
    def client(self):
        """
        Create connection object with mysql or postgre based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        self._build_value(self.runtime_params)
        if self._connection is not None and hasattr(self._connection, 'close'):
            self._client = self.wrapper(connection=self._connection)
            return self._client

        self._connection = self._create_connection()
        self._client = self.wrapper(connection=self._connection)
        return self._client

    def _create_connection(self):
        if self.wrapper == MySQLWrapper:
            import pymysql
            _connection = pymysql.connect(host=self.host, port=int(self.port),
                                          user=self.username, password=self.password,
                                          database=self.database,
                                          connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.wrapper == PostgresWrapper:
            import psycopg2
            _connection = psycopg2.connect(host=self.host, port=int(self.port),
                                           user=self.username, password=self.password,
                                           database=self.database)
            _connection.autocommit = self.autocommit or AUTO_COMMIT
        elif self.wrapper == MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            _connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.host+';PORT='+ str(int(self.port)) +
                                        ';DATABASE='+self.database+';UID='+
                                        self.username+';PWD='+self.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")

        return _connection

    def estimate_size(self):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.estimate_size`"""
        return self.client.count(self.query)

    def get_range_tracker(self, start_position, stop_position):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.get_range_tracker`"""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Use an unsplittable range tracker. This means that a collection can
        # only be read sequentially for now.
        range_tracker = beam.io.range_trackers.OffsetRangeTracker(start_position, stop_position)
        range_tracker = beam.io.range_trackers.UnsplittableRangeTracker(range_tracker)

        return range_tracker

    def read(self, range_tracker):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.read`"""
        self._build_value(self.runtime_params)

        if self.table is not None:
            self.query = """SELECT * FROM {}""".format(self.table)
        elif self.query is not None:
            self.query = self.query
        elif self.sql_url is not None:
            self.query = self.download_sql(self.sql_url, self.sql_url_auth_header)
        else:
            raise ValueError("Source must have either a table or query or sql_url")

        if self.schema_only:
            if not self.table:
                raise Exception("table argument is required for schema")
            self.query = "DESCRIBE {}".format(self.table)

        for records, schema in self.client.read(self.query, batch=self.batch):
            print("SQLSource records: ", records)
            if self.schema is None:
                self.schema = schema

            if self.schema_only:
                yield records
                break

            for row in records:
                yield self.client.row_as_dict(row, schema)

    def _build_value(self, keys):
        for key in keys:
            setattr(self, key, self.get_value(getattr(self, key, None)))

    def _validate(self):
        if self.table is not None and self.query is not None:
            raise ValueError('Both a table and a query were specified.'
                             ' Please specify only one of these.')
        elif self.table is None and self.query is None:
            raise ValueError('A table or a query must be specified')
        elif self.table is not None:
            self.table = self.table
            self.query = None
        else:
            self.query = self.query
            self.table = None

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Implements :class:`~apache_beam.io.iobase.BoundedSource.split`
        This function will currently not be called, because the range tracker
        is unsplittable
        """
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = beam.io.range_trackers.OffsetRangeTracker.OFFSET_INFINITY

        # Because the source is unsplittable (for now), only a single source is
        # returned.
        yield beam.io.iobase.SourceBundle(
            weight=1,
            source=self,
            start_position=start_position,
            stop_position=stop_position)

    @staticmethod
    def get_value(value_obj):
        """
        Extract the value from value_obj, if value object is a runtime value provider
        :param value_obj:
        :return:
        """
        if callable(value_obj):
            return value_obj()
        if hasattr(value_obj, 'get'):
            return value_obj.get()
        elif hasattr(value_obj, 'value'):
            return value_obj.value
        else:
            return value_obj

    @staticmethod
    def download_sql(url, auth_headers):
        try:
            headers = json.loads(auth_headers)
        except Exception as ex:
            logging.debug("Could not json.loads the auth headers, {}, exception {}".format(auth_headers, ex))
            headers = None
        if os.path.exists(url) and os.path.isfile(url):
            with open(url, 'r') as fobj:
                return fobj.read()
        else:
            logging.info("Downloading form {}".format(url))
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                query = res.text
                logging.debug(("Downloaded query ", query))
                return query
            else:
                raise Exception("Could not successfully download data from {}, text, {}, status: {}".format(url, res.text, res.status_code))


class SQLWriter(beam.PTransform):

    def __init__(self,
                 host,
                 port,
                 username,
                 password,
                 database,
                 table,
                 schema=None,
                 create_disposition=SQLDisposition.CREATE_NEVER,
                 write_disposition=SQLDisposition.WRITE_APPEND,
                 batch_size=WRITE_BATCH,
                 test_client=None,
                 insert_retry_strategy=None,
                 validate=True,
                 coder=None,
                 autocommit=AUTO_COMMIT,
                 wrapper=MySQLWrapper):

        """
        TODO://
        1. use create disposition and write disposition
        2. user batch_size as input, currently default bundle size is used


        :param host:
        :param port:
        :param username:
        :param password:
        :param database:
        :param table:
        :param schema: not used
        :param create_disposition: not used
        :param write_disposition: not used
        :param batch_size: not used
        :param test_client:
        :param insert_retry_strategy:
        :param validate:
        :param coder:
        :param autocommit:
        :param wrapper:
        """
        self.create_disposition = SQLDisposition.validate_create(
            create_disposition)
        self.write_disposition = SQLDisposition.validate_write(
            write_disposition)
        self.schema = SQLWriter.get_dict_table_schema(schema)
        self.batch_size = batch_size
        self.test_client = test_client

        self.insert_retry_strategy = insert_retry_strategy
        self._validate = validate

        # connection
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if wrapper in [MySQLWrapper, MSSQLWrapper, PostgresWrapper]:
            self.wrapper = wrapper
        else:
            raise ExceptionInvalidWrapper("Wrapper can be in [MySQLWrapper, MSSQLWrapper, PostgresWrapper]")

        self.table = table
        self.database = database
        self.autocommit = autocommit
        self.coder = coder

    @staticmethod
    def get_table_schema_from_string(schema):
        table_schema = TableSchema()
        schema_list = [s.strip() for s in schema.split(',')]
        for field_and_type in schema_list:
            field_name, field_type = field_and_type.split(':')
            field_schema = TableFieldSchema()
            field_schema.name = field_name
            field_schema.type = field_type
            field_schema.mode = 'NULLABLE'
            table_schema.fields.append(field_schema)
        return table_schema

    @staticmethod
    def table_schema_to_dict(table_schema):
        """Create a dictionary representation of table schema for serialization
        """
        def get_table_field(field_spec):
            """Create a dictionary representation of a table field
            """
            result = dict()
            result['name'] = field_spec.name
            result['type'] = field_spec.type
            result['mode'] = getattr(field_spec, 'mode', 'NULLABLE')
            if hasattr(field_spec, 'description') and field_spec.description is not None:
                result['description'] = field_spec.description
            if hasattr(field_spec, 'fields') and field_spec.fields:
                result['fields'] = [get_table_field(f) for f in field_spec.fields]
            return result

        if not isinstance(table_schema, TableSchema):
            raise ValueError("Table schema must be of the type sql.TableSchema")
        schema = {'fields': []}
        for field in table_schema.fields:
            schema['fields'].append(get_table_field(field))
        return schema

    @staticmethod
    def get_dict_table_schema(schema):
        if isinstance(schema, (dict, value_provider.ValueProvider)) or callable(schema) or schema is None:
            return schema
        elif isinstance(schema, (str, )):
            table_schema = SQLWriter.get_table_schema_from_string(schema)
            return SQLWriter.table_schema_to_dict(table_schema)
        elif isinstance(schema, TableSchema):
            return SQLWriter.table_schema_to_dict(schema)
        else:
            raise TypeError('Unexpected schema argument: %s.' % schema)

    def _build_connection_mysql(self):
        """
        Create connection object with mysql or postgres based on the wrapper passed

        TODO://
        1. Make connection based on dsn or connection string

        :return:
        """
        # if self.connection is not None and hasattr(self.connection, 'close'):
        #     return self.connection

        if self.wrapper == MySQLWrapper:
            import pymysql
            connection = pymysql.connect(host=self.host, port=int(self.port),
                                         user=self.username, password=self.password,
                                         database=self.database,
                                         connect_timeout=CONNECTION_INIT_TIMEOUT, autocommit=AUTO_COMMIT)
        elif self.wrapper == PostgresWrapper:
            import psycopg2
            connection = psycopg2.connect(host=self.host, port=int(self.port),
                                          user=self.username, password=self.password,
                                          database=self.database)
            connection.autocommit = self.autocommit or AUTO_COMMIT
        elif self.wrapper ==MSSQLWrapper:
            import pyodbc
            driver='{ODBC Driver 17 for SQL Server}'
            connection = pyodbc.connect('DRIVER='+driver+';SERVER='+
                                        self.source.host+';PORT='+ str(int(self.source.port)) +
                                        ';DATABASE='+self.source.database+';UID='+
                                        self.source.username+';PWD='+self.source.password)

        else:
            raise ExceptionInvalidWrapper("Invalid wrapper passed")
        self.connection = connection
        return connection

    def expand(self, pcoll):
        p = pcoll.pipeline

        return (pcoll
                | beam.ParDo(SQLWriteDoFn(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    username=self.username,
                    password=self.password,
                    destination=self.table,
                    batch_size=self.batch_size,
                    autocommit=self.autocommit,
                    wrapper=self.wrapper,
                    schema=self.schema,
                    create_disposition=self.create_disposition,
                    write_disposition=self.write_disposition,
                    validate=self._validate))
                )

    def display_data(self):
        res = {}
        if self.table is not None:
            res['table'] = DisplayDataItem(self.table, label='Table')
        return res