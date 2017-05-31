import logging
import sys
import subprocess
from tempfile import NamedTemporaryFile

__version = 2.3


class HiveToVWInput(object):
    """
    Authors:
        (Primary) Micahel Musson michael.musson@gmail.com
        (Secondary) Hamel Husain hamel.husain@gmail.com

    Convience function that transforms a hive table into a format that can be
    ingested directly by Vowpal Wabbit(VW) [1].  VW utilizes out of core
    learning (online) which allows you to build predictive models very fast on
    large amounts of data with an extremely low memory footprint.

    Attributes
    ----------
    src_table : string
        hive table in the form of namespace.table_name that contains the training
        data for vw

    dst_table : string
        hive table you wish to create, without the namespace as this table is
        created in the tmp namespace by default.  This table  will contain the
        result of the data transformation. WARNING: This table will
        be dropped then recreated!
        example: 'vw_data'

    label_column : string
        column in src_table that corresponds to the label, or the target
        variable your are trying to predict for classification or regression.

    tag_column : string
        column in src_table that uniquely identifies the unit of observation
        you are trying to predict.  This field is useful for joining predictions
        back onto your original dataset.  For example, if you are trying to
        predict behavior of users this might be 'user_id'.  VW will return the
        tag along with the prediction when making predictions.

    limit : int or None
        Useful for testing purposes.  Specifying an integer will limit the
        observations from src_table by using a LIMIT statement in SQL.  You
        might want to use this to see if data is being materialized in the
        way you expect.

    filter_sql : string
        This is arbitrary sql you want to add to the end of the where clause
        that you would typically want to use for sampling.

    excludes : list of strings
        List of columns in src_table that are not features, labels, or tags.  If
        a column is not specified as a tag_column or a label_column it will
        automatically be considered as a feature in your model unless it is
        included in this list.

    custom_namespaces : dict of column names to namespaces
        In VW there a concept of namespaces, where you can group columns into
        groups.  These groups can be used for generating interaction terms.
        For more background on namespaces see VW documentation.

    hive_host : string
        The hostname for the Hive connection. The default is 'localhost'.

    hive_port : numeric
        The port for the Hive connection. The default is 10000.


    Example
    -------
    >>> import HiveToVW

    # Create custom namespace mappings for columns
    # Anything not given a namespace will be mapped to "other"

    >>> CUSTOM_NS = { 'sibsp': 'family',
                       'parch': 'family',
                       'sex': 'demographic',
                       'age': 'demographic'}

    # Exclude these columns from the final result
    >>> EXCLUDE_COLS = ['Name']

    # Instantiate the HivetoVW Converter

    >>> h2vw = HiveToVW.HiveToVWInput(src_table = 'tmp.titanic',
                              dst_table = 'titanic_vw_output',
                              label_column = 'survived',
                              tag_column = 'passengerid',
                              excludes = EXCLUDE_COLS,
                              custom_namespaces=CUSTOM_NS,
                              hive_metastore_host='localhost',
                              hive_metastore_port=3621)

    # Run the converter
    >>> htvw_val.run()

    [1] http://hunch.net/~vw/

    """
    def __init__(self,
                 src_table,
                 dst_table,
                 label_column,
                 tag_column,
                 limit=None,
                 filter_sql=None,
                 excludes=None,
                 custom_namespaces=None,

                 hive_metastore_host='localhost',
                 hive_metastore_port=3621,
                ):
        if len(src_table.split('.')) != 2:
            raise TypeError('src_table should be of the format namespace.table_name')
        else:
            (self.src_db, self.src_table_name) = src_table.split('.')
        if len(dst_table.split('.')) != 1:
            raise TypeError("""
                dst_table NOT contain the namespace. It will get created
                in the "tmp" namespace""")
        else:
            (self.dst_db, self.dst_table_name) = ('tmp', dst_table)
        self.src_table = src_table
        self.dst_table = dst_table
        self.filter_sql = filter_sql
        self.excludes = excludes or []
        self.label_column = label_column
        self.limit = limit
        self.tag_column = tag_column
        self.custom_namespaces = custom_namespaces or {}

        # Initialize the hive metastore connection
        self.metastore_conn = HiveMetastoreHook(hive_metastore_host, hive_metastore_port)

        # Initialize the hive cli connection
        self.hive_conn = HiveCliHook()

        # Build the namespace groups
        self.nsgroups = self.get_ns_groups(self.src_table_name, self.src_db)

    def get_ns_groups(self, table_name, db):
        """
        Given a hive table, return a dictionary of columns in each namespace
        """
        # Fetch the schema for this table
        cols = self.metastore_conn.get_table_schema(table_name, db)
        nsgroups = {}
        # Build the mapping from namespace to column list
        for c in cols:
            cname = c['name']
            ctype = c['type']
            if cname in self.excludes + [self.label_column, self.tag_column]:
                next
            elif cname in self.custom_namespaces:
                nsgroups[self.custom_namespaces[cname]] = \
                    nsgroups.get(self.custom_namespaces[cname], []) + [(cname, ctype)]
            elif len(cname.split('__')) > 2:
                ns = cname.split('__')[1]
                nsgroups[ns] = nsgroups.get(ns, []) + [(cname, ctype)]
            else:
                nsgroups['other'] = nsgroups.get('other', []) + [(cname, ctype)]

        return nsgroups

    def __col_sql(self, colname, coltype):
        """ Return the properly formatted SQL for the particular column name and type"""
        if coltype == 'double':
            return "CASE WHEN COALESCE(CAST({col} AS DOUBLE),0.0) = 0.0 THEN '' ELSE CONCAT('{col}:', PRINTF('%.2f', {col}), ' ') END".format(col=colname)
        elif coltype in ['bigint', 'int']:
            return "CASE WHEN COALESCE({col},0) = 0 THEN '' ELSE CONCAT('{col}:', PRINTF('%d', {col}), ' ') END".format(col=colname)
        elif coltype in ['boolean']:
            return "CASE WHEN COALESCE(CAST({col} as int),0) = 0 THEN '' ELSE CONCAT('{col}:', PRINTF('%d', CAST({col} as int)), ' ') END".format(col=colname)
        elif coltype == 'string':
            # Clean up all the spurious characters
            #return "CASE WHEN COALESCE({col},'') = '' THEN '' ELSE CONCAT({col}, ' ') END".format(col=colname)
            return "CASE WHEN COALESCE({col},'') = '' THEN '' ELSE CONCAT(REGEXP_REPLACE({col}, '[\\\\x00-\\\\x2a|\\\\x2c|\\\\x2f|\\\\x3a-\\\\x40|\\\\x5b-\\\\x5e|\\\\x60|\\\\x7b-\\\\x7f]', ''), ' ') END".format(col=colname)
        else:
            raise NotImplementedError

    def __col_ns(self, namespaces):
        """ Return the combined SQL for each particular VW namespace
            This will be of the format "|namespace FeatureA:123 FeatureB:0.0 SomeText"
        """
        featureset = []
        for ns, cols in namespaces.iteritems():
            features = "\n,".join([self.__col_sql(c[0], c[1]) for c in cols ])
            featureset.append("\n'|{ns} '\n, {fs}".format(ns=ns, fs=features))
        return ','.join(featureset)

    def __assemble_sql(self, cols, label_col, tag_col, src_table):
        s = "AND {}".format(self.filter_sql) if self.filter_sql else ""
        sql = """
            SELECT CONCAT(PRINTF('%.4f', COALESCE({label}, 0.0)), ' 1.0 ', {tag_col}, {cols} )
            FROM {table_name}
            WHERE ds > ''
                AND {label} IS NOT NULL
                {sample}
            """.format(label=label_col,
                       cols=self.__col_ns(cols),
                       tag_col = tag_col,
                       table_name=src_table,
                       sample=s
                      )
        return sql

    def create_dest_table(self):
        sql = """
        DROP TABLE IF EXISTS {ns}.{table};
        CREATE EXTERNAL TABLE {ns}.{table} (
            input_line    STRING
        )
        STORED AS TEXTFILE
        ;
        """.format(ns=self.dst_db, table=self.dst_table_name)
        logging.info(sql)
        try:
            output = self.hive_conn.run_cli(sql)
        except:
            logging.error(output)

    def gen_sql(self):
        select_sql = self.__assemble_sql(self.nsgroups,
                                         self.label_column,
                                         self.tag_column,
                                         self.src_table)

        limit_sql = "LIMIT {}".format(self.limit) if self.limit else ""

        insert_sql = """
            SET hive.exec.compress.output=false;
            SET mapred.reduce.tasks=10;
            INSERT OVERWRITE TABLE {dst_db}.{dst_table}
            {select_sql}
            {limit_sql}
            ;
        """.format(dst_db=self.dst_db,
                   dst_table=self.dst_table_name,
                   select_sql=select_sql,
                   limit_sql=limit_sql)

        return(insert_sql)

    def run(self):
        """ Compile the SQL and run it on Hive! """
        self.create_dest_table()
        finalsql = self.gen_sql()
        logging.info(finalsql)
        output = self.hive_conn.run_cli(finalsql)
        logging.info(output)
        dest_loc = self.metastore_conn.get_table_location(self.dst_table_name, self.dst_db)
        logging.info("You should be able to find your files at {}".format(dest_loc))


class HiveCliHook(object):

    """Simple wrapper around the hive CLI. Gratuitously based off
    the Airflow HiveCliHook.
    """

    def __init__(self, hive_cli_params=None):
        self.hive_cli_params = hive_cli_params or ''

    def _prepare_cli_cmd(self):
        """
        This function creates the command list from available information
        """
        hive_bin = 'hive'
        cmd_extra = []
        hive_params_list = self.hive_cli_params.split()
        return [hive_bin] + cmd_extra + hive_params_list

    def _prepare_hiveconf(self, d):
        """
        This function prepares a list of hiveconf params
        from a dictionary of key value pairs.

        :param d:
        :type d: dict

        >>> hh = HiveCliHook()
        >>> hive_conf = {"hive.exec.dynamic.partition": "true",
        ... "hive.exec.dynamic.partition.mode": "nonstrict"}
        >>> hh._prepare_hiveconf(hive_conf)
        ["-hiveconf", "hive.exec.dynamic.partition=true",\
 "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]
        """
        if not d:
            return []
        return as_flattened_list(
            itertools.izip(
                ["-hiveconf"] * len(d),
                ["{}={}".format(k, v) for k, v in d.items()]
                )
            )

    def run_cli(self, hql, schema=None, verbose=True, hive_conf=None):
        """
        Run an hql statement using the hive cli. If hive_conf is specified
        it should be a dict and the entries will be set as key/value pairs
        in HiveConf


        :param hive_conf: if specified these key value pairs will be passed
            to hive as ``-hiveconf "key"="value"``. Note that they will be
            passed after the ``hive_cli_params`` and thus will override
            whatever values are specified in the database.
        :type hive_conf: dict

        >>> hh = HiveCliHook()
        >>> result = hh.run_cli("USE mydb;")
        >>> ("OK" in result)
        True
        """
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())
        tmp_dir = '/tmp/'
        with NamedTemporaryFile(dir=tmp_dir) as f:
            f.write(hql.encode('UTF-8'))
            f.flush()
            hive_cmd = self._prepare_cli_cmd()
            hive_conf_params = self._prepare_hiveconf(hive_conf)
            hive_cmd.extend(hive_conf_params)
            hive_cmd.extend(['-f', f.name])

            if verbose:
                logging.info(" ".join(hive_cmd))
            sp = subprocess.Popen(
                hive_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=tmp_dir)
            self.sp = sp
            stdout = ''
            while True:
                line = sp.stdout.readline()
                if not line:
                    break
                stdout += line.decode('UTF-8')
                if verbose:
                    logging.info(line.decode('UTF-8').strip())
            sp.wait()
            if sp.returncode:
                logging.error("Problem when running:")
                logging.error(hql)
                logging.error("Command returned {}".format(sp.returncode))
            return stdout


class HiveMetastoreHook(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.metastore = self.get_metastore_client()

    def get_metastore_client(self):
        """Returns a Hive thrift client."""
        from thrift.transport import TSocket, TTransport
        from thrift.protocol import TBinaryProtocol
        from hive_service import ThriftHive
        socket = TSocket.TSocket(self.host, self.port)
        transport = TTransport.TBufferedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return ThriftHive.Client(protocol)

    def get_table(self, table_name, db='default'):
        """Get a metastore table object"""
        self.metastore._oprot.trans.open()
        if db == 'default' and '.' in table_name:
            db, table_name = table_name.split('.')[:2]
        table = self.metastore.get_table(dbname=db, tbl_name=table_name)
        self.metastore._oprot.trans.close()
        return table

    def get_table_location(self, table_name, db='default'):
        """Get the HDFS location of a table"""
        return self.get_table(table_name, db).sd.location

    def get_table_schema(self, table_name, db='default'):
        """Get the schema of a table"""
        schema = [{'name': c.name, 'type': c.type, 'comment': c.comment} for c in
                  self.get_table(table_name=table_name, db=db).sd.cols]
        return schema
