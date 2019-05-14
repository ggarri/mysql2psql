

__author__ = 'ggarrido'

import subprocess
import csv
from collections import OrderedDict
import pymysql.cursors
from pymysql.converters import decoders, through


RED, GREEN, YELLOW, NC = '\033[0;31m', '\033[0;32m', '\033[0;33m', '\033[0m'

class MysqlParser():
    """
    Generate MySql schema on JSON format given a Mysql Connectior, defined DbName and Tables
    """

    information_schema = 'information_schema'

    def __init__(self, conn_params, information_schema=None):
        # replace date, datetime, timstamp decoders so they're not decoded into date objects
        custom_decoders = decoders.copy()
        custom_decoders[7] = through
        custom_decoders[10] = through
        custom_decoders[11] = through
        custom_decoders[12] = through

        conn_params['conv'] = custom_decoders
        self.connection = pymysql.connect(**conn_params)
        self.cursor = self.connection.cursor(pymysql.cursors.SSCursor)
        self.skip_pre_sql = False
        if information_schema is not None: self.information_schema = information_schema

    def close(self):
        self.cursor.close()
        self.connection.close()

    def set_skip_pre_sql(self, skip=False):
        self.skip_pre_sql = skip

    def get_schema(self, db_name, tables=[]):
        """
        Iterate over every table from give DbName, filter by passed tables array
        :param db_name: DbName to generate schema from
        :param tables: Filtered tables
        :return: Json schema
        """
        mysql_schema = {
            'tables': self._get_db_tables_schema(db_name, tables)
        }

        return mysql_schema

    @staticmethod
    def mysqldump_data(config, db_name, tables, output_path):
        cmd = "mysqldump -h%s -u%s %s %s --compatible=postgresql --no-create-info --compact  " \
              "--extended-insert=FALSE --default-character-set=utf8 --complete-insert %s %s > %s "\
              % (config['host'], config['user'], ('-p'+config['password'] if 'password' in config else ''),
                 ('-P'+str(config['port']) if 'port' in config else ''), db_name, ' '.join(tables), output_path)
        subprocess.call(cmd, shell=True)

    @staticmethod
    def mysqldump_tables(config, db_name, tables, output_path):
        cmd = "mysqldump -h%s -u%s %s %s --compatible=postgresql --no-data --compact %s %s > %s "\
              % (config['host'], config['user'], ('-p'+config['password'] if 'password' in config else ''),
                 ('-P'+str(config['port']) if 'port' in config else ''), db_name, ' '.join(tables), output_path)
        subprocess.call(cmd, shell=True)


    def run_pre_sql(self, db_name, table, table_attrs, schema_changes):
        # def sql_fix_violates_fk(col_name, col_reference):
        #     reg = re.compile(ur'^[\\]?["]?(\w*)[\\]?["]?\s*\((\w*)\)$')
        #     reg_match = re.search(reg, col_reference)
        #     ref_table, ref_col = reg_match.group(1), reg_match.group(2)
        #     # Getting original table name from new schema
        #     ref_table_attr = [  cg_table_name for cg_table_name, cg_table_attrs in schema_changes['tables'].iteritems() \
        #             if cg_table_name == ref_table or ('name' in cg_table_attrs and cg_table_attrs['name'] ==  ref_table)
        #     ]
        #
        #     if len(ref_table_attr) != 0: ref_table = ref_table_attr[0]
        #
        #     return """UPDATE IGNORE `%s` AS x
        #       LEFT JOIN `%s` y ON (y.%s = x.%s)
        #       SET x.%s = 0 WHERE y.%s IS NULL""" % \
        #     (table, ref_table, ref_col, col_name, col_name, ref_col)
        if self.skip_pre_sql: return 0

        def get_utc_pre_sql(tableName, columns):
            if tableName == 'channel_log_resa': return []
            return ["UPDATE IGNORE `%s` SET `%s` = `%s` - INTERVAL 2 HOUR WHERE `%s` IS NOT NULL" % (tableName, col_name, col_name, col_name) \
                        for col_name, col_attr in columns.iteritems() \
                        if col_attr['type'] == 'datetime' or col_attr['type'] == 'timestamp']

        if not '_PRE_SQL_' in table_attrs: table_attrs['_PRE_SQL_'] = []

        table_attrs['_PRE_SQL_'] += get_utc_pre_sql(table, table_attrs['columns'])
        for pre_sql in table_attrs['_PRE_SQL_']:
            try:
                res = self.cursor.execute(pre_sql)
                if res is not None and res != 0: print "%s (Affected rows %d)" % (pre_sql, res)
            except Exception, e: print RED + ("ERROR: %s\n MSG: %s" % (pre_sql, str(e))) + NC


    def get_table_raw_data(self, db_name, table, cols, table_attrs, schema_changes, table_temp_filename):
        """
        Return raw data from passed table cols, applying conversion rules
        :param table:
        :param cols:
        :param export_rules:
        :return:
        """

        def append_join(idx, ref_alias, join_attrs):
            alias = 'j'+str(idx)
            return ' INNER JOIN %s.%s AS %s ON (%s.%s = %s.%s) ' % (
                db_name,
                join_attrs['table'],
                alias,
                ref_alias,
                join_attrs['col'],
                alias,
                join_attrs['col_ref']
            )

        # Generate SELECT SQL to export raw data
        sql, res = '', None
        alias = 't'; sql = "SELECT t.`%s` FROM `%s`.%s as %s" % ('`, t.`'.join(cols), db_name, table, alias)
        if '_JOIN_' in table_attrs:
            if not isinstance(table_attrs, list): table_attrs['_JOIN_'] = [table_attrs['_JOIN_']]
            for idx, join_attrs in enumerate(table_attrs['_JOIN_']):
                sql += append_join(idx, alias, join_attrs)

        if '_WHERE_' in table_attrs:
            sql += ' WHERE ' + table_attrs['_WHERE_']

        if len(sql) > 0:

            with_header=False
            delimiter='|'
            quotechar="'"
#            quoting=csv.QUOTE_NONNUMERIC
            quoting=csv.QUOTE_NONE
            escapechar='\\'
            con_sscursor=True
            self.cursor.execute(sql)
            cabecera= [campo[0] for campo in self.cursor.description]
            ofile = open(table_temp_filename,'wb')
            csv_writer = csv.writer(ofile, delimiter=delimiter, quotechar=quotechar,quoting=quoting,escapechar=escapechar)
            if with_header:
                csv_writer.writerow(cabecera)
            if con_sscursor:
                 while True:
                    x = self.cursor.fetchone()
                    if x:
                        csv_writer.writerow(x)
                    else:
                        break
            else:
                for x in self.cursor.fetchall():
                    csv_writer.writerow(x)
            ofile.close()

        return res

    def _get_db_tables_schema(self, db_name, tables=[]):
        """
        Iterate over every table(filtered) and obtain information from information_schema
        :param db_name: DbName where table belongs
        :param tables: Tables to filter
        :return: Json with every table from given dbname
        """
        output = OrderedDict()
        sql = """
        SELECT
         T.table_name, T.engine, T.table_collation, T.auto_increment
        FROM
         %s.tables as T
        WHERE
         T.table_schema = '%s'
        """ % (self.information_schema, db_name)

        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        for table_info in res:
            # Due to an update in the PyMysql lib it return list instead of dict
            if isinstance(table_info, tuple):
                table_info = dict(zip(('table_name', 'engine', 'table_collation', 'auto_increment')
                                      , list(table_info)))
            if len(tables) > 0 and table_info['table_name'] not in tables:
                continue
            output[table_info['table_name']] = {
                'name': table_info['table_name'],
                'collation': table_info['table_collation'],
                'engine': table_info['engine'],
                'autoIncrement': table_info['auto_increment'],
                'columns': self._get_table_columns_schema(db_name, table_info['table_name']),
                'indexes': self._get_table_indexes_schema(db_name, table_info['table_name'])
            }

        return output

    def _get_table_columns_schema(self, db_name, table_name):
        """
        Iterate over every column from given DbName and Table
        :param db_name: DbName where columns belongs to
        :param table_name: Table where columns belongs to
        :return: Json with every column from given table
        """
        columns = OrderedDict()
        sql = """
        SELECT
         C.column_name, C.is_nullable, C.data_type, C.column_default, C.column_type,
         C.character_maximum_length as size, C.column_key as isPk, C.extra,
         CONCAT(K.referenced_table_name, '(',  referenced_column_name, ')') as reference,
         CONCAT_WS(',', C.numeric_precision, C.numeric_scale) as dsize
        FROM
         %s.columns C
        LEFT JOIN  %s.KEY_COLUMN_USAGE K ON (
            K.column_name = C.column_name and K.constraint_schema = C.table_schema and C.table_name = K.table_name
        )
        WHERE
         C.table_name = '%s'
         AND C.table_schema = '%s'
        """ % (self.information_schema, self.information_schema, table_name, db_name)

        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        for column_info in res:
            # Due to an update in the PyMysql lib it return list instead of dict
            if isinstance(column_info, tuple):
                column_info = dict(
                    zip(('column_name', 'is_nullable', 'data_type', 'column_default', 'column_type','size',
                         'isPk', 'extra', 'reference', 'dsize'),
                        list(column_info))
                )

            columns[column_info['column_name']] = {
                'name': column_info['column_name'],
                'type': column_info['data_type'],
                'nullable': column_info['is_nullable'] == 'YES',
                'size': column_info['size'] if column_info['data_type'] not in ['double', 'decimal'] else column_info['dsize'],
                'default': column_info['column_default'],
                'extra': column_info['extra'],
                'isPk': column_info['isPk'] == 'PRI',
                'fullType': column_info['column_type'],
                'reference': column_info['reference'],
            }

        return columns

    def _get_table_indexes_schema(self, db_name, table_name):
        """
        Iterate over every column from given DbName and Table
        :param db_name: DbName where columns belongs to
        :param table_name: Table where columns belongs to
        :return: Json with every column from given table
        """
        indexes = OrderedDict()
        sql = """
        SELECT table_name AS `table_name`,
               index_name AS `index_name`,
               GROUP_CONCAT(column_name ORDER BY seq_in_index) AS `columns`
        FROM %s.statistics S
        WHERE S.table_schema = '%s'
          AND S.table_name = '%s'
          AND S.index_name <> 'PRIMARY'
        GROUP BY 1,2;
        """ % (self.information_schema, db_name, table_name)

        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        for index_info in res:
            # Due to an update in the PyMysql lib it return list instead of dict
            if isinstance(index_info, tuple):
                index_info = dict(
                    zip(('table_name', 'index_name', 'columns'),
                        list(index_info))
                )

            indexes[index_info['index_name']] = {
                'name': index_info['index_name'],
                'columns': index_info['columns'].split(',')
            }

        return indexes


    def get_user_user_pass(self, opcode):
        sql = """
        SELECT cl.db_login as login,
        cl.db_pass as pass
        FROM base7_config.client_db as cl
        WHERE cl.identifier = '%s'
        """ % (opcode)

        self.cursor.execute(sql)
        res = self.cursor.fetchone()

        if res is None: return None, None
        return res[0], res[1]


    def get_all_databases(self, prefix):
        sql = """
        SELECT TABLE_SCHEMA as db_name
        FROM """+self.information_schema+""".tables
        WHERE TABLE_SCHEMA <> 'mysql'
	AND TABLE_SCHEMA <> 'sys'
	AND TABLE_SCHEMA <> 'performance_schema'
        AND TABLE_SCHEMA <> 'information_schema'"""

        if prefix and len(prefix)>0:
            sql += ' AND TABLE_SCHEMA LIKE \''+prefix+'%\''

        sql += ' GROUP BY TABLE_SCHEMA'

        self.cursor.execute(sql)
        res = self.cursor.fetchall()
        return [str(db[0]) for db in res]
