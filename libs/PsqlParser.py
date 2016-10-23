__author__ = 'ggarrido'

import os
import re
import json
import time
from RuleHandler import RuleHandler
from MysqlParser import MysqlParser
import dumperAuxFuncs
from decimal import Decimal

REGEX_TYPE = type(re.compile(''))
PGSQL_BLOCK = 1000

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

class PsqlParser():

    def __init__(self, cursor, conn):
        self.cur = cursor
        self.conn = conn
        self.raw_dump_rules = json.loads(open('./rules/mysql_raw_dump.json').read())
        self.rules = json.loads(open('./rules/mysql_to_psql.json').read())

    def close(self):
        self.cur.close()
        self.conn.close()

    def get_schema_from_mysql(self, mysql_schema):
        pq_schema = mysql_schema.copy()
        rule_handler = RuleHandler(None, {'table': self.rules['table'], 'column': self.rules['column']})
        return rule_handler.obtain_modified_schema(pq_schema)

    def generate_sql_user(self, mysql_parser, opcode, psql_users_path):
        output = open(psql_users_path, 'w')
        if opcode is not None and len(opcode) > 0 and opcode != 'empty':
            users_sql = self._get_sql_user(opcode, mysql_parser)
            output.write(users_sql)
        output.close()


    def generate_sql_schema(self, schema, schema_name, psql_tables_path):
        """
        Generate sql queries from given schema
        :param schema: Psql schema
        :return: Psql queries to generate tables
        """
        psql_tables = open(psql_tables_path, 'w')
        psql_tables.write("SET client_min_messages TO WARNING;\n")
        psql_tables.write("DROP SCHEMA IF EXISTS %s CASCADE;\n" % schema_name)
        psql_tables.write("CREATE SCHEMA IF NOT EXISTS %s;\n" % schema_name)
        psql_tables.write("SET SCHEMA '%s';\n" % schema_name)
        psql_tables.write("CREATE EXTENSION \"unaccent\";\n\n")

        for table_name, table_attr in schema['tables'].iteritems():
            psql_tables.write("\n-- CREATE TABLE %s \n %s \n %s \n" % (
                table_attr['name'], self._get_sql_drop_table(table_attr),
                self._get_sql_create_table(table_attr)
            ))

        psql_tables.close()

    # @deprecated
    def generate_dump_from_mysql_dump(self, schema_changes, schema_name, mysql_dump_path, psql_dump_path):
        """
        Read an mysql dump file and convert it into psql syntax
        :param schema_changes: changes applied into mysql schema
        :param schema_name: psql schema name
        :param mysql_dump_path: mysql source file
        :param psql_dump_path: desc file for dump
        """
        mysql_dump = open(mysql_dump_path, 'r')
        psql_dump = open(psql_dump_path, 'w')
        psql_dump.write(self._get_dump_initial_statements())
        psql_dump.write("\n\n")

        for i, line in enumerate(mysql_dump):
            insert_sql = self._convert_mysql_insert_to_psql(schema_changes, line)
            if insert_sql is not None:
                psql_dump.write(insert_sql)
                psql_dump.write("\n")

        psql_dump.write("\n\n")
        psql_dump.write(self._get_dump_final_statements())
        for skip in RuleHandler.get_skip_colums(schema_changes):
            psql_dump.write("ALTER TABLE \"%s\" DROP COLUMN IF EXISTS \"%s\";\n" % skip)

        mysql_dump.close()
        psql_dump.close()


    def generate_dump_from_raw(self, mysql_parser, db_name, pg_schema, schema_name, psql_dump_path, tables_path, schema_changes):
        """
        Obtain raw data from mysql connection and convert into INSERT INTOs
        :param mysql_parser
        :param schema:
        :param schema_name:
        :param psql_dump_path:
        :type mysql_parser: MysqlParser
        """
        psql_dump = open(psql_dump_path, 'w')

        pre_sql_tables = { table_name: pg_schema['tables'][table_name] \
                           for table_name in schema_changes['tables'].keys() \
                           if '_PRE_SQL_' in schema_changes['tables'][table_name] and table_name in pg_schema['tables']}

        for table_name, table_attrs in pre_sql_tables.iteritems():
            mysql_parser.run_pre_sql(db_name, table_name, table_attrs, schema_changes)

        for table_name, table_attrs in pg_schema['tables'].iteritems():
            print "Parsing table '%s' data...." % table_name
            table_name_to = table_attrs if not table_attrs.get('name', {}) else table_attrs['name']
            table_filename = os.path.join(tables_path, "%s.sql" % (table_name_to))
            table_dump = open(table_filename, 'w+')

            cols_from = [col_name for col_name, col_attr in table_attrs['columns'].iteritems()
                 if not col_attr.get('_SKIP_', False)]
            cols_to = [col_name if not col_attr.get('name', {}) else col_attr['name']
                       for col_name, col_attr in table_attrs['columns'].iteritems()
                 if not col_attr.get('_SKIP_', False)]

            start_time = time.time()
            rows = mysql_parser.get_table_raw_data(db_name, table_name, cols_from, table_attrs, schema_changes)
            table_raw_rules = self._get_table_raw_dump_rules(table_name, cols_from, table_attrs['columns'])
            sql_copy_data_template = ','.join(['%s' for x in range(0, len(cols_to))]) + '\n'
            columns = '", "'.join(cols_to)
            psql_dump.write("\copy \"%s\" (\"%s\") FROM '%s' WITH (FORMAT CSV, QUOTE '''', DELIMITER ',', NULL 'NULL');\n"
                % (table_name_to, columns, table_filename))

            for row_data in rows:
                row_data = list(row_data)
                self._apply_raw_dump_rules(row_data, table_raw_rules)
                csv_row_data = sql_copy_data_template % tuple(map(self._supaFilta, row_data))
                table_dump.write(csv_row_data)

            table_dump.close()
        psql_dump.close()

    def generate_psql_index_fk(self, schema, output_file):
        output = open(output_file, 'w')
        output.write("SET client_min_messages TO ERROR;\n")
        output.write("SET SCHEMA 'public';\n")

        output.write("\n\n")
        for table_name, table_attr in schema['tables'].iteritems():
            output.write(self._get_sql_sequence(table_attr))
            output.write(self._get_sql_fkeys(table_attr))
            output.write(self._get_sql_indexes(table_attr))

        output.close()


    def generate_psql_views(self, schema, schema_name_v1, schema_name_v2, psql_views_path):
        """
        Generate view to be able to query on old db schema trough new v2 db schema
        :param schema:
        :param schema_name_v1:
        :param schema_name_v2:
        :param psql_views_path:
        :return:
        """
        psql_views = open(psql_views_path, 'w')
        psql_views.write("SET client_min_messages TO ERROR;\n")
        psql_views.write("DROP SCHEMA IF EXISTS %s CASCADE;\n\n" % schema_name_v1)
        psql_views.write("CREATE SCHEMA IF NOT EXISTS %s;\n\n" % schema_name_v1)

        for table_name_v1, table_attr in schema['tables'].iteritems():
            table_name_v2 = table_attr['name']
            columns_pri, columns_ref, columns, columns_ignore = \
                PsqlParser._get_categorized_columns(table_attr['columns'])

            columns =  merge_dicts(columns_pri, columns_ref, columns)

            columns_v2 = [ '"'+col_attr['name']+'"' for col_name_v1, col_attr in columns.iteritems() ]
            columns_v2 += [ 'NULL' for col_name_v1, col_attr in columns_ignore.iteritems() ]

            columns_v1 = [ '"'+col_name_v1+'"' for col_name_v1, col_attr in columns.iteritems()]
            columns_v1 += [ '"'+col_name_v1+'"' for col_name_v1, col_attr in columns_ignore.iteritems() ]

            view_sql = ('CREATE VIEW %s (%s) AS \n SELECT %s FROM %s WITH CASCADED CHECK OPTION;\n\n' % (
                "%s.%s" % (schema_name_v1, table_name_v1),
                ', '.join(columns_v1),
                ', '.join(columns_v2),
                "%s.%s" % (schema_name_v2, table_name_v2)
            ))

            psql_views.write(view_sql + "\n")
        psql_views.close()


    def _get_table_raw_dump_rules(self, table_name, cols, attrs):
        tuple_to_check = []
        for rule_attr, rule_conds in self.raw_dump_rules.get('column', {}).iteritems():
            for rule_cond in rule_conds:
                tuple_to_check += [(col_key, attrs[col_name], rule_cond['method']) \
                    for col_key, col_name in enumerate(cols) \
                        if attrs[col_name].get(rule_attr, None) == rule_cond['value']
                            or (rule_cond['value'] == "notNone" and attrs[col_name].get(rule_attr, None) is not None)
                ]
        return tuple_to_check

    def _apply_raw_dump_rules(self, row_data, tuple_to_check):
        for col_key, col_attrs, rule_method in tuple_to_check:
            params = [row_data[col_key], col_attrs]
            row_data[col_key] = getattr(dumperAuxFuncs, rule_method)(*params)


    @staticmethod
    def _convert_mysql_insert_to_psql(schema_changes, line):
        """
        Convert mysql insert sql statement into psql one
        :param schema_changes:
        :param line:
        :return:
        """
        insert_regex = re.compile('^INSERT INTO "([\w\d]+)"([\w\W]+)VALUES([\w\W]+);$')
        try:
            line = line.decode("utf8").strip().replace(r"\\", "WUBWUBREALSLASHWUB").\
                replace(r"\'", "''").replace("WUBWUBREALSLASHWUB", r"\\").\
                replace("0000-00-00 00:00:00", "2000-01-01 00:00:00").\
                replace("0000-00-00", "2000-01-01")
        except:
            print "Can't decode value"
            print line
            return None

        # Grag table name from insert query and check if there is a new name for it
        table_name = insert_regex.match(line).group(1)
        orig_table_name = insert_regex.match(line).group(1)
        if RuleHandler.STR_SKIP == schema_changes.get('tables', {}).get(table_name, {}):
            return None
        if 'name' in schema_changes.get('tables', {}).get(table_name, {}):
            table_name = schema_changes['tables'][table_name]['name']

        # Grag columns names from insert query and check if there is a new name for them
        columns = re.findall('"([^"]*)"', insert_regex.match(line).group(2))
        for key, col in enumerate(columns):
            if 'name' in schema_changes.get('tables', {}).get(orig_table_name, {}).get('columns', {}).get(col, {}):
                columns[key] = schema_changes['tables'][orig_table_name]['columns'][col]['name']

        column_str = '("' + '", "'.join(columns) + '")'

        # Values to be inserted
        values = insert_regex.match(line).group(3)

        # Re-build insert query with new names
        insert_sql = "INSERT INTO \"%s\" %s VALUES %s;" % (table_name, column_str, values)
        return insert_sql.encode('utf8')


    @staticmethod
    def _create_rules(rules, node_rules, node_atrrs):
        """
        Generates list of rules from class general rules
        :param rules: Dict to allocate new rules
        :param node_rules: Global class rules defined on the node level
        :param node_atrrs: List of available attrs on that level
        :return:
        """
        for node_attr, node_value in node_atrrs.iteritems():
            if node_attr not in node_rules:
                continue
            for rule in node_rules[node_attr]:
                # if isinstance(rule['from'], REGEX_TYPE) and node_value.startswith('mediumtext'):
                if rule['from'] == node_value:
                    rules[node_attr] = rule['to']

    @staticmethod
    def _get_sql_drop_table(table_attr):
        """
        Generate drop database statement
        :param table_attr: table attrs
        :return: SQL statement for dropping
        """
        template = 'DROP TABLE IF EXISTS "%s" CASCADE;' % (table_attr['name'])
        return template


    def _get_sql_user(self, opcode, mysql_parser):
        db_name = 'client_'+opcode
        b7_user, b7_pass = mysql_parser.get_user_user_pass(opcode)

        if b7_user is None or b7_pass is None: return ""
        if len(b7_user) == 0 or b7_user == 'root': return ""
        b7_pass = b7_pass.replace('$', '\\0024')
        return """-- Adding PG User
        DO $$DECLARE r record;
        BEGIN
           IF NOT EXISTS (
              SELECT *
              FROM   pg_catalog.pg_user
              WHERE  usename = '%s') THEN

              CREATE USER %s WITH PASSWORD U&'%s';
           ELSE
              ALTER USER %s WITH PASSWORD U&'%s';
           END IF;
        END$$;
        ALTER DATABASE %s OWNER TO %s;
        GRANT CONNECT ON DATABASE %s TO %s;

        GRANT %s TO %s;

        GRANT USAGE ON SCHEMA %s TO %s;
        GRANT ALL ON ALL SEQUENCES IN SCHEMA %s TO %s;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s;
        GRANT USAGE ON SCHEMA %s TO %s;
        GRANT ALL ON ALL SEQUENCES IN SCHEMA %s TO %s;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s;

        ALTER DATABASE %s SET search_path TO %s;
        ALTER USER %s SET search_path TO %s;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO %s;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO %s;
        ALTER DEFAULT PRIVILEGES IN SCHEMA v1 GRANT ALL ON TABLES TO %s;
        ALTER DEFAULT PRIVILEGES IN SCHEMA v1 GRANT ALL ON SEQUENCES TO %s;
        """ % (b7_user, b7_user, b7_pass, b7_user, b7_pass,
               db_name, b7_user,
               db_name, b7_user,

               'b7group_user', b7_user,

               'v1', b7_user,
               'v1', b7_user,
               'v1', b7_user,
               'public', b7_user,
               'public', b7_user,
               'public', b7_user,

               db_name, ', '.join(('v1', 'public')),
               b7_user, ', '.join(('v1', 'public')),
               b7_user,
               b7_user,
               b7_user,
               b7_user,
            )


    def _get_sql_sequence(self, table_attr):
        """
        Return psql statement to set SEQUENCE value for specific table
        """
        template = 'SELECT setval(\'%s_%s_seq\', %d, false);\n'
        return "\n".join([template % (
                    table_attr['name'], col_attrs['name'], table_attr['autoIncrement']
                ) for col_name, col_attrs in table_attr['columns'].iteritems() if col_attrs['isPk'] and table_attr['autoIncrement'] ])

    @staticmethod
    def _get_dump_initial_statements():
        return """-- Converted by db_converter
        SET standard_conforming_strings=on;
        SET escape_string_warning=on;
        SET client_min_messages TO ERROR;
        SET client_encoding = 'UTF8';
        SET NAMES 'UTF8';
        SET CONSTRAINTS ALL DEFERRED;
        """

    @staticmethod
    def _get_dump_final_statements():
        return """
        SET CONSTRAINTS ALL IMMEDIATE;
        """

    @staticmethod
    def _supaFilta( v):
        if v is None: return 'NULL'
        if v is True: return 'true'
        if v is False: return 'false'

        # if isinstance(v, unicode): v = v.encode('utf8')
        if isinstance(v, str): return "'" + v.replace("'", "''") + "'"

        return v

    def _get_sql_fkeys(self, table_attr):
        """
        Generate create database statement
        :param table_attr: table attrs
        :return: SQL statement for creating
        """
        default_on_def = 'RESTRICT DEFERRABLE INITIALLY IMMEDIATE'
        fkey_template = 'ALTER TABLE "%s" ADD CONSTRAINT %s_%s_fkey FOREIGN KEY (%s) REFERENCES %s ON DELETE %s;'
        # index_template = 'CREATE INDEX %s_%s_idx ON %s (%s);'
        fkeys = ''

        for col_name, col_attrs in table_attr['columns'].iteritems():
            if col_attrs['reference']:
                fkeys += '\n' + fkey_template % \
                (table_attr['name'], table_attr['name'], col_attrs['name'], col_attrs['name'],
                 col_attrs['reference'], col_attrs['on_delete'] if 'on_delete' in col_attrs else default_on_def)
                # fkeys += '\n' + index_template % (table_attr['name'], col_attrs['name'], table_attr['name'], col_attrs['name'])

        return fkeys

    def _get_sql_indexes(self, table_attr):
        """
        Generate indexes database statement
        :param table_attr: table attrs
        :return: SQL statement for creating
        """
        index_template = 'CREATE INDEX %s_%s_x ON %s ("%s");\n'
        indexes = '\n';

        for index_name, index_attrs in table_attr['indexes'].iteritems():
            columns = list()
            for index_column_name in index_attrs['columns']:
                columns.append(table_attr['columns'][index_column_name]['name'])
            indexes += index_template % (table_attr['name'], index_attrs['name'], table_attr['name'], '" ,"'.join(columns))

        return indexes

    def _get_sql_create_table(self, table_attr):
        """
        Generate create database statement
        :param table_attr: table attrs
        :return: SQL statement for creating
        """
        template = 'CREATE TABLE IF NOT EXISTS "%s" (\n %s );'
        columns_pri, columns_ref, columns, columns_ignore = \
            PsqlParser._get_categorized_columns(table_attr['columns'])
        v2_columns = []
        for columnName, columnAttr in merge_dicts(columns_pri, columns_ref, columns).iteritems():
            v2_columns.append(PsqlParser._get_sql_column(columnAttr))
        return template % (table_attr['name'], ", \n ".join(v2_columns))

    @staticmethod
    def _get_categorized_columns(tableColumns):
        """
        Generate return table columns by category
        :return: PK, ref_cols, remaining, skipped
        """
        columns = {}
        columns_ref = {}
        columns_pri = {}
        columns_ignore = {}

        for col_name, col_attrs in tableColumns.iteritems():
            if RuleHandler.STR_SKIP in col_attrs:
                columns_ignore[col_name] = col_attrs
            elif col_attrs['isPk']:
                columns_pri[col_name] = col_attrs
            elif col_attrs['reference']:
                columns_ref[col_name] = col_attrs
            else:
                columns[col_name] = col_attrs

        return columns_pri, columns_ref, columns, columns_ignore

    @staticmethod
    def _get_sql_column(column_attr):
        """
        Generate table columns statements
        :param column_attr: col attrs
        :return: SQL statement adding columns
        """
        col_def_sql = ' "%s"' % column_attr['name']
        if column_attr['extra'] == 'auto_increment':
            col_def_sql += ' SERIAL'
        else:
            col_def_sql += ' %s' % column_attr['type'].upper()

        if column_attr['size'] and column_attr['type'] not in ['text', 'bytea', 'smallint', 'decimal', 'set']:
            col_def_sql += '(' + str(column_attr['size']) + ')'
        if not column_attr['nullable']:
            col_def_sql += ' NOT NULL'
        if column_attr['isPk']:
            col_def_sql += ' PRIMARY KEY'
        if column_attr['default'] is not None:
            if column_attr['default'].replace(".", "", 1).isdigit():
                if column_attr['type'] == 'boolean':
                    col_def_sql += ' DEFAULT ' + ('true' if column_attr['default'] != '0' else 'false')
                else:
                    col_def_sql += ' DEFAULT ' + column_attr['default']
            elif column_attr['default'] == 'current_timestamp':
                col_def_sql += ' DEFAULT ' + column_attr['default']
            elif column_attr['default'].lower() == "true" or column_attr['default'].lower() == "false":
                col_def_sql += ' DEFAULT ' + column_attr['default'].upper()
            else:
                col_def_sql += " DEFAULT U&'%s'" % column_attr['default']

        return col_def_sql

    @staticmethod
    # NOT IN USE
    def _psql_escape(value, value_type):
        if value is None:
            return 'null'
        if value_type in ['int', 'decimal']:
            return value if not isinstance(value, Decimal) else float(value)
        if value_type in ['boolean']:
            return 'false' if value == '0' else 'true'
        if value_type.startswith('timestamp') or value_type == 'date':
            return str(value)
        # return str(value.encode('utf8').replace('\'', '\'\'').replace('\\', '\\\\'))
        # return psycopg2._param_escape(value.encode('utf8'))
        return '$$'+str(value.encode('utf8'))+'$$'

    @staticmethod
    def sql_copy_format(row_data):
        row_data = re.sub(r"::(\w*)", "", row_data)
        row_data = row_data.replace("', E'", "', '")
        return row_data
