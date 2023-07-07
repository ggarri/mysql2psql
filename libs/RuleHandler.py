__author__ = 'ggarrido'


class RuleHandler:
    """
        Apply a list of rules into given json. Just overwrite in case rules has a value on the same location
    """

    STR_SKIP = '_SKIP_'

    def __init__(self, rules=None, node_rules=None):
        """
        :param rules: Schema rules (same nesting level replacement)
        :param node_rules: Node level rules (column, table)
        :return:
        """
        self.rules = rules if rules is not None else {}
        self.node_rules = node_rules if node_rules is not None else {}

    def obtain_modified_schema(self, schema):
        """
        Iterate over every table in schema replacing its attrs in case there is rule for it
        :param schema: Db schema
        :return: schema after rules where applied
        """
        res_schema = schema.copy()
        self._apply_rules(res_schema)
        self._apply_node_rules(res_schema)
        return res_schema

    def _apply_rules(self, schema):
        if self.rules is None or 'tables' not in self.rules:
            return

        for table_name, table_attrs in self.rules['tables'].items():
            # If table doesn't WITH TIME ZONE on the schema, skip iter
            if 'tables' not in schema or table_name not in schema['tables']:
                continue

            # If table_attr is SKIP string, it means table is removed from schema
            elif table_attrs == self.STR_SKIP:
                del schema['tables'][table_name]
                continue

            # Otherwise, apply schema changes
            schema_part = schema['tables'][table_name]
            self._apply_rule_table(schema_part, table_attrs)

    def _apply_rule_table(self, schema, table_attrs):
        for table_attr_key, table_name_val in table_attrs.iteritems():
            if table_attr_key != 'columns':
                schema[table_attr_key] = table_name_val
                continue
            # If table doesn't have any column declared, skip iter
            elif 'columns' not in schema:
                continue
            # In case of columns, iterate over all of them, replacing values
            schema_part = schema['columns']
            self._apply_rule_col(schema_part, table_name_val)

    def _apply_rule_col(self, schema, col_attrs):
        for col_name, col_attrs in col_attrs.iteritems():
            # If schema doesn't have col_name defined, skip iter
            if col_name not in schema:
                continue
            elif col_attrs == self.STR_SKIP:
                schema[col_name][self.STR_SKIP] = True
                # del schema[col_name]
                continue
            for col_attr_key, col_attr_value in col_attrs.iteritems():
                schema[col_name][col_attr_key] = col_attr_value
                if col_attr_key == 'type' and 'size' not in col_attrs:
                    schema[col_name]['size'] = None

    def _apply_node_rules(self, schema):
        for table_name, table_attrs in schema['tables'].items():
            self._apply_table_node_rule(schema['tables'][table_name], table_attrs)

    def _apply_table_node_rule(self, schema, table_attrs):
        for table_attr_key, table_attr_value in table_attrs.items():
            # In case it is a table attr and there is rules for them
            if table_attr_key != 'columns' and table_attr_key in self.node_rules.get('table', {}):
                for node_attr_fromto in self.node_rules['table'][table_attr_key]:
                    if table_attr_value == node_attr_fromto['from']:
                        schema[table_attr_key] = node_attr_fromto['to']
            # In case of columns
            elif table_attr_key == 'columns':
                # Replace in case from value matches current column attr value
                for col_name, col_attrs in table_attr_value.items():
                    self._apply_col_node_rule(schema['columns'][col_name], col_attrs)

    def _apply_col_node_rule(self, schema, col_attrs):
        for col_attr_key, col_attr_value in col_attrs.items():
            if col_attr_key not in self.node_rules.get('column', {}):
                continue
            for node_attr_fromto in self.node_rules['column'][col_attr_key]:
                if col_attr_value == node_attr_fromto['from']:
                    # Check if there are cases depending of other attr values
                    schema[col_attr_key] = node_attr_fromto['to']
                    if '_IF_' in node_attr_fromto:
                        for if_cond in node_attr_fromto['_IF_']:
                            if schema[if_cond['attr']] == if_cond['val']:
                                schema[col_attr_key] = if_cond['to']

    @staticmethod
    def get_skip_colums(schema_changes):
        skipped_cols = []
        for table_name, table_attrs in schema_changes['tables'].tems():
            if 'columns' not in table_attrs:
                continue
            for col_name, col_attrs in table_attrs['columns'].items():
                if RuleHandler.STR_SKIP == col_attrs:
                    skipped_cols.append((table_attrs.get('name', table_name), col_name))
        return skipped_cols