import orpheus.core.orpheus_const as const
from orpheus.core.manager import Manager

class AttributeManager(Manager):
    suffix = const.ATTRIBUTE_SUFFIX
    pkey = 'aid'
    valid_types = set(['int', 'float', 'text']) # and many more to be added

    def create_table(self, dataset):
        print("Creating the attribute table ...")
        table = const.PUBLIC_SCHEMA + dataset + self.suffix
        self.conn.cursor.execute("CREATE TABLE %s (aid SERIAL PRIMARY KEY, \
                                                   attname TEXT, \
                                                   atttype TEXT);" % table)
    
    def init_attribute_table_dataset(self, dataset, schema):
        self.p.pmessage("Initializing the attribute table ...")
        self.conn.refresh_cursor()
        values = ",".join(["('%s', '%s')" % (attname, atttype) for attname, atttype in schema])
        self.conn.cursor.execute(
            "INSERT INTO %s VALUES %s;" % (const.PUBLIC_SCHEMA + dataset + self.suffix, values))
        self.conn.connect.commit()

    def update_attribute_table(self, attribute_graph_name, attname, atttype):
        self.conn.cursor.execute("INSERT INTO %s VALUES ('%s', '%s');" % (attribute_graph_name, attname, atttype))
        self.conn.connect.commit()

    def schema_diff(self, parent, child):
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"
        self.conn.cursor.execute(sql % parent)
        parent_schema = self.conn.cursor.fetchall()
        self.conn.cursor.execute(sql % child)
        child_schema = self.conn.cursor.fetchall()

        return self.schema_diff_helper(parent_schema, child_schema)

    def schema_diff_helper(parent_schema, child_schema):
        parent_schema, child_schema = set(parent_schema), set(child_schema)

        deletions = parent_schema - child_schema
        additions = child_schema - parent_schema

        deletions_t = [tuple(attname for attname, atttype in deletions), tuple(atttype for attname, atttype in deletions)]
        additions_t = [tuple(attname for attname, atttype in additions), tuple(atttype for attname, atttype in additions)]

        edits = set(deletions_t[0]).intersection(set(additions_t[0]))
        if edits:
            edits = [(a, deletions_t[1][deletions_t[0].index(a)], additions_t[1][additions_t[0].index(a)]) for a in edits]

        return list(deletions), list(additions), list(edits)