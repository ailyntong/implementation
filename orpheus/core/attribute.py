import orpheus.core.orpheus_const as const
from orpheus.core.exception import NotImplementedError
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
    
    def init_attribute_table(self, dataset, schema):
        self.p.pmessage("Initializing the attribute table ...")
        self.conn.refresh_cursor()
        values = ",".join(["('%s', '%s')" % (attname, atttype) for attname, atttype in schema])
        self.conn.cursor.execute(
            "INSERT INTO %s(attname, atttype) VALUES %s;" % (const.PUBLIC_SCHEMA + dataset + self.suffix, values))
        self.conn.connect.commit()

    def update_attribute_table(self, attribute_table, deletions, additions, edits):
        if edits:
            raise NotImplementedError("currently cannot resolve column type changes")

        self.conn.refresh_cursor()
        # get aids for removed columns
        if deletions:
            sql = "SELECT aid FROM %s WHERE (attname, atttype) IN (VALUES" % attribute_table
            for attname, atttype in deletions:
                sql += " ('%s', '%s')," % (str(attname), str(atttype))
            sql = sql[:-1] + ");"
            self.conn.cursor.execute(sql)
            removed_aids = set([int(r[0]) for r in self.conn.cursor.fetchall()])
        else:
            removed_aids = set()
        # get aids for new columns
        if additions or edits:
            sql = "INSERT INTO %s(attname, atttype) VALUES" % attribute_table
            for attname, atttype in additions + edits:
                sql += " ('%s', '%s')," % (str(attname), str(atttype))
            sql = sql[:-1] + " RETURNING aid;"
            self.conn.cursor.execute(sql)
            new_aids = set([int(r[0]) for r in self.conn.cursor.fetchall()])
        else:
            new_aids = set()

        self.conn.connect.commit()

        return list(removed_aids), list(new_aids)

    def get_attributes(self, attribute_table, alist):
        alist_str = "'{" + ','.join(map(str, alist)) + "}'"
        self.conn.cursor.execute("SELECT attname, atttype FROM %s WHERE aid = ANY(%s::int[])" % (attribute_table, alist_str))
        result = self.conn.cursor.fetchall()
        attribute_names = list(map(lambda x: str(x[0]), result))
        attribute_types = list(map(lambda x: str(x[1]), result))
        return attribute_names, attribute_types

    # don't use
    def schema_diff(self, parent, child):
        sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"
        self.conn.cursor.execute(sql % parent)
        parent_schema = self.conn.cursor.fetchall()
        self.conn.cursor.execute(sql % child)
        child_schema = self.conn.cursor.fetchall()

        return self._schema_diff_helper(parent_schema, child_schema)

    def _schema_diff_helper(self, parent_schema, child_schema):
        parent_schema, child_schema = set(parent_schema), set(child_schema)

        deletions = parent_schema - child_schema
        additions = child_schema - parent_schema

        deletions_t = [tuple(attname for attname, atttype in deletions), tuple(atttype for attname, atttype in deletions)]
        additions_t = [tuple(attname for attname, atttype in additions), tuple(atttype for attname, atttype in additions)]

        edits = set(deletions_t[0]).intersection(set(additions_t[0]))
        if edits:
            edits = [(a, deletions_t[1][deletions_t[0].index(a)], additions_t[1][additions_t[0].index(a)]) for a in edits]

        return list(deletions), list(additions), list(edits)