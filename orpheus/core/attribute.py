import orpheus_const as const
from helper import Print

class AttributeManager(object):
    def __init__(self, conn, request=None):
        self.conn = conn
        self.p = Print(request)

    def init_attribute_table_dataset(self, dataset, schema):
        self.p.pmessage("Initializing the attribute table ...")
        self.conn.refresh_cursor()
        values = ",".join(["('%s', '%s')" % (attname, atttype) for attname, atttype in schema])
        init_attribute_sql = "INSERT INTO %s VALUES %s;" % \
                             (const.PUBLIC_SCHEMA + dataset + const.ATTRIBUTETABLE_SUFFIX, values)
        self.conn.cursor.execute(init_attribute_sql)
        self.conn.connect.commit()

    def update_attribute_table(self, attribute_graph_name, attname, atttype):
        sql = "INSERT INTO %s VALUES ('%s', '%s');" % (attribute_graph_name, attname, atttype)
        self.conn.cursor.execute(sql)
        self.conn.connect.commit()