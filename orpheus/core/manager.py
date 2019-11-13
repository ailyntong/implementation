from orpheus.core.helper import Print
import orpheus.core.orpheus_const as const

class Manager(object):
    def __init__(self, conn, config=None, request=None):
        self.conn = conn
        self.config = config
        self.request = request
        self.p = Print(request)

    def create_table(self, dataset):
        pass

    def drop_table(self, dataset):
        try:
            self.conn.cursor.execute("DROP TABLE %s;" % (const.PUBLIC_SCHEMA + dataset + self.suffix))
            self.conn.connect.commit()
        except:
            self.conn.refresh_cursor()

    def get_max_id(self, table):
        self.conn.cursor.execute("SELECT MAX(%s) FROM %s;" % (self.pkey, table))
        return int(self.conn.cursor.fetchall()[0][0])