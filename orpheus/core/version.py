from datetime import datetime
import orpheus.core.orpheus_const as const
from orpheus.core.manager import Manager
from orpheus.core.helper import Print

class VersionManager(Manager):
    suffix = const.VERSION_SUFFIX
    pkey = 'vid'

    def create_table(self, dataset):
        print("Creating the version table ...")
        table = const.PUBLIC_SCHEMA + dataset + self.suffix
        self.conn.cursor.execute("CREATE TABLE %s (vid SERIAL PRIMARY KEY, \
                                              author TEXT, \
                                              num_records INT, \
                                              parent INTEGER[], \
                                              children INTEGER[], \
                                              create_time TIMESTAMP, \
                                              commit_time TIMESTAMP, \
                                              commit_msg TEXT);" % table)
    
    def init_version_graph(self, dataset, ridlist, user):
        self.p.pmessage("Initializing the version table ...")
        self.conn.refresh_cursor()
        self.conn.cursor.execute(
            "INSERT INTO %s VALUES (1, '%s', %s, '{-1}', '{}', '%s', '%s', 'init commit');" % \
                (const.PUBLIC_SCHEMA + dataset + self.suffix, user, str(len(ridlist)), str(datetime.now()), str(datetime.now())))
        self.conn.connect.commit()

    def update_version_graph(self, vgraph, user, num_records, parent_lst, table_create_time, msg):
        # create new version
        parent_lst_str = "'{" + ', '.join(parent_lst) + "}'"
        commit_time = str(datetime.now())
        table_create_time = table_create_time or commit_time
        vid = self.get_max_id(vgraph) + 1
        values = "(%s, '%s', %s, %s, %s, %s, %s, %s)" % (vid, user, num_records, parent_lst_str, "'{}'", "'%s'" % table_create_time, "'%s'" % commit_time, "'%s'" % msg)
        self.conn.cursor.execute("INSERT INTO %s VALUES %s;" % (vgraph, values))

        # update child column in the parent tuple
        self.conn.cursor.execute(
            "UPDATE %s SET children = ARRAY_APPEND(children, %s) WHERE vid = ANY(%s::int[]);" % \
                (vgraph, vid, parent_lst_str))

        self.conn.connect.commit()
        return vid

class IndexManager(Manager):
    suffix = const.INDEX_SUFFIX
    pkey = 'vid'

    def create_table(self, dataset):
        print("Creating the index table ...")
        table = const.PUBLIC_SCHEMA + dataset + self.suffix
        self.conn.cursor.execute("CREATE TABLE %s (vid INTEGER PRIMARY KEY, \
                                              rlist INTEGER[]);" % table)

    def init_index_table(self, dataset, ridlist):
        self.p.pmessage("Initializing the index table ...")
        self.conn.refresh_cursor()
        self.conn.cursor.execute(
            "INSERT INTO %s VALUES (1, '{%s}');" % \
                (const.PUBLIC_SCHEMA + dataset + self.suffix, str(','.join(map(str, ridlist)))))
        self.conn.connect.commit()

    def update_index_table(self, indextable, vid, ridlist):
        self.conn.cursor.execute(
            "INSERT INTO %s VALUES (%s, ARRAY%s);" % (indextable, vid, ridlist))
        self.conn.connect.commit()