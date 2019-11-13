import shutil
import orpheus.core.orpheus_const as const
from orpheus.core.manager import Manager
from orpheus.core.exception import NotImplementedError

class RelationNotExistError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "Relation %s does not exist" % self.name

class RelationOverwriteError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "Relation %s exists, add flag to allow overwrite" % self.name

class ReservedRelationError(Exception):
  def __init__(self, tablename):
      self.name = tablename
  def __str__(self):
      return "Relation %s is a reserved name, please use a different one" % self.name

class ColumnNotExistError(Exception):
  def __init__(self, column):
      self.name = column
  def __str__(self):
      return "Column %s does not exist" % self.name

class RelationManager(Manager):
    suffix = const.DATA_SUFFIX
    pkey = 'rid'

    def create_table(self, dataset, schema):
        print("Creating the data table using the schema provided ...")
        table = const.PUBLIC_SCHEMA + dataset + self.suffix
        self.cursor.execute("CREATE TABLE %s (rid SERIAL PRIMARY KEY, \
                                              %s);" % (table, schema))

    def get_datatable_schema(self, from_table):
        sql = "SELECT column_name, data_type \
               FROM INFORMATION_SCHEMA.COLUMNS \
               WHERE table_name = '%s' AND column_name != 'rid';" % from_table
        self.conn.cursor.execute(sql)
        schema = self.conn.cursor.fetchall()
        attribute_names = map(lambda x: str(x[0]), schema)
        attribute_types = map(lambda x: str(x[1]), schema)
        return attribute_names, attribute_types

    # to_file needs an absolute path
    def checkout(self, vlist, datatable, indextable, to_table=None, to_file=None, delimiters=',', header=False, ignore=False):
        # sanity check
        if to_table:
            if RelationManager.reserve_table_check(to_table):
                raise ReservedRelationError(to_table)
            if self.check_table_exists(to_table):
                if ignore:
                   self.drop_table_force(to_table) # TODO: ask if user wants to overwrite
                else:
                    raise RelationOverwriteError(to_table)

        if not self.check_table_exists(datatable):
            raise RelationNotExistError(datatable)

        attribute_names, attribute_types = self.get_datatable_schema(datatable)
        ridlist = self.select_records_from_vlist(vlist, indextable)

        if to_table:
            self._checkout_table(attribute_names, ridlist, datatable, to_table, ignore)
        if to_file:
            tmp_file = self._checkout_file(attribute_names, ridlist, datatable, to_file, delimiters, header)
            shutil.copy(tmp_file, to_file)

        self.conn.connect.commit()
        


    def _checkout_file(self, attributes, ridlist, datatable, to_file, delimiters, header):
        tmp_file = '/tmp/' + to_file.split('/')[-1]
        # convert to a tmp_table first
        self.drop_table_force('tmp_table')
        self._checkout_table(attributes, ridlist, datatable, 'tmp_table', None)
        sql = "COPY %s (%s) TO '%s' DELIMITER '%s' CSV HEADER;" if header else "COPY %s (%s) TO '%s' DELIMITER '%s' CSV;"
        sql = sql % ('tmp_table', ','.join(attributes), temp_file, delimiters)
        self.conn.cursor.execute(sql)

        return tmp_file

    # Select the records into a new table
    def _checkout_table(self, attributes, ridlist, datatable, to_table, ignore):
        if not ignore:
            sql = "SELECT %s INTO %s FROM %s WHERE rid = ANY('%s'::int[]);" \
                % (', '.join(attributes), to_table, datatable, ridlist)
        else: # TODO
            pass
        self.conn.cursor.execute(sql)

    def drop_table(self, table_name):
        if not self.check_table_exists(table_name):
            raise RelationNotExistError(table_name)
        self.conn.cursor.execute("DROP TABLE %s;" % table_name)
        self.conn.connect.commit()

    def drop_table_force(self, table_name):
        if not self.check_table_exists(table_name):
            return
        self.conn.cursor.execute("DROP TABLE %s;" % table_name)
        self.conn.connect.commit()
    
    def select_all_rids(self, table_name):
        self.conn.cursor.execute("SELECT rid FROM %s;" % table_name)
        return [x[0] for x in self.conn.cursor.fetchall()]

    def generate_complement_sql(self, table_name, view_name, attributes=None):
        if not attributes:
            sql = "TABLE %s EXCEPT TABLE %s" % (table_name, view_name)
        else:
            sql = "(SELECT %s FROM %s) EXCEPT (SELECT %s FROM %s)" % (','.join(attributes), table_name, ','.join(attributes), view_name)
        return sql

    def create_parent_view(self, datatable, indextable, parent_vlist, view_name):
        self.conn.cursor.execute(
            "CREATE VIEW %s AS \
             SELECT * FROM %s INNER JOIN %S ON rid = ANY(rlist) \
             WHERE vid = ANY(ARRAY[%s]);" % (view_name, datatable, indextable, ','.join(parent_vlist)))

    def drop_view(self, view_name):
        self.conn.cursor.execute("DROP VIEW IF EXISTS %s;" % view_name)

    def select_intersection(self, table_name, view_name, join_attributes, projection='rid'):
        # SELECT rid FROM tmp_table INNER JOIN dataset_datatable ON tmp_table.employee_id = dataset_datatable.employee_id;
        join_clause = " AND ".join(["%s.%s=%s.%s" % (table_name, attr, view_name, attr) for attr in join_attributes])
        self.conn.cursor.execute(
            "SELECT %s.%s \
             FROM %s INNER JOIN %s ON %s;" % (view_name, projection, table_name, view_name, join_clause))
        return self.conn.cursor.fetchall()

    def convert_csv_to_table(self, file_path, table, attributes, delimiter=',', header=False):
        sql = "COPY %s (%s) FROM '%s' DELIMITER '%s' CSV HEADER;" % (table, ",".join(attributes), file_path, delimiters) if header \
          else "COPY %s (%s) FROM '%s' DELIMITER '%s' CSV;" % (table, ",".join(attributes), file_path, delimiters)
        self.conn.cursor.execute(sql)
        self.conn.connect.commit()

    def create_relation(self, table_name): # TODO
        raise NotImplementedError("create_relation not implemented")

    # will drop existing table to create the new table
    def create_relation_force(self, table_name, sample_table, sample_table_attributes=None):
        if self.check_table_exists(table_name):
            self.drop_table(table_name)
        if not sample_table_attributes:
            sample_table_attributes, _ = self.get_datatable_schema(sample_table)

        # an easier approach to create empty table
        self.conn.cursor.execute("CREATE TABLE %s AS SELECT %s FROM %s WHERE 1=2;" % (table_name, ','.join(sample_table_attributes), sample_table))
        self.conn.connect.commit()

    def check_table_exists(self, table_name):
        self.conn.cursor.execute(
            "SELECT EXISTS ( \
                SELECT 1 \
                FROM information_schema.tables \
                WHERE table_name = '%s');" % table_name)
        return self.conn.cursor.fetchall()[0][0]

    def update_datatable(self, table_name, sql):
        attribute_names, attribute_types = self.get_datatable_schema(table_name)
        self.conn.cursor.execute("INSERT INTO %s (%s) %s RETURNING rid;" % (table_name, ','.join(attribute_names), sql))
        new_rids = list(map(lambda t: t[0], self.conn.cursor.fetchall()))
        self.conn.connect.commit()
        return new_rids

    def clean(self): # TODO
        raise NotImplementedError("clean not implemented")

    @staticmethod
    def reserve_table_check(name):
        '''
        @summary: check if name is reserved
        @param name: table name
        @result: return True if table with name is reserved
        '''
        reserved = [const.DATA_SUFFIX, const.INDEX_SUFFIX, const.VERSION_SUFFIX, const.ATTRIBUTE_SUFFIX, 'orpheus']
        return any(s in name for s in reserved)

    def select_records_from_vlist(self, vlist, indextable):
        target_v = ','.join(vlist)
        self.conn.cursor.execute("SELECT DISTINCT rlist FROM %s WHERE vid = ANY(ARRAY[%s]);" % (indextable, target_v))
        data = [','.join(map(str, x[0])) for x in self.conn.cursor.fetchall()]
        return '{' + ','.join(data) + '}'

    def get_primary_key(self, table_name):
        self.conn.cursor.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) as data_type \
             FROM pg_index i \
             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
             WHERE i.indrelid = '%s'::regclass \
             AND i.indisprimary;" % table_name)
        return self.conn.cursor.fetchall()[0]

    def get_num_rows(self, table_name):
        self.conn.cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
        return self.conn.cursor.fetchall()[0][0]