import yaml
import logging
import click
import psycopg2

from orpheus.core.exception import BadStateError, ConnectionError, SQLSyntaxError

class DatabaseConnection:
    def __init__(self, config):
        try:
            self.verbose = False
            self.connect = None
            self.cursor = None
            self.config = config
            logging.basicConfig(filename=config['meta']['log_path'], format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S ')
            self.user_log = open(config['meta']['user_log'], 'a')
            self.home = config['orpheus']['home']
            self.current_db = config['database']
            self.user = config['user']
            self.password = config['passphrase']
            self.connect_str = "host=" + self.config['host'] + " port=" + str(self.config['port']) + " dbname=" + self.currentDB + " user=" + self.user + " password=" + self.password
            self.connect_db()
        except KeyError as e:
            raise BadStateError("context missing field %s, abort" % e.args[0])

    def connect_db(self):
        print("Connecting to the database [%s] ..." % self.current_db)
        try:
            if self.verbose:
                click.echo('Trying to connect to %s' % self.current_db)
            logging.info('Trying to connect to %s' % self.current_db)
            self.connect = psycopg2.connect(self.connect_str)
            self.cursor = self.connect.cursor()
        except psycopg2.OperationalError as e:
            logging.error('%s is not open; connect_str was %s' % (self.currentDB, self.connect_str))
            raise ConnectionError("Cannot connect to the database [%s] @ [%s]:[%s]. Check connection, username, password and database name." % (self.currentDB, self.config['host'], self.config['port']))

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
            if SQLParser.is_select(sql): # return records
                colnames = [desc[0] for desc in self.cursor.description]
                print(', '.join(colnames))
                for row in self.cursor.fetchall():
                    print(', '.join(str(e) for e in row))
            else:
                print(self.cursor.statusmessage)
            self.connect.commit() # commit UPDATE/INSERT messages
        except psycopg2.ProgrammingError:
            raise SQLSyntaxError()
    
    def init_dataset(self, executor, inputfile, dataset, schema, header=False, attributes=None):
        self.refresh_cursor()
        print("Creating the dataset [%s] to the database [%s] ...") % (dataset, self.current_db)
        
        # TODO: user stuff

        try:
            # for each dataset, create 4 tables
            # dataset_data, which includes all records, rid as PK, based on schema
            # dataset_version, which keep track of all version information
            # dataset_index, which includes all the vid and rid mappings
            # dataset_attribute, which includes all schema information

            if '.csv' not in inputfile:
                # TODO: finish other input later
                raise NotImplementedError("File formats other than CSV not implemented")
            if not attributes:
                # TODO: attribute inferring
                raise NotImplementedError("Attribute inferring not implemented")

            # create cvd into public schema 
            # TODO: change to private schema later

            executor.relation_manager.create_table(dataset, schema)
            executor.version_manager.create_table(dataset)
            executor.index_manager.create_table(dataset)
            executor.attribute_manager.create_table(dataset)

            # dump data
            file_path = self.config['orpheus']['home'] + inputfile 
            if header:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV HEADER;" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX, ",".join(attributes), file_path))
            else:
                self.cursor.execute("COPY %s (%s) FROM '%s' DELIMITER ',' CSV;" % (const.PUBLIC_SCHEMA + dataset + const.DATATABLE_SUFFIX, ",".join(attributes), file_path))
            
            self.connect.commit()
        except Exception as e:
            raise OperationError()

    def drop_dataset(self, executor, dataset):
        self.refresh_cursor()
        
        executor.relation_manager.drop_dataset(dataset)
        executor.version_manager.drop_dataset(dataset)
        executor.index_manager.drop_dataset(dataset)
        executor.attribute_manager.drop_dataset(dataset)

        self.connect.commit()

    def list_datasets(self):
        self.refresh_cursor()
        try:
            self.cursor.execute("SELECT * FROM %s;" % (self.user + '.datasets'))
            return [t[0] for t in self.cursor.fetchall()]
        except psycopg2.ProgrammingError:
            raise BadStateError("No dataset has been initialized before, try init first")

    def show_dataset(self): # TODO
        raise NotImplementedError("show_dataset not implemented")

    @classmethod
    def load_config(cls):
        try:
            with open('config.yaml', 'r') as f:
                obj = yaml.load(f)
            return obj
        except IOError:
            raise BadStateError("config.yaml file not found or data not clean, abort")

    # @classmethod
    # def create_user(cls, user, password, db):
    #     # Create user in the database
	# 	# Using corresponding SQL or prostegres commands
    #     # Set one-time only connection to the database to create user
    #     try:
    #         server_config = cls.load_config()
    #         # TODO: fix so that password isn't hardcoded
    #         conn_str = "host=" + server_config['host'] + " port=" + str(server_config['port']) + " dbname=" + db + " user='postgres' password='test'"
    #         connect = psycopg2.connect(conn_str)
    #         cursor = connect.cursor()
    #         # passphrase = EncryptionTool.passphrase_hash(password)
    #         cursor.execute("CREATE USER %s SUPERUSER;" % user) # TODO: add password detection later
    #         connect.commit()
    #     except psycopg2.OperationalError:
    #         raise ConnectionError("Cannot connect to %s at %s: %s" % (db, server_config['host'], str(server_config['port'])))
    #     except Exception as e:
    #         raise e

    def create_user(self, user, password):
        self.execute_sql("CREATE USER %s SUPERUSER WITH PASSWORD '%s'" % (user, password))
