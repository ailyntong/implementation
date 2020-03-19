from orpheus.core.access import AccessManager
from orpheus.core.attribute import AttributeManager
from orpheus.core.db import DatabaseConnection
from orpheus.core.exception import DatasetExistsError, BadStateError, NotImplementedError, BadParametersError
from orpheus.core.helper import Print
from orpheus.core.metadata import MetadataManager
from orpheus.core.relation import RelationManager, RelationNotExistError, RelationOverwriteError, ReservedRelationError
from orpheus.core.schema_parser import Parser as SimpleSchemaParser
from orpheus.core.sql_parser import SQLParser
from orpheus.core.version import VersionManager, IndexManager
from orpheus.core.vgraph import VersionGraph
# from orpheus.core.user_control import UserManager

import orpheus.core.orpheus_const as const

import click
import sys
# import user
import json
import pandas as pd
import os
import sqlparse

from django.contrib import messages

class Executor(object):
    def __init__(self, config, conn, request=False):
        self.config = config
        self.conn = conn
        self.request = request
        self.p = Print(request)
        
        try:
            #TODO: Import these dummy; also include SQLParser
            self.metadata_manager = MetadataManager
            self.metadata_manager.config = config
            self.relation_manager = RelationManager(conn)
            self.attribute_manager = AttributeManager(conn)
            self.version_manager = VersionManager(conn, request)
            self.index_manager = IndexManager(conn, request)
            self.vgraph = VersionGraph(config, request)
        except Exception as e:
            import traceback 
            traceback.print_exc()
            self.p.perror(str(e))
            raise Exception

    @staticmethod
    def run(config, func_name, *args):
        conn = DatabaseConnection(config)
        executor = Executor(config, conn)
        executor.__getattribute__(func_name)(args)
    #TODO Try to change mentions of attributes into the new attributes name instead. Focus mainly on init, checkout, commit
    #TODO If that fails, check db.py for changes
    def exec_init(self, input_file, dataset, table, schema):
        try:
            if (not table and not schema) or (table and schema):
                raise BadParametersError("Need either a table or a schema file (not both)")

            if table:
                attribute_names, attribute_types = self.relation_manager.get_datatable_schema(table)
            else:
                abs_path = self.config['orpheus']['home'] + schema if schema[0] != '/' else schema
                attribute_names, attribute_types = SimpleSchemaParser.get_attributes_from_file(abs_path)
        except Exception as e:
            import traceback 
            traceback.print_exc()
            self.p.perror(str(e))
            raise Exception
        # at this point, we have a valid conn obj and relation_manager obj
        try:
            # schema of the dataset, of format (name, type)
            schema_tuple = list(zip(attribute_names, attribute_types))
            # create new dataset
            # create new attribute names
            mod_names = ['a' + str(i) for i in range(1, len(schema_tuple) + 1)]
            self.conn.init_dataset(self, input_file, dataset, schema_tuple, attributes=mod_names)
            # get all rids in list
            rid_lst = self.relation_manager.select_all_rids(const.PUBLIC_SCHEMA + dataset + const.DATA_SUFFIX)
            # init attribute table
            self.attribute_manager.init_attribute_table(dataset, schema_tuple)
            aid_lst = list(range(1, self.attribute_manager.get_max_id(dataset + const.ATTRIBUTE_SUFFIX) + 1))
            # init version info
            self.version_manager.init_version_graph(dataset, rid_lst, self.config['user'])
            self.index_manager.init_index_table(dataset, aid_lst, rid_lst)
        except DatasetExistsError as e:
            self.p.perror(str(e))
            return
        except Exception as e:
            # revert back to the state before create
            self.conn.drop_dataset(self, dataset)
            self.p.perror(str(e))
            return

        try:
            self.vgraph.init_vgraph_json(dataset, 1) # init vid = 1
        except Exception as e:
            self.vgraph.delete_vgraph_json(dataset)
            raise Exception

        # self.metadata_manager.write_head(dataset, '')

        self.p.pmessage("Dataset [%s] successfully created" % dataset)

    def exec_drop(self, dataset):
        # TODO: add a popup window to confirm
        # E.g. if click.confirm('Are you sure you want to drop %s?' % dataset):
        try:
            self.conn.drop_dataset(self, dataset)
            self.p.pmessage("Dataset [%s] has been dropped" % dataset)
        except Exception as e:
            self.p.perror(str(e))
            raise Exception
        
        self.vgraph.delete_vgraph_json(dataset)

    def exec_checkout(self, dataset, vlist, to_table, to_file, delimiters, header, ignore):
        if not to_table and not to_file:
            self.p.perror(str(BadParametersError("Need a destination, either a table (-t) or a file (-f)")))
            return

        abs_path = self.config['orpheus']['data'] + '/' + to_file if to_file and to_file[0] != '/' else to_file

        # only allow one checkout at a time
        # try:
        #     curr_head = self.metadata_manager.load_head(dataset)
        #     if curr_head != set(vlist):
        #         self.p.pmessage('Changing head from %s to %s' % (str(curr_head), str(vlist)))
        # except Exception as e:
        #     self.p.perror(str(e))
        #     raise Exception

        try:
            meta_obj = self.metadata_manager.load_meta()
            datatable = dataset + const.DATA_SUFFIX
            indextable = dataset + const.INDEX_SUFFIX
            attributetable = dataset + const.ATTRIBUTE_SUFFIX

            alist = self.index_manager.get_aids(dataset, vlist)
            print(alist)
            attnames, atttypes = self.attribute_manager.get_attributes(attributetable, alist)
            alist = ['a' + str(i) for i in alist]
            self.relation_manager.checkout(vlist, datatable, indextable, attribute_names=attnames, aids=alist, to_table=to_table, to_file=abs_path, delimiters=delimiters, header=header, ignore=ignore)

            # update meta info
            AccessManager.grant_access(to_table, self.conn.user)
            self.metadata_manager.update(to_table, abs_path, dataset, vlist, meta_obj)
            self.metadata_manager.commit_meta(meta_obj)
            # self.metadata_manager.write_head(dataset, vlist)
            if to_table:
                self.p.pmessage("Table %s has been cloned from version %s" % (to_table, ",".join(vlist)))
            if to_file:
                 self.p.pmessage("File %s has been cloned from version %s" % (to_file, ",".join(vlist)))
        except Exception as e:
            if to_table and not (RelationOverwriteError or ReservedRelationError):
                self.relation_manager.drop_table(to_table)
            if to_file:
                pass # TODO: delete the file
            self.p.perror(str(e))
            raise Exception

    def exec_commit(self, message, dataset, table_name, file_name, delimiters, header, schema):
        # sanity check
        if not table_name and not file_name:
            self.p.perror(str(BadParametersError("Need a source, either a table (-t) or a file (-f)")))
            return
        if table_name and file_name:
            self.p.perror(str(NotImplementedError("Can either commit a file or a table at a time")))
            return
        if table_name and not self.relation_manager.check_table_exists(table_name):
            self.p.perror(str(RelationNotExistError(table_name)))
            raise Exception
        if file_name and not schema:
            if not header:
                self.p.perror(str(BadParametersError("Need a schema file")))
            self.p.perror(str(BadParametersError("Use of header currently not supported")))
            return

        # load parent information about the table
        # TODO: We need to get the derivation information of the commited table;
        # Otherwise, in the multitable scenario, we do not know which datatable/version_graph/index_table
        # that we need ot update information.
        try:
            abs_path = self.config['orpheus']['data'] + '/' + file_name if file_name else self.config['orpheus']['home']
            if table_name:
                parent_vid_lst = self.metadata_manager.load_parent_id(table_name)
            else:
                parent_vid_lst = self.metadata_manager.load_parent_id(abs_path, mapping='file_map')
            self.p.pmessage("Parent dataset is %s" % parent_vid_lst[0])
            self.p.pmessage("Parent versions are %s" % ','.join(parent_vid_lst[1]))
        except Exception as e:
            self.p.perror(str(e))
            raise Exception
        
        parent_name, parent_lst = parent_vid_lst[0], parent_vid_lst[1]
        datatable_name = parent_name + const.DATA_SUFFIX
        indextable_name = parent_name + const.INDEX_SUFFIX
        graph_name = parent_name + const.VERSION_SUFFIX
        attributetable_name = parent_name + const.ATTRIBUTE_SUFFIX

        # identify new schema and update attribute table, data table schema
        try:
            if file_name: # get schema from schema file
                schema_path = self.config['orpheus']['home'] + schema if schema[0] != '/' else schema
                new_attribute_names, new_attribute_types = SimpleSchemaParser.get_attributes_from_file(schema_path)
            else: # get schema from table
                new_attribute_names, new_attribute_types = self.relation_manager.get_datatable_schema(table_name)
            new_schema = zip(new_attribute_names, new_attribute_types)
            # get parent schema
            parent_alist = self.index_manager.get_aids(parent_name, parent_lst)
            parent_attribute_names, parent_attribute_types = self.attribute_manager.get_attributes(attributetable_name, parent_alist)
            parent_schema = zip(parent_attribute_names, parent_attribute_types)
            # calculate diff with new schema
            deletions, additions, edits = self.attribute_manager._schema_diff_helper(parent_schema, new_schema)
            print(additions)
            # update attribute table
            removed_aids, new_aids = self.attribute_manager.update_attribute_table(attributetable_name, deletions, additions, edits)
            print(removed_aids, new_aids)
            new_caids = ['a' + str(i) for i in new_aids]
            # update data table schema
            self.relation_manager.update_datatable_schema(datatable_name, additions + edits, new_caids)
        except Exception as e:
            self.p.perror(str(e))
            raise Exception

        try:
            # convert file into tmp_table first, then set the table_name to tmp_table
            if file_name:
                # # need to know the schema for this file
                # attribute_names, attribute_types = self.relation_manager.get_datatable_schema(datatable_name)
                # create a tmp table
                alist = ['a' + str(i) for i in parent_alist if i not in removed_aids] + ['a' + str(i) for i in new_aids]
                self.relation_manager.create_relation_force('tmp_table', datatable_name, sample_table_attributes=new_attribute_names, sample_table_aid=alist)
                # push everything from csv to tmp_table
                self.relation_manager.convert_csv_to_table(abs_path, 'tmp_table', alist, delimiters=delimiters, header=header)
                table_name = 'tmp_table'
        except Exception as e:
            self.p.perror(str(e))
            raise Exception
        
        if table_name:
            try:
                # # update attribute table
                # alist = self.index_manager.get_aids(parent_name, parent_lst)
                # commit_attribute_names, commit_attribute_types = self.attribute_manager.get_attributes(attributetable_name, alist)
                # new_schema, new_alist, new_cols = self.attribute_manager.update_attribute_table(attributetable_name, parent_lst, alist, list(zip(commit_attribute_names, commit_attribute_types)))
                # if len(set(zip(attribute_names, attribute_types)) - set(zip(commit_attribute_names, commit_attribute_types))) > 0:
                #     raise BadStateError("%s and %s have different schemas" % (table_name, parent_name))
                view_name = "%s_view" % parent_name
                self.relation_manager.create_parent_view(datatable_name, indextable_name, parent_lst, view_name)
                # find existing rows that match data in table to be committed
                existing_rids = [t[0] for t in self.relation_manager.select_intersection(table_name, view_name, alist)]
                sql = self.relation_manager.generate_complement_sql(table_name, view_name, attributes=alist)
                # update data table
                new_rids = self.relation_manager.update_datatable(datatable_name, alist, sql)
                self.relation_manager.drop_view(view_name)
                
                self.p.pmessage("Found %s new records" % len(new_rids))
                self.p.pmessage("Found %s existing records" % len(existing_rids))

                curr_version_rid = existing_rids + new_rids

                # it can happen that there are duplicates in there
                table_create_time = self.metadata_manager.load_table_create_time(table_name) if table_name != 'tmp_table' else None
                # update_version_graph
                curr_vid = self.version_manager.update_version_graph(graph_name, self.config['user'], len(curr_version_rid), parent_lst, table_create_time, message)
                # update index table
                new_alist = list(set(parent_alist).difference(set(removed_aids)).union(set(new_aids)))
                self.index_manager.update_index_table(indextable_name, curr_vid, new_alist, curr_version_rid)
                self.p.pmessage("Committing version %s with %s records" % (curr_vid, len(curr_version_rid)))

                # update metadata
                if table_name:
                    self.metadata_manager.update_parent_id(table_name, parent_name, curr_vid)
                else:
                    self.metadata_manager.update_parent_id(abs_path, parent_name, curr_vid, mapping='file_map')
            except Exception as e:
                view_name = '%s_view' % parent_name
                self.relation_manager.drop_view(view_name)
                self.p.perror(str(e))
                raise Exception

        # cleanup
        if self.relation_manager.check_table_exists('tmp_table'):
            self.relation_manager.drop_table('tmp_table')
        
        self.vgraph.update_vgraph_json(parent_name, curr_vid, parent_lst)
        
        self.p.pmessage("Version %s has been committed!" % curr_vid)
        return parent_name, curr_vid, parent_lst

    def exec_run(self, sql):
        try:
            # excute_sql_line(ctx, sql)
            sqlparser = SQLParser(conn)
            sql_code = sqlparser.parse(sql)
            return conn.execute_sql(sql_code)
        except Exception as e:
            import traceback
            traceback.print_exc()
            #TODO find out what messages is (from django.contrib)
            messages.error(self.request, str(e))
            raise Exception

    def exec_explain(self, sql):
        try:
            # execute_sql_line(ctx, sql)
            sqlparser = SQLParser(conn)
            sql_code = sqlparse.format(sqlparser.parse(sql), reindent=True, keyword_case='upper')
            # TODO: less hacky -- success = SQL, others = other message
            messages.success(self.request, sql_code)
            # messages.info(self.request, sql_code, extra_tags='sql')
        except Exception as e:
            import traceback
            traceback.print_exc()
            messages.error(self.request, str(e))
            raise Exception

    def exec_show(self, dataset):
        table_lst = []
        
        def show_helper(table_name, pk="vid"):
            sql = "SELECT * FROM %s ORDER BY %s LIMIT 4;" % (table_name, pk)
            attr_names, transactions = self.conn.execute_sql(sql)
            table_lst.append((attr_names, transactions))
            return
        
        show_helper(dataset + const.VERSTION_SUFFIX)
        show_helper(dataset + const.DATA_SUFFIX)
        show_helper(dataset + const.INDEX_SUFFIX)
        return table_lst
        
    def exec_restore(self, table_name):

        def exec_restore_helper(table_name, is_serial=False, pk="vid"):
            sql = "SELECT * INTO %s FROM %s;" % (table_name, table_name + "_backup")
            self.conn.refresh_cursor()
            self.conn.execute_sql(sql)
            if is_serial:
                sql = "ALTER TABLE %s ADD COLUMN %s SERIAL PRIMARY KEY;" % (table_name, pk)
            else:
                sql = "ALTER TABLE %s ADD COLUMN %s PRIMARY KEY;" % (table_name, pk)
            self.conn.execute_sql(sql)
            self.conn.connect.commit()

        #TODO: asks if this is hardcoded in 
        cvd_name = "protein_links"
        try:
            # TODO: import stuff
            # Drop all CVDs
            cvd_lst = CVDs.objects.values('name')
            for cvd in cvd_lst:
                messages.info(self.request, "Dropping the CVD [%s] ..." % cvd['name'])
                self.exec_drop(cvd['name'])
            
            # Drop all privae tables
            private_tables = PrivateTables.objects.values('name')
            for table in private_tables:
                message.info(self.request, "Dropping the private table [%s]" % table['name'])
                sql = "DROP TABLE IF EXISTS \"%s\";" % table['name']
                self.conn.refresh_cursor()
                self.conn.execute_sql(sql)
            
            # Delete all local files
            private_files = PrivateFiles.objects.values('name')
            for file_name in private_files:
                messages.info(self.request, "Dropping the private file [%s]" % file_name['name'])
                fpath = self.config['orpheus']['home'] + file_name['name']
                try:
                    os.remove(fpath)
                except OSError:
                    pass

            # delete all tuples in privatetables and privatefiles,
            # and all but "protein_link" in CVDs
            PrivateTables.objects.all().delete()
            PrivateFiles.objcts.all().delete()
            CVDs.objects.filter(~ Q(name=cvd_name)).delete() # ??
            messages.info(self.request, "Cleared all Django models")

            # restore records from protein_links_backup
            datatable = const.PUBLIC_SCHEMA + cvd_name + const.DATA_SUFFIX
            exec_restore_helper(datatable, True, "rid")
            messages.info(self.request, "Restored %s" % datatable)

            indextable = const.PUBLIC_SCHEMA + cvd_name + const.INDEX_SUFFIX
            exec_restore_helper(indextable)
            messages.info(self.request, "Restored %s" % indextable)

            versiontable = const.PUBLIC_SCHEMA + cvd_name + const.VERSION_SUFFIX
            exec_restore_helper(versiontable)
            messages.info(self.request, "Restored %s" % versiontable)

            # Copy vGraph from backup
            vgraph_path = self.config['vgraph_json'] + '/' + cvd_name
            vgraph_path_backup = vgraph_path + '_backup'
            with open(vgraph_path_backup) as f:
                with open(vgraph_path, 'w') as f1:
                    for line in f:
                        f1.write(line)
            messages.info(self.request, "Restored Version Graph")
        
        except Exception:
            self.conn.refresh_cursor()
            # TODO: the exception message is vague
            raise Exception