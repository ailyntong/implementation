import json
from datetime import datetime

from orpheus.core.exception import BadStateError
from orpheus.core.helper import Print
from orpheus.core.manager import Manager

class MetadataManager(Manager):
    config = None
    # TODO: refactor executor usage into static
    # def __init__(self, config, request=None):
    #     try:
    #         self.file_path = ".."
    #         self.meta_info = config['meta']['info']
    #         self.meta_modifiedIds = config['meta']['modifiedIds']
    #         self.p = Print(request)
    #     except KeyError as e:
    #         raise BadStateError("Context missing field %s, abort" % e.args[0])

    @staticmethod
    def _check_config(config=config):
        # return self.meta_info, self.modified_ids
        try:
            meta_info = config['meta']['info']
            meta_modifiedIds = config['meta']['modifiedIds']
            return meta_info, meta_modifiedIds
        except KeyError as e:
            raise BadStateError("Context missing field %s, abort" % e.args[0])

    # Read metadata
    @staticmethod
    def load_meta():
        meta_info, _ = MetadataManager._check_config()
        with open(meta_info, 'r') as f:
            meta_info = f.readline()
        return json.loads(meta_info)

    # Commit metadata
    @staticmethod
    def commit_meta(meta):
        meta_info, _ = MetadataMangaer._check_config()
        open(meta_info, 'w').close()
        with open(meta_info, 'w') as f:
            f.write(json.dumps(meta))
        print("Metadata committed")

    @staticmethod
    def update(to_table, to_file, dataset, vlist, old_meta):
        print("Updating metadata ...")
        if to_table:
            update_tablemap(to_table, dataset, vlist, old_meta)
        if to_file:
            update_filemap(to_file, dataset, vlist, old_meta)
    
    @staticmethod
    def update_tablemap(to_table, dataset, vlid, old_meta):
        old_meta['table_map'][to_table] = dataset, vlist
        old_meta['table_created_time'][to_table] = str(datetime.now())
        return old_meta

    @staticmethod
    def update_filemap(to_file, dataset, vid, old_meta):
        old_meta['file_map'][to_file] = dataset, vlist
        # keep track of time?
        return old_meta

    @staticmethod
    def load_modified():
        _, meta_modifiedIds = MetadataManager._check_config()
        with open(meta_modifiedIds, 'r') as f:
            meta_modifiedIds = f.readline()
        return json.loads(meta_modifiedIds)

    @staticmethod
    def load_modified_id(table_name):
        meta = MetadataManager.load_meta()
        modified = MetadataManager.load_modified()
        modified_ids = []
        if table_name not in meta['merged_tables']:
            try:
                modified_ids = modified[table_name]
            except KeyError:
                raise ValueError("Table %s does not have changes, nothing to commit" % table_name)
        return modified_ids

    @staticmethod
    def load_parent_id(table_name, mapping='table'):
        try:
            meta = MetadataManager.load_meta()
            parent_vlist = meta[mapping][table_name]
            return parent_vlist
        except KeyError as e:
            raise BadStateError("Metadata information missing field %s, abort" % e.args[0])

    @staticmethod
    def update_parent_id(table_name, dataset, pvid, mapping='table'):
        plist = [str(pvid)]
        try:
            meta = MetadataManager.load_meta()
            meta[mapping][table_name] = dataset, plist
            MetadataManager.commit_meta(meta)
        except KeyError as e:
            raise BadStateError("Metadata information missing field %s, abort" % e.args[0])

    @staticmethod
    def load_table_create_time(table_name):
        try:
            meta = MetadataMangaer.load_meta()
            create_time = meta['table_created_time'][table_name]
            return create_time
        except KeyError:
            return None