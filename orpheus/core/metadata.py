import json
from datetime import datetime

from orpheus.core.exception import BadStateError
from orpheus.core.helper import Print
from orpheus.core.manager import Manager

class MetadataManager(Manager):
    # TODO: refactor executor usage into static
    def __init__(self, config, request=None):
        try:
            self.file_path = ".."
            self.meta_info = config['meta']['info']
            self.meta_modifiedIds = config['meta']['modifiedIds']
            self.p = Print(request)
        except KeyError as e:
            raise BadStateError("Context missing field %s, abort" % e.args[0])

    @staticmethod
    def _check_config(self):
        return self.meta_info, self.modified_ids
        # try:
        #     meta_info = config['meta']['info']
        #     meta_modifiedIds = config['meta']['modifiedIds']
        #     return meta_info, meta_modifiedIds
        # except KeyError as e:
        #     raise BadStateError("Context missing field %s, abort" % e.args[0])

    # Read metadata
    @staticmethod
    def load_meta(config):
        meta_info, _ = MetadataManager._check_config(config)
        with open(meta_info, 'r') as f:
            meta_info = f.readline()
        return json.loads(meta_info)

    # Commit metadata
    @staticmethod
    def commit_meta(config, meta):
        meta_info, _ = MetadataMangaer._check_config(config)
        open(meta_info, 'w').close()
        with open(meta_info, 'w') as f:
            f.write(json.dumps(meta))
        print("Metadata committed")

    @staticmethod
    def update(config, to_table, to_file, dataset, vlist, old_meta):
        print("Updating metadata ...")
        if to_table:
            self.update_tablemap(to_table, dataset, vlist, old_meta)
        if to_file:
            self.update_filemap(to_file, dataset, vlist, old_meta)
    
    @staticmethod
    def update_tablemap(config, to_table, dataset, vlid, old_meta):
        old_meta['table_map'][to_table] = dataset, vlist
        old_meta['table_created_time'][to_table] = str(datetime.now())
        return old_meta

    @staticmethod
    def update_filemap(config, to_file, dataset, vid, old_meta):
        old_meta['file_map'][to_file] = dataset, vlist
        # keep track of time?
        return old_meta

    @staticmethod
    def load_modified(config):
        _, meta_modifiedIds = MetadataManager._check_config(config)
        with open(meta_modifiedIds, 'r') as f:
            meta_modifiedIds = f.readline()
        return json.loads(meta_modifiedIds)

    @staticmethod
    def load_modified_id(config, table_name):
        meta = MetadataManager.load_meta(config)
        modified = MetadataManager.load_modified(config)
        modified_ids = []
        if table_name not in meta['merged_tables']:
            try:
                modified_ids = modified[table_name]
            except KeyError:
                raise ValueError("Table %s does not have changes, nothing to commit" % table_name)
        return modified_ids

    @staticmethod
    def load_parent_id(config, table_name, mapping='table'):
        try:
            meta = MetadataManager.load_meta(config)
            parent_vlist = meta[mapping][table_name]
            return parent_vlist
        except KeyError as e:
            raise BadStateError("Metadata information missing field %s, abort" % e.args[0])

    @staticmethod
    def update_parent_id(config, table_name, dataset, pvid, mapping='table'):
        plist = [str(pvid)]
        try:
            meta = MetadataManager.load_meta(config)
            meta[mapping][table_name] = dataset, plist
            MetadataManager.commit_meta(meta)
        except KeyError as e:
            raise BadStateError("Metadata information missing field %s, abort" % e.args[0])

    @staticmethod
    def load_table_create_time(config, table_name):
        try:
            meta = MetadataMangaer.load_meta(config)
            create_time = meta['table_created_time'][table_name]
            return create_time
        except KeyError:
            return None