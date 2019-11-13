from orpheus.core.encryption import EncryptionTool

import orpheus.core.exception as exception
import json

class LocalUserExistsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class InvalidCredentialError(Exception):
    def __str__(self):
        return "credentials do not match records"

class UserManager(object):
    @classmethod
    def config_path(self):
        return ".meta/config"
    
    @classmethod
    def user_path(self):
        return ".meta/users"

    @classmethod
    def check_user_exists(cls, user):
        from os import listdir
        from os.path import isfile
        return user in [usr for usr in listdir(cls.user_path())] and isfile('/'.join([cls.user_path(), user, 'config']))

    @classmethod
    def create_user(cls, conn, user, password=None):
        from os import makedirs
        if cls.check_user_exists(user):
            return None

        user_obj = {'user': user}
        passphrase = EncryptionTool.passphrase_hash(password)
        user_obj['passphrase'] = passphrase

        user_directory = '/'.join([cls.user_path(), user])
        makedirs(user_directory)
        with open('/'.join([user_directory, 'config']), 'w+') as f:
            f.write(json.dumps(user_obj))

        conn.create_user(user, password)
        
        return 1

    # this method is very dangerous! use with caution
    @classmethod
    def delete_user(cls, user, password):
        raise exception.NotImplementedError("delete_user not implemented")

    @classmethod
    def get_current_state(cls):
        try:
            with open(cls.config_path(), 'r') as f:
                config_info = json.loads(f.readline())
            return config_info
        except Exception:
            return None

    @classmethod
    def write_current_state(cls, obj):
        user_obj = {'database': '', 'user': '', 'passphrase': ''}
        for key in user_obj:
            user_obj[key] = obj[key]
        with open(cls.config_path(), 'w') as f:
            f.write(json.dumps(user_obj))

    @classmethod
    def verify_credential(cls, user, raw):
        if cls.check_user_exists(user):
            user_obj = cls._get_user_config(user)
            if user_obj['passphrase'] == EncryptionTool.passphrase_hash(raw):
                return True
        raise InvalidCredentialError()

    @classmethod
    def _get_user_config(cls, user):
        with open('/'.join([cls.user_path(), user, 'config']), 'r') as f:
            user_obj = json.loads(f.readline())
        return user_obj

    # for debugging purposes
    @classmethod
    def _list_users(cls):
        from os import listdir
        return [usr for usr in listdir(cls.user_path())]