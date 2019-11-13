import hashlib, binascii
from orpheus.core.exception import BadParametersError

class EncryptionTool():

    # TODO: salt should have a random state?
    # disabled for linux
    @staticmethod
    def passphrase_hash(raw, salt=b'datahub', method='raw', iteration=100000):
        if method == 'sha256':
            return binascii.hexlify(hashlib.pbkdf2_hmac(method, raw, salt, iteration))
        elif method == 'raw':
            return raw
        else:
            raise BadParametersError("Invalid encryption method")
