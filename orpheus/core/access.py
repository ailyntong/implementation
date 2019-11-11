from orpheus.core.exception import NotImplementedError

class AccessManager():

    def check_access(self):
        raise NotImplementedError("AccessManager.check_access(self) not implemented")

    @staticmethod
    def grant_access(table, user_name):
        raise NotImplementedError("AccessManager.grant_access(table, user_name) not implemented")
