# Database Manager exceptions
class UserNotSetError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class ConnectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class OperationError(Exception):
    def __str__(self):
        return 'Operation failure, check system parameters'

class DatasetExistsError(Exception):
    def __init__(self, value, user):
        self.value = value
        self.user = user
    def __str__(self):
        return 'Dataset [%s] exists under user [%s]' % (self.value, self.user)

class SQLSyntaxError(Exception):
    def __str__(self):
        return 'Error during executing sql, please revise!'


#Any system level general exceptions should be included here
#Module related exception should go into each module

class BadStateError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class NotImplementedError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value

class BadParametersError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return self.value