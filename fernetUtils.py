from cryptography.fernet import Fernet

class FernetSingleton:
    _instance = None

    def __new__(cls, key):
        if cls._instance is None:
            cls._instance = super(FernetSingleton, cls).__new__(cls)
            cls._instance._fernet = Fernet(key)
        return cls._instance

    @property
    def fernet(self):
        return self._fernet