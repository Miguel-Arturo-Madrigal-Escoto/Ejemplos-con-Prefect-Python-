import mysql.connector

class MySQLTask:
    def __init__(self, db: str, user: str, password: str, host: str) -> None:
        self.__db = db
        self.__user = user
        self.__password = password
        self.__host = host

        self.__connection = mysql.connector.connect(
            host = self.__host,
            user = self.__user,
            password = self.__password,
            database = self.__db
        )
        self.__cursor = self.__connection.cursor()

    def run(self, query: str, params:tuple = None) -> int:
        if params is None:
            self.__cursor.execute(query)
        else:
            self.__cursor.execute(query, params)
        
        self.__connection.commit()
        
        return self.__cursor.rowcount