import psycopg2


class Database:
    def __init__(self, collector_pool_size: int = 1):
        self.database = "invest_simple"
        self.host = "139.130.195.51"  # "192.168.15.200"
        self.user = "pi"  # "interface_account"
        self.password = "actionN06"
        self.port = "5432"

        # define connection and cursor for later use
        self.connection = None
        self.cursor = None

    def connect(self):
        # attempt to connect to database
        try:
            # connect to database
            self.connection = psycopg2.connect(database=self.database,
                                               host=self.host,
                                               user=self.user,
                                               password=self.password,
                                               port=self.port)

            # get cursor
            self.cursor = self.connection.cursor()
        except psycopg2.OperationalError as e:
            print(e)
        except (Exception, psycopg2.DatabaseError) as e:
            print(e)
            exit()

        # if self.conn is not None: self.conn.close()
        return self.connection, self.cursor

    def query(self, query):
        # query all data from cashflow
        self.cursor.execute(query)

        # return results
        return self.cursor.fetchall()
