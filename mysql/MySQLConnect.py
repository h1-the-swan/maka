from sqlalchemy import create_engine, MetaData, Table

class MySQLConnect(object):
    # Use DBConfiguration.db_connection_string with SQLAlchemy's create_engine to make a database connection to use with pandas
    def __init__(self,
                 protocol = 'mysql+pymysql',
                 db_name = None,
                 host_name = None,
                 user_name = None,
                 password = None,
                 encoding = None,
                 module_path = None,
                 # db_settings = None,
                 test = False):

        self.engine = None
        self.metadata = None
        self.tables = None

        # if db_settings:
        #     self.db_name = db_settings['database_name']
        #     self.protocol = db_settings['database_protocol']
        #     self.host_name = db_settings['database_host']
        #     self.user_name = db_settings['database_user']
        #     self.password = db_settings['database_pass']
        #     if db_settings.has_key('database_encoding'):
        #         self.encoding = db_settings['database_encoding']
        #     else:
        #         self.encoding = encoding
        #     self.module_path = db_settings['module_path']
        #     self.engine = self.get_engine()
        # else:
        #     self.db_name = db_name
        #     self.protocol = protocol.lower()
        #     self.host_name = host_name
        #     self.user_name = user_name
        #     self.password = password
        #     self.encoding = encoding
        #     self.module_path = module_path
        self.db_name = db_name
        self.protocol = protocol.lower()
        self.host_name = host_name
        self.user_name = user_name
        self.password = password
        self.encoding = encoding
        self.module_path = module_path
        self.test = test

        if self.host_name:
            self.get_engine()

    def get_connection_string(self):
        con = self.protocol + "://"
        con = con + self.user_name
        if self.password:
            con = con + ":" + self.password
        con = con + "@" + self.host_name
        if self.db_name:
            con = con + "/" + self.db_name
        con = con + "?local_infile=1"
        if self.encoding:
            con = con + "&charset=" + self.encoding
        return con

    def get_engine(self):
        """Get sqlalchemy engine
        :returns: sqlalchemy engine object

        """
        if not self.engine:
            self.engine = create_engine(self.get_connection_string())
            self.metadata = MetaData(self.engine)
            self.metadata.reflect()
        self.tables = self.get_all_tables()
        return self.engine

    def load_table(self, table_name=None):
        """Load a database table by name

        :table_name: database table name
        :returns: sqlalchemy Table object

        """
        return Table(table_name, self.metadata, autoload=True)

    def get_all_tables(self):
        """
        Returns a dictionary of {tablename: Table object}
        for the tables in the database
        """
        return {tablename: self.load_table(tablename) for tablename in self.engine.table_names()}

    def read_sql(self, sq):
        """
        Use pandas.read_sql() to execute a SQL query to get a dataframe
        See pandas documentation
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("You need to install Pandas for this")
        return pd.read_sql(sq, self.engine)


    def __repr__(self):
        result = "DBConfiguration():\n\t"
        result = result + "PROTOCOL='%s'\n\t"%(self.protocol)
        result = result + "DATABASE_NAME='%s'\n\t"%(self.db_name)
        result = result + "DATABASE_HOST='%s'\n\t"%(self.host_name)
        result = result + "DATABASE_USER='%s'\n\t"%(self.user_name)
        if( self.password ):
            result = result + "PASSWORD <is_set>\n\t"
        else:
            result = result + "PASSWORD <is_NOT_set>\n\t"
        result = result + "MODULE_PATH='%s'"%(self.module_path)
        return result

