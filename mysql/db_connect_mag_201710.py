import os
from MySQLConnect import MySQLConnect

database_settings = {
    'protocol': 'mysql+pymysql',
    'host_name': '127.0.0.1',
    'user_name': os.environ.get('MYSQL_USERNAME'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'encoding': 'utf8',
    'module_path': None,
    'db_name': 'mag_2017-10'
}

def get_db_connection():
    c = MySQLConnect(**database_settings)
    return c

def prepare_base(db):
    # http://docs.sqlalchemy.org/en/latest/orm/extensions/automap.html
    from sqlalchemy.ext.automap import automap_base
    Base = automap_base()
    Base.prepare(db.engine, reflect=True)
    return Base
