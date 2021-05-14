import psycopg2
from random import randint
from datetime import datetime

class Tracker(object):
    
    """
    job_id, status, updated_time
    """
    
    def __init__(self,jobname):
        self.jobname = jobname
    
    def assign_job_id(self):
        value = str(randint(0, 10000)).zfill(5)
        job_id = self.jobname + str(value) + datetime.today().strftime('%Y%m%d')
        return job_id
    
    def update_job_status(self,job_id,status,conn):
      
        update_time = str(datetime.now())
        connection = conn
        
        try:
            dbCursor = conn.cursor()
            jobSqlCmd = "insert into spark_log(job_id,job_status,upd_date) values('"+ job_id + "','" + status + "','" + update_time + "')"
            dbCursor.execute(jobSqlCmd)
            dbCursor.close()
            
        except (Exception, psycopg2.Error) as error:
            print("error executing db statement for job tracker.")
            return
    
    def get_db_connection(self,dbName,dbHost,dbUser,dbPwd,dbPort):
        
        connection = None
  
        DB_NAME = dbName
        DB_HOST = dbHost
        DB_USER = dbUser
        DB_PWD = dbPwd
        DB_PORT = dbPort
        SSLMODE = "require"
    
        conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(DB_HOST, DB_USER, DB_NAME, DB_PWD,SSLMODE)
        
        try:
            connection = psycopg2.connect(conn_string)
            return connection
          
        except (Exception, psycopg2.Error) as error:
            print(error)