from pyspark.sql.functions import *
class DBhelper:
    def __init__ (self,db_name,db_user,db_server_address,db_password ):
        self.db_name = db_name
        self.db_user =  db_user
        self.db_server_address = db_server_address
        self.db_password = db_password
        self.db_connection_string = f"jdbc:sqlserver://{db_server_address};databaseName={db_name}"
        self.db_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    
    def save_dataframe(self,df,table_name):
        df.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", self.db_connection_string) \
        .option("dbtable", table_name) \
        .option("user", self.db_user) \
        .option("driver", self.db_driver)\
        .option("password", self.db_password) \
        .save()



    