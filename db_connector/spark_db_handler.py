


class DBHandler:
    """
    Class to handle database operations.
    """
    
    def __init__(self, db_name="fin_db", user="db_user", password="db_pwd", host="localhost", port="5000"):
            self.dbname=db_name
            self.user=user
            self.password=password
            self.host=host
            self.port=port

    def insert_sma_spark(self, spark_df, stock_name):
        """
        Insert SMA data into PostgreSQL using Spark JDBC.
        
        :param spark_df: Spark DataFrame containing SMA data
        :param stock_name: Schema name (e.g. 'AAPL')
        :param db_url: JDBC URL to PostgreSQL
        :param db_user: DB username
        :param db_password: DB password
        """
        table_name = f"{stock_name}.sma"
        spark_df.write \
            .format("jdbc") \
            .option("url", self.db_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

