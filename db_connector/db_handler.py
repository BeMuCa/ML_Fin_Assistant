"""
This function handles the database connection and queries.
"""
    
    
import psycopg2
from psycopg2 import sql
import pandas as pd

class DBHandler:
    """
    Class to handle database operations.
    """
    
    def __init__(self, db_name="fin_db", user="db_user", password="db_pwd", host="localhost", port="5000"):
        try:
            print("Connecting to the database...")
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            raise
    
    def close_connection(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()


    #### SETUP THE DATABASE TABLE's ####
    def create_table(self, stock_name):
        """
        Create a table in the database if it does not exist.
        
        :param conn: Connection object.
        """
        try:
            with self.conn.cursor() as cursor:
                
                # Create schema for the stock 
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {stock_name};")
                
                # EMA Table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {stock_name}.ema (
                        timestamp TIMESTAMP PRIMARY KEY DEFAULT NOW(),
                        ema_50 FLOAT NOT NULL,
                        ema_200 FLOAT NOT NULL,
                        ema_9 FLOAT NOT NULL
                    );
                """)
                # SMA Table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {stock_name}.sma (
                        timestamp TIMESTAMP PRIMARY KEY DEFAULT NOW(),
                        ema_50 FLOAT NOT NULL,
                        ema_200 FLOAT NOT NULL,
                        ema_9 FLOAT NOT NULL
                    );
                """)
                
                # MACD Table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {stock_name}.macd (
                        timestamp TIMESTAMP PRIMARY KEY DEFAULT NOW(),
                        macd_line FLOAT NOT NULL,
                        macd_signal FLOAT NOT NULL,
                        macd_hist FLOAT NOT NULL
                    );
                """)
                
                # Market Movement Table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {stock_name}.stock_movement (
                        timestamp TIMESTAMP PRIMARY KEY DEFAULT NOW(),
                        volatility FLOAT NOT NULL,
                        volume FLOAT NOT NULL,
                        daily_closing FLOAT NOT NULL,
                        daily_opening FLOAT NOT NULL,
                        daily_high FLOAT NOT NULL,
                        daily_low FLOAT NOT NULL,
                        gap FLOAT NOT NULL,
                        gap_percentage FLOAT NOT NULL
                    );
                """)
                
                # ML Model Performance Table
                cursor.execute(f"""
                    CREATE TABLE amd.track_ml_results (
                        id BIGSERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        predicted INTEGER,
                        actual INTEGER);
                """)
                               
                self.conn.commit()
                print("Table created successfully.")
        except psycopg2.Error as e:
            print(f"Error creating table: {e}")
            raise 
            #conn.rollback()

    def insert_stock_movement(self, stock_name, data):
        """
        Insert data into the specified table.
        
        :param conn: Connection object.
        :param stock_name: Name of the stock schema.
        :param table_name: Name of the table to insert data into.
        :param data: Dictionary containing the data to insert.
        """
        try:
            with self.conn.cursor() as cursor:
                
                # SMA
                cursor.execute(
                    f"""
                    INSERT INTO {stock_name}.stock_movement (
                        timestamp,
                        volatility,
                        volume,
                        daily_closing,
                        daily_opening,
                        daily_high,
                        daily_low,
                        gap,
                        gap_percentage
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data["your_timestamp"],
                        data["volatility_value"],
                        data["volume_value"],
                        data["closing_price"],
                        data["opening_price"],
                        data["high_price"],
                        data["low_price"],
                        data["gap_value"],
                        data["gap_pct_value"]
                    )
                )
        except psycopg2.Error as e:
            print(f"Error inserting data into {stock_name}.stock_movement: {e}")
            self.conn.rollback()

    def insert_sma(self, stock_name, data):
        """
        Insert data into the specified table.
        
        :param conn: Connection object.
        :param stock_name: Name of the stock schema.
        :param table_name: Name of the table to insert data into.
        :param data: Dictionary containing the data to insert.
        """
        try:
            with self.conn.cursor() as cursor:
                
                # SMA
                cursor.execute(
                f"""
                INSERT INTO {stock_name}.sma (
                    timestamp,
                    sma_50,
                    sma_200,
                    sma_9
                    ) VALUES (%s, %s, %s, %s);""",
                    (data["timestamp"], data["sma_50"], data["sma_200"], data["sma_9"])
                )
        except psycopg2.Error as e:
            print(f"Error inserting data into {stock_name}.sma: {e}")
            self.conn.rollback()                
                
    def insert_ema(self, stock_name, data):
        """
        Insert data into the specified table.
        
        :param conn: Connection object.
        :param stock_name: Name of the stock schema.
        :param table_name: Name of the table to insert data into.
        :param data: Dictionary containing the data to insert.
        """
        try:
            with self.conn.cursor() as cursor:
                # EMA
                cursor.execute(
                    f"""
                    INSERT INTO {stock_name}.ema (
                        timestamp,
                        ema_50,
                        ema_200,
                        ema_9
                    ) VALUES (%s, %s, %s, %s);""",
                    (data["timestamp"], data["ema_50"], data["ema_200"], data["ema_9"])
                )
        except psycopg2.Error as e:
            print(f"Error inserting data into {stock_name}.ema: {e}")
            self.conn.rollback()

    def insert_macd(self, stock_name, data):
        """
        Insert data into the specified table.
        
        :param conn: Connection object.
        :param stock_name: Name of the stock schema.
        :param table_name: Name of the table to insert data into.
        :param data: Dictionary containing the data to insert.
        """
        try:
            with self.conn.cursor() as cursor:
                # MACD
                cursor.execute(
                    f"""
                    INSERT INTO {stock_name}.macd (
                        timestamp,
                        macd_line,
                        macd_signal,
                        macd_hist
                    ) VALUES (%s, %s, %s, %s);""",
                    (data["timestamp"], data["macd_line"], data["macd_signal"], data["macd_hist"])
                )
        except psycopg2.Error as e:
            print(f"Error inserting data into {stock_name}.macd: {e}")
            self.conn.rollback()


    def combine_indicators(self, stock_name):
        """
        Combine all indicators into a single table.
        
        :param conn: Connection object.
        :param stock_name: Name of the stock schema.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT
                        e.timestamp,
                        e.ema_50,
                        e.ema_200,
                        e.ema_9,
                        s.sma_50,
                        s.sma_200,
                        s.sma_9,
                        m.macd_line,
                        m.macd_signal,
                        m.macd_hist,
                        mm.volatility,
                        mm.volume,
                        mm.daily_closing,
                        mm.daily_opening,
                        mm.daily_high,
                        mm.daily_low,
                        mm.gap,
                        mm.gap_percentage
                    FROM {stock_name}.stock_movement AS mm
                    JOIN {stock_name}.sma AS s ON mm.timestamp = s.timestamp
                    JOIN {stock_name}.macd AS m ON s.timestamp = m.timestamp
                    JOIN {stock_name}.ema AS e ON m.timestamp = e.timestamp
                    ORDER BY e.timestamp DESC;
                    """
                )
                self.conn.commit()
                results = cursor.fetchall() # returned tuples 
                columns = [ 
                    "timestamp", "ema_50", "ema_200", "ema_9",
                    "sma_50", "sma_200", "sma_9",
                    "macd_line", "macd_signal", "macd_hist",
                    "volatility", "volume", "daily_closing", "daily_opening",
                    "daily_high", "daily_low", "gap", "gap_percentage"
                ]
                
                return pd.DataFrame(results, columns=columns)
        except psycopg2.Error as e:
            print(f"Error combining indicators for {stock_name}: {e}")
            raise

    def add_bull_bear_label(self, df): # POSSIBLE ADJUSTMENTS HERE 
        """
        Depending on the trajectory of the next day daily close, add label bullish/bearish

        Bull if the day after tomorrow is higher than now.
        
        """
        df = df.sort_values("Datetime", ascending=False).reset_index(drop=True)
        labels = []

        for i in range(len(df)):
            current_close = df.loc[i, "daily_closing"]

            # Check if there are 4 future rows to compare
            if i + 5 < len(df):
                future_closes = df.loc[i+5, "daily_closing"].values # MIGHT ADD THE PERCENTAGE DIFFERENCE
                                                                    # >1% ? depends on stock
                if (future_closes > current_close):
                    labels.append(1)
                else:
                    labels.append(-1)
            else:
                labels.append(None)  # Not enough future data

        df["label"] = labels
        return df
        
        

if __name__ == "__main__":
    # Example usage
    db_handler = DBHandler()
    db_handler.create_table(db_handler.conn, "AMD")  # Replace "AAPL" with your stock name