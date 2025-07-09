from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator


class Pipeline:
    def __init__(self):
        self.spark = SparkSession.builder\
            .appName("BullBearClassifier")\
            .config("spark.jars", "B:/1_Berk_Coding/Jars/postgresql-42.7.3.jar") \
            .getOrCreate()
            
        self.labeled_data = None
        self.features = [ 
            "timestamp", "ema_50", "ema_200", "ema_9",
            "sma_50", "sma_200", "sma_9",
            "macd_line", "macd_signal", "macd_hist",
            "volatility", "volume", "daily_closing", "daily_opening",
            "daily_high", "daily_low", "gap", "gap_percentage"
        ]
        
    def load_df(self, df):
        
        self.labeled_data = df
        
    def split_data(self,df):    
        # 7. Train/Test split
        self.train_data, self.test_data = df.randomSplit([0.8, 0.2], seed=42)
        #return self.train_data, self.test_data
        
    def setup_pipeline(self):
        # 4. Assemble features into one vector
        assembler = VectorAssembler(inputCols=self.features, outputCol="features")

        # 5. Define model
        lr = LogisticRegression(featuresCol="features", labelCol="label")

        # 6. Create ML pipeline
        self.pipeline = Pipeline(stages=[assembler, lr])
        
    def train_model(self):
        self.trained_model = self.pipeline.fit(self.train_data)
        return self.trained_model
    
    def evaluate_model(self):
        predictions = self.trained_model.transform(self.test_data)
        evaluator = BinaryClassificationEvaluator(labelCol="label")
        accuracy = evaluator.evaluate(predictions)

        print(f" Accuracy: {accuracy:.2%}")

    def save_model(self,ticker):
        # 10. Save the trained model
        self.model.write().overwrite().save(f"trained_models/{ticker}_bullbear_classifier")





if __name__ == "__main__":
    print("Starting the Pipeline..")