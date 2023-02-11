from datetime import datetime

from pyspark.sql.pandas.group_ops import DataFrame
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


class LogisticRegressionPileline:
    def __init__(self, save_path=None, load_model=False, save_model=False):
        if load_model:
            print(f"Load pretrained model from {save_path}!")
            self.lr_model = LogisticRegression(maxIter=10, regParam=0.01, labelCol='rating')
            self.lr_model.load(save_path)
        else:
            print("Train model from scratch!")
            self.lr_model = LogisticRegression(maxIter=10, regParam=0.01, labelCol='rating')
        self.tokenizer = Tokenizer(inputCol="content", outputCol="word")
        self.hashingTF = HashingTF(inputCol=self.tokenizer.getOutputCol(), outputCol="features")
        # lr = LogisticRegression(maxIter=10, regParam=0.01, labelCol='rating')
        self.pipeline = Pipeline(stages=[self.tokenizer, self.hashingTF, self.lr_model])
        self.save_model = save_model

    def flush_to_es(self) -> None:
        pass

    def train(self, df: DataFrame):
        train_df = df.withColumn("rating", col("rating").cast("integer"))
        print("Start training model...")
        self.model = self.pipeline.fit(train_df)
        print("Finish training model!")

        if self.save_model:
            current_datetime = datetime.now()
            date_string = "{}{:02d}{:02d}{:02d}{:02d}".format(current_datetime.year, current_datetime.month,
                                                              current_datetime.day, current_datetime.hour, current_datetime.minute)

            self.model.save(f"{self.save_forder}/model_lr_{date_string}")
            print(f"Save model at: {self.save_forder}/model_lr_{date_string}")

    def predict(self, df: DataFrame):
        print("Start predicting data test.")
        predictions = self.model.transform(df)

        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="rating", metricName="f1")
        accuracy = evaluator.evaluate(predictions)
        print(f"F1: {accuracy}")
