from datetime import datetime
from pyspark.sql.pandas.group_ops import DataFrame
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

TRAINING_COLS = ['word', 'features', 'rawPrediction', 'probability']


class LogisticRegressionPileline:
    def __init__(self, save_path=None, load_model=False, save_model=False):
        if load_model:
            print(f"Load pretrained model from {save_path}!")
            self.lr_model = LogisticRegressionModel(maxIter=10, regParam=0.01, labelCol='rating')
            self.lr_model.load(save_path)
        else:
            print("Train model from scratch!")
            self.lr_model = LogisticRegression(maxIter=10, regParam=0.01, labelCol='rating')
        self.tokenizer = Tokenizer(inputCol="content", outputCol="word")
        self.hashing_tf = HashingTF(inputCol=self.tokenizer.getOutputCol(), outputCol="features")
        self.pipeline = Pipeline(stages=[self.tokenizer, self.hashing_tf, self.lr_model])
        self.save_model = save_model

    def train(self, df: DataFrame):
        print("Start training model...")
        self.model = self.pipeline.fit(df)
        print("Finish training model!")

        if self.save_model:
            current_datetime = datetime.now()
            date_string = "{}{:02d}{:02d}{:02d}{:02d}".format(current_datetime.year, current_datetime.month,
                                                              current_datetime.day, current_datetime.hour, current_datetime.minute)

            self.model.save(f"{self.save_forder}/model_lr_{date_string}")
            print(f"Save model at: {self.save_forder}/model_lr_{date_string}")

    def predict(self, df: DataFrame) -> DataFrame:
        print("Start predicting data test.")
        predictions = self.model.transform(df)

        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="rating", metricName="f1")
        accuracy = evaluator.evaluate(predictions)
        return_df = predictions.drop(*TRAINING_COLS)
        print(f"F1: {accuracy}")
        return return_df
