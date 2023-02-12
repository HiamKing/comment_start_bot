from datetime import datetime

from pyspark.sql.pandas.group_ops import DataFrame
from pyspark.ml.feature import HashingTF
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

TRAINING_COLS = ['word', 'features', 'rawPrediction', 'probability', 'process']


class SentimentAnalysisPileline:
    def __init__(self, method="multi-class", save_folder=None, load_model=False, save_model=False, pretrained_file=None) -> None:
        self.method = method
        if pretrained_file is not None:
            self.pretrained_path = save_folder + "/" + pretrained_file
        if load_model:
            print(f"Load pretrained model from {self.pretrained_path}!")
            self.pipeline = Pipeline.load(self.pretrained_path)
        else:
            print("Train model from scratch!")
            if self.method == "multi-class":
                self.clsf_model = LogisticRegression(maxIter=10, regParam=0.01, labelCol='rating')
                self.hashing_tf = HashingTF(inputCol="process", outputCol="features")
                self.pipeline = Pipeline(stages=[self.hashing_tf, self.clsf_model])
        self.save_model = save_model
        self.save_folder = save_folder

    def train(self, df: DataFrame) -> str:
        print(f"Start training model with method {self.method}...")
        self.model = self.pipeline.fit(df)
        print(f"Finish training model with method {self.method}!")

        if self.save_model:
            current_datetime = datetime.now()
            date_string = "{}{:02d}{:02d}{:02d}{:02d}{:02d}".format(current_datetime.year, current_datetime.month,
                                                                    current_datetime.day, current_datetime.hour, current_datetime.minute, current_datetime.second)

            self.pipeline.save(f"{self.save_folder}/{self.method}_{date_string}")

            print(f"Save {self.method} model at: {self.save_folder}/{self.method}_{date_string}")
            return f'{self.method}_{date_string}'

    def predict(self, df: DataFrame) -> DataFrame:
        print("Start predicting data test.")
        predictions = self.model.transform(df)

        evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="rating", metricName="f1")
        f1_score = evaluator.evaluate(predictions)
        print(f"Method: {self.method} - F1-score: {f1_score}")
        return_df = predictions.drop(*TRAINING_COLS)
        return return_df

    # def predict_with_new_example(self, comment):
    #     print(f"Predict example :{comment}")
    #     schema = StructType([
    #         StructField("content", StringType(), True)
    #     ])
    #     df = spark.createDataFrame([(comment,)], schema)

    #     predicted_example = self.model.transform(df)

    #     predicted_example.show()
