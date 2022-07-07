''''
Reads a samole dataset, trains and saves the pipeline 
using sparkML'''

from pandas import StringDtype
from pyspark import SparkContext

from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf
import re

from pyspark.ml.feature import CountVectorizer, IDF, StopWordsRemover, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pathlib import Path

sc = SparkContext()
sqlcontext = SQLContext(sc)

SRC_DIR = Path(__file__).resolve().parent

path = "/Users/Salman/Downloads/trainingandtestdata" #path to the dataset

df = sqlcontext \
    .read \
    .format('.csv') \
    .options(header=False) \
    .load(path) \
    .selectExpr("_c0 as sentiment", "_c5 as message")


#get user city from the username field 
#get the temp for the city on that date 
#add the fields to the data 


################# Tokenize the data

pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', \
        x.lower().strip()).split(), ArrayType(StringType())
    )
df = df.withColumn("cleaned_data", pre_process(df.message)).dropna()


################# Split the dataframe into training and testing
train, test = df.randomSplit([0.8,0.2],seed = 100)

################# Create an ML Pipeline
# Peforms TF-IDF calculation and Logistic Regression
remover = StopWordsRemover(inputCol="cleaned_data", outputCol="words")
vector_tf = CountVectorizer(inputCol="words", outputCol="tf")
idf = IDF(inputCol="tf", outputCol="features", minDocFreq=3)
label_indexer = StringIndexer(inputCol = "sentiment", outputCol = "label")
logistic_regression = LogisticRegression(maxIter=100)

pipeline = Pipeline(stages=[remover, vector_tf, idf, label_indexer, logistic_regression])

################# Fit the pipeline to the training dataframe
trained_model = pipeline.fit(train)

'''
The labels are labelled with positive (4) as 0.0 
negative (0) as 1.0
'''
################# Predicting the test dataframe and calculating accuracy
prediction_df = trained_model.transform(test)

evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
accuracy = evaluator.evaluate(prediction_df)
print(accuracy)

################# Save the pipeline model
model_path = SRC_DIR.joinpath('models')
trained_model.write().overwrite() \
    .save(model_path)
