import io
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import (LinearRegression, DecisionTreeRegressor, 
                                   RandomForestRegressor, GBTRegressor)
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import SparkSession

# Используйте как путь куда сохранить модель
MODEL_PATH = 'spark_ml_model'


def process(spark, train_data, test_data):
    #train_data - путь к файлу с данными для обучения модели
    #test_data - путь к файлу с данными для оценки качества модели
    train_df = spark.read.parquet(train_data)
    test_df = spark.read.parquet(test_data)
    
    
    initial_train_df = train_df.withColumnRenamed("ctr", "label")
    initial_test_df = test_df.withColumnRenamed("ctr", "label")
    
    vector = VectorAssembler(inputCols=initial_train_df.columns[:-1], outputCol='features')

    train_df = vector.transform(initial_train_df)
    test_df = vector.transform(initial_test_df)
    
    rmse_to_model = {}


    # decision tree
    
    decision_tree = DecisionTreeRegressor()

    paramGrid = ParamGridBuilder() \
        .addGrid(decision_tree.maxDepth, [3, 5, 7]) \
        .addGrid(decision_tree.maxBins, [16, 32, 64]) \
        .addGrid(decision_tree.minInfoGain, [0.0, 0.1, 0.3]) \
        .build()
    
    tvs = TrainValidationSplit(estimator=decision_tree,
                               estimatorParamMaps=paramGrid,
                               evaluator=RegressionEvaluator(),
                               trainRatio=0.8)
    

    best_decision_tree = tvs.fit(train_df).bestModel
    
    best_decision_tree_prediction = best_decision_tree.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    best_decision_tree_rmse = evaluator.evaluate(best_decision_tree_prediction)
    
    rmse_to_model[best_decision_tree_rmse] = best_decision_tree
    

    # linear regression

    linear_reg = LinearRegression()

    paramGrid = ParamGridBuilder() \
        .addGrid(linear_reg.maxIter, [30, 40, 50]) \
        .addGrid(linear_reg.regParam, [0.3, 0.4, 0.5]) \
        .addGrid(linear_reg.elasticNetParam, [0.7, 0.8, 0.9]) \
        .build()
    
    tvs = TrainValidationSplit(estimator=linear_reg,
                               estimatorParamMaps=paramGrid,
                               evaluator=RegressionEvaluator(),
                               trainRatio=0.8)
    

    best_linear_reg = tvs.fit(train_df).bestModel
    
    best_linear_reg_prediction = best_linear_reg.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    best_linear_reg_rmse = evaluator.evaluate(best_linear_reg_prediction)

    rmse_to_model[best_linear_reg_rmse] = best_linear_reg


    # random forest

    random_forest = RandomForestRegressor()

    paramGrid = ParamGridBuilder() \
        .addGrid(linear_reg.maxIter, [30, 40, 50]) \
        .addGrid(linear_reg.regParam, [0.3, 0.4, 0.5]) \
        .addGrid(linear_reg.numTrees, [10, 20, 30]) \
        .build()
    
    tvs = TrainValidationSplit(estimator=random_forest,
                               estimatorParamMaps=paramGrid,
                               evaluator=RegressionEvaluator(),
                               trainRatio=0.8)
    

    best_random_forest = tvs.fit(train_df).bestModel
    
    best_random_forest_prediction = best_random_forest.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    best_linear_reg_rmse = evaluator.evaluate(best_random_forest_prediction)

    rmse_to_model[best_random_forest_prediction] = best_random_forest


    # GBTRegressor

    gbtr_regressor = GBTRegressor()

    paramGrid = ParamGridBuilder() \
        .addGrid(linear_reg.maxIter, [30, 40, 50]) \
        .addGrid(linear_reg.regParam, [0.3, 0.4, 0.5]) \
        .addGrid(linear_reg.numTrees, [10, 20, 30]) \
        .build()
    
    tvs = TrainValidationSplit(estimator=gbtr_regressor,
                               estimatorParamMaps=paramGrid,
                               evaluator=RegressionEvaluator(),
                               trainRatio=0.8)
    

    best_gbtr_regressor = tvs.fit(train_df).bestModel
    
    best_gbtr_regressor_prediction = best_gbtr_regressor.transform(test_df)
    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    best_gbtr_regressor_rmse = evaluator.evaluate(best_gbtr_regressor_prediction)

    rmse_to_model[best_gbtr_regressor_rmse] = best_gbtr_regressor
    
    
    max_rmse = max(list(rmse_to_model.keys()))
    print(max_rmse)
    best_model = rmse_to_model[max_rmse]
    
    pipeline = Pipeline(stages=[vector, best_model])
    p_model = pipeline.fit(initial_train_df)
    p_model.write().overwrite().save(MODEL_PATH)
    

def main(argv):
    train_data = argv[0]
    print("Input path to train data: " + train_data)
    test_data = argv[1]
    print("Input path to test data: " + test_data)
    spark = _spark_session()
    process(spark, train_data, test_data)


def _spark_session():
    return SparkSession.builder.appName('PySparkMLFitJob').getOrCreate()


if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) != 2:
        sys.exit("Train and test data are require.")
    else:
        main(arg)
