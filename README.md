This is the code for IML conference talk "Big Data ML" including demo of
* [SparkML](https://spark.apache.org/docs/latest/ml-guide.html) pipelines
* LightGBM on Spark using [SynapseML](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/LightGBM/Overview/)
* Model serving using SynapseML [Stress Free Serving](https://microsoft.github.io/SynapseML/docs/Deploy%20Models/Quickstart%20-%20Deploying%20a%20Classifier/)

Examples are based on [HSE Sber Hack](https://www.kaggle.com/datasets/zokost/sber-and-hse-hack?select=train.csv) dataset, 
in order to run them download the data into ./data folder and run
```shell
sbt test
```

After that you can deploy model as a REST service using
```shell
sbt run
```
By default, service is started at port 8898.

Note that [OpenMP](https://www.geeksforgeeks.org/openmp-introduction-with-installation-guide/) libraries must be installed in order to use LightGBM. 
