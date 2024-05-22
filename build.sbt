name := "SparkML"

version := "0.1"

scalaVersion := "2.12.19"

//idePackagePrefix := Some("org.apache.spark.iml")

val sparkVersion = "3.3.4"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion withSources(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion withSources()
)

libraryDependencies += ("org.scalatest" %% "scalatest" % "3.2.2" % "test" withSources())

resolvers += "SynapseML" at "https://mmlspark.azureedge.net/maven"
// Please use 1.0.4 version for Spark3.2 and 1.0.4-spark3.3 version for Spark3.3
libraryDependencies += "com.microsoft.azure" %% "synapseml" % "1.0.4-spark3.3"

// Used for transliteration of MMC code descriptions
libraryDependencies += "com.ibm.icu" % "icu4j" % "75.1"

// Used for REST testing
libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.2"
