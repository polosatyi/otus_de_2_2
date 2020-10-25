name := "bostoncrimes"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
//  "org.apache.spark" % "spark-sql_2.11" % "2.4.7"
  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
)
