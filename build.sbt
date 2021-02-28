
name := "assignment"

version := "0.1"

scalaVersion := "2.12.4"

val sparkVersion = "3.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies +=  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.5" % "test"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value


assemblyJarName in assembly := "assignment-1.0.jar"
