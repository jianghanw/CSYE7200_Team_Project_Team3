name := """Dota2 Win Prediction"""
organization := "https://www.northeastern.edu/"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.12.13"

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "5.1.0" % Test

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "https://www.northeastern.edu/.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "https://www.northeastern.edu/.binders._"
