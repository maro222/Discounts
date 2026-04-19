ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "Discounts"
  )
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.43.0.0"