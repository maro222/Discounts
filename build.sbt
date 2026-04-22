ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.8.3"

lazy val root = (project in file("."))
  .settings(
    name := "Discounts"
  )
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.43.0.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"

javaOptions ++= Seq(
  "-Xms512m",    // starting heap size  — allocate this upfront
  "-Xmx4g"       // maximum heap size   — never exceed this
)
fork := true     // REQUIRED — without this, javaOptions are ignored