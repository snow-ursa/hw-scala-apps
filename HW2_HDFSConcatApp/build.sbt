ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "HW2_HDFSConcatApp"
  )

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.1"
