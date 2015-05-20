lazy val root = (project in file(".")).settings(
  name := "tutorial",
  organization := "org.tutorial",
  version := "1.0",
  scalaVersion := "2.11.6",
  libraryDependencies ++= libraries,
  resolvers ++= repositories,
  mainClass in assembly := Some("org.tutorial.storm.Main"),
  assemblyJarName in assembly := "tutorial.jar",
  assemblyMergeStrategy in assembly := {
    case x if x.endsWith("MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

lazy val libraries = Seq(
  "com.typesafe" % "config" % "1.2.1",
  "com.typesafe.akka" %% "akka-actor" % "2.3.11",
  "org.slf4j" % "slf4j-api" % "1.7.10",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.storm" % "storm-core" % "0.9.4"
)

lazy val repositories = Seq(
)
