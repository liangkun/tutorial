lazy val root = (project in file(".")).settings(
  name := "tutorial",
  organization := "org.tutorial",
  version := "1.0",
  scalaVersion := "2.11.6",
  scalacOptions ++= compilerOptions,
  libraryDependencies ++= libraries,
  resolvers ++= repositories,
  mainClass in assembly := Some("org.tutorial.storm.WordCount"),
  assemblyJarName in assembly := "tutorial.jar",
  assemblyMergeStrategy in assembly := {
    case "defaults.yaml" => MergeStrategy.discard
    case x if x.endsWith("MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

lazy val compilerOptions = Seq(
  "-feature",
  "-deprecation",
  "-Yresolve-term-conflict:package"
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
