
name := "merge"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-encoding", "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused"
)

libraryDependencies ++= Seq(
  "com.github.seratch" %% "awscala" % "0.6.+",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "com.github.pureconfig" %% "pureconfig" % "0.9.1"
)

val circeVersion = "0.9.3"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}