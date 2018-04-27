name := "de_insight"

version := "0.1"

scalaVersion := "2.12.5"

//libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.315"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3" % "2.0.0-preview-9",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided"
)
