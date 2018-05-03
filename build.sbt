
lazy val root = (project in file("."))
  .settings(
    name := "analysis",
    commonSettings,
    assemblySettings,
    libraryDependencies ++= commonDependencies
  ).dependsOn(utils)

lazy val utils = (project in file("utils"))
	.settings(
    name := "utils",
    commonSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val merge = (project in file("merge"))
	.settings(
    name := "merge",
    commonSettings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.github.seratch" %% "awscala" % "0.6.+",
      "org.apache.spark" %% "spark-sql" % "2.3.0"
    )
  ).dependsOn(utils)

lazy val simulation = (project in file("simulation"))
	.settings(
    name := "simulation",
	  commonSettings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.github.seratch" %% "awscala" % "0.6.+",
      "org.apache.kafka" %% "kafka" % "1.1.0",
      "org.apache.commons" % "commons-compress" % "1.16.1"
    )
  ).dependsOn(utils)


lazy val commonSettings = Seq(
	organization := "org.dizhang",
	version := "0.1",
	scalaVersion := "2.11.12",
	scalacOptions ++= compilerSettings
)

lazy val compilerSettings = Seq(
  "-target:jvm-1.8",
  "-encoding",
  "UTF-8",
  "-unchecked",
  "-deprecation",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps"
)


lazy val commonDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.slf4j" % "slf4j-api" % "1.7.5" % "provided",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "provided",
  "com.github.pureconfig" %% "pureconfig" % "0.9.1"

)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  }
)