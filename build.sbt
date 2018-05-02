lazy val commonSettings = Seq(
	organization := "org.dizhang"
	version := "0.1"
	scalaVersion := "2.11.12"
)
lazy val util = (project in file("util"))
	.settings(
	commonSettings
)
lazy val merge = (project in file("merge"))
	.settings(
	commonSettings
).dependOn(util)
lazy val simulation = (project in file("simulate"))
	.settings(
	commonSettings
).dependOn(util)
lazy val root = (project in file("."))
	.settings(
	commonSettings
).dependOn(util)


