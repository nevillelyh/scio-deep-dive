import sbt._
import Keys._

val scioVersion = "0.7.4"
val beamVersion = "2.12.0"
val scalaMacrosVersion = "2.1.1"

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization          := "com.spotify",
  // Semantic versioning http://semver.org/
  version               := "0.1.0-SNAPSHOT",
  scalaVersion          := "2.12.8",
  scalacOptions         ++= Seq("-target:jvm-1.8",
                                "-deprecation",
                                "-feature",
                                "-unchecked"),
  javacOptions          ++= Seq("-source", "1.8",
                                "-target", "1.8")
)

lazy val paradiseDependency =
  "org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full
lazy val macroSettings = Seq(
  libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  addCompilerPlugin(paradiseDependency)
)

lazy val root: Project = Project(
  "scio-deep-dive",
  file(".")
).settings(
  commonSettings ++ macroSettings,
  description := "scio-deep-dive",
  fork in run := true,
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % "test",
    "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    "org.slf4j" % "slf4j-simple" % "1.7.25",
    "org.hamcrest" % "hamcrest-all" % "1.3"
  )
)
