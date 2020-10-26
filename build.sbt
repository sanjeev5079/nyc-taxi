import Build.defaultSparkSettings
scalaVersion in ThisBuild     := "2.11.11"
version                       := System.getProperty("app.build.version", "0.1.0-SNAPSHOT")
organization                  := "se.telenor"
organizationName              := "telenor"

lazy val root = Project(id="aep-elt-data-erasure", base = file(".")).settings(defaultSparkSettings)

