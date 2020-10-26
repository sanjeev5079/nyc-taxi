import sbt.Keys.{publishMavenStyle, _}
import sbt.{Def, _}
import sbtassembly.AssemblyKeys.{assembly, assemblyOption}

object Build {

  lazy val sparkVersion = "2.3.2"
  lazy val defaultSparkSettings: Seq[Def.Setting[_]] = Seq(

    // Lib
    libraryDependencies ++= Seq(
      "org.rogach"           %% "scallop"            % "3.1.5",
      "com.typesafe"         % "config"              % "1.3.3",
      "org.apache.spark"     %% "spark-sql"          % sparkVersion                  % "provided",
      "org.apache.spark"     %% "spark-hive"         % sparkVersion                  % "provided",
      "com.holdenkarau"      %% "spark-testing-base" % s"${sparkVersion}_0.11.0"     % "test",
      "org.scalatest"        %% "scalatest"          % "3.0.5"                       % "test",
      "org.apache.xbean"     % "xbean-asm5-shaded"   % "3.17",
      "se.telenor"           %% "spark-common"       % "0.1.5",
      "ch.cern.sparkmeasure" % "spark-measure_2.11"  % "0.17"
  ),
    // Publish
    publishMavenStyle := true,
    // do not include repository details in generated pom file
    pomIncludeRepository := { _ => false },
    // do not publish test artifact
    Test / publishArtifact  := false,
    // do not publish the src artifact
    publishArtifact in(Compile, packageSrc) := false,
    // do not publish the normal jar, we need the assembly one
    publishArtifact in (Compile,packageBin) :=false,
    // set publish repository
    publishTo := {
      val nexus = "https://nexus.se.telenor.net/repository"
      if (isSnapshot.value){
        Some("snapshots" at s"$nexus/maven-snapshots/")
      }else {
        Some("releases" at s"$nexus//maven-releases/")
      }
    },
    // Assembly
    assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
  ) ++ addArtifact(artifact in(Compile, assembly), assembly) // publish the fat jar!
}
