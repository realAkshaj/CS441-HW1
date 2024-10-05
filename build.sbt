import sbtassembly.MergeStrategy

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.4"

lazy val root = (project in file("."))
  .settings(
    name := "HailMary"
  )

//lazy val app = (project in file("app"))
//  .settings(
//    assembly / mainClass := Some("com.example.Main"),
//    // more settings here ...
//  )

name := "akuMapRed"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.3",                 // For configuration management
  "com.knuddels" % "jtokkit" % "1.1.0",
  "org.slf4j" % "slf4j-log4j12" % "2.0.2",
  "org.apache.hadoop" % "hadoop-client" % "3.3.4",    // Hadoop client
  "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-M2.1",// Deeplearning4j
  "org.apache.hadoop" % "hadoop-client" % "3.3.4",
  "org.apache.hadoop" % "hadoop-common" % "3.3.4",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.4",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.4",
  "org.apache.hadoop" % "hadoop-yarn-client" % "3.3.4",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.4",
  "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta6",
  "org.nd4j" % "nd4j-api" % "1.0.0-beta6",
  "org.nd4j" % "nd4j-native" % "1.0.0-beta6",
  "org.nd4j" % "nd4j-common" % "1.0.0-beta6",
  "org.yaml" % "snakeyaml" % "2.0",
  "com.opencsv" % "opencsv" % "5.9"
  //"org.scalanlp" % "breeze" % "2.1.0"
)



ThisBuild / assemblyMergeStrategy := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
    MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) =>

    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
