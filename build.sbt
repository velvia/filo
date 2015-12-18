val mySettings = Seq(organization := "org.velvia.filo",
                     scalaVersion := "2.10.4") ++
                 universalSettings

lazy val schema = (project in file("schema")).dependsOn(flatbuffers)
                    .settings(mySettings:_*)
                    .settings(compileJavaSchemaTask)
                    .settings(compile <<= compile in Compile dependsOn compileJavaSchema)
                    .settings(javaSource in Compile := baseDirectory.value / "flatbuffers" / "gen-java")

lazy val flatbuffers = (project in file("flatbuffers"))
                         .settings(mySettings:_*)
                         .settings(version := "0.2.0")
                         .settings(publish := {})   // flatbuffers never changes

lazy val filoScala = (project in file("filo-scala")).dependsOn(schema, flatbuffers)
                        .settings(mySettings:_*)
                        .settings(name := "filo-scala")
                        .settings(libraryDependencies ++= deps)

lazy val filoScalaJmh = (project in file("filo-scala-jmh")).dependsOn(filoScala)
                        .settings(mySettings:_*)
                        .settings(name := "filo-scala-jmh")
                        .settings(libraryDependencies ++= deps)
                        .settings(libraryDependencies ++= Seq(jodaTime, jodaConvert))
                        .settings(jmhSettings:_*)


lazy val filo = (project in file(".")).aggregate(schema, flatbuffers, filoScala)

releaseSettings ++ publishSettings

publish := {}   // should only affect the root project.  Don't want publish to error out.

val jodaTime = "joda-time"               % "joda-time"     % "2.2"
val jodaConvert = "org.joda"                % "joda-convert"  % "1.2"

lazy val deps = Seq(
  "com.nativelibs4java"    %% "scalaxy-loops" % "0.3.3" % "provided",
  jodaTime % "provided",
  jodaConvert % "provided",
  "org.scalatest"          %% "scalatest"     % "2.1.0" % "test",
  "org.scalacheck"         %% "scalacheck"    % "1.11.0" % "test")

lazy val compileJavaSchema = taskKey[Unit]("Run flatc compiler to generate Java classes for schema")
lazy val compileJavaSchemaTask = compileJavaSchema := {
  val schemaFiles = ("ls -1 schema/flatbuffers/".!!).split("\n")
  schemaFiles.filter(_.endsWith(".fbs")).foreach { schema =>
    val result = s"flatc -j -o schema/flatbuffers/gen-java schema/flatbuffers/$schema".!!
    println(s"*** Generated Java classes from FlatBuffer schema $schema.  Results: $result")
  }
}

//////////////////////////
///

lazy val coreSettings = Seq(
  scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature"),
  // needed to disable Unsafe warning
  javacOptions ++= Seq("-XDignore.symbol.file")
)

lazy val universalSettings = coreSettings ++ styleSettings ++ releaseSettings ++
                             publishSettings

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val styleSettings = Seq(
  scalastyleFailOnError := true,
  compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
  // Is running this on compile too much?
  // (compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle
  (Keys.`package` in Compile) <<= (Keys.`package` in Compile) dependsOn compileScalastyle
)

lazy val publishSettings = bintray.Plugin.bintrayPublishSettings ++ Seq(
  licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
)
