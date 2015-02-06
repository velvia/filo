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

lazy val filoScala = (project in file("filo-scala")).dependsOn(schema, flatbuffers)
                        .settings(mySettings:_*)
                        .settings(name := "filo-scala")
                        .settings(libraryDependencies ++= deps)

resolvers += "Pellucid Bintray" at "http://dl.bintray.com/pellucid/maven"

lazy val deps = Seq("com.pellucid" %% "framian" % "0.3.3",
                    "org.scalatest" %% "scalatest" % "2.1.0" % "test",
                    "org.scalacheck" %% "scalacheck" % "1.11.0" % "test")

lazy val compileJavaSchema = taskKey[Unit]("Run flatc compiler to generate Java classes for schema")
lazy val compileJavaSchemaTask = compileJavaSchema := {
  val result = "flatc -j -o schema/flatbuffers/gen-java schema/flatbuffers/column.fbs".!!
  println(s"*** Generated Java classes from FlatBuffer schema\n  results: $result")
}

//////////////////////////
///

lazy val coreSettings = Seq(
  scalacOptions ++= Seq("-Xlint", "-deprecation", "-Xfatal-warnings", "-feature")
)

lazy val universalSettings = coreSettings ++ styleSettings ++ releaseSettings ++
                             publishSettings

lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

lazy val styleSettings = Seq(
  scalastyleFailOnError := true,
  compileScalastyle := org.scalastyle.sbt.ScalastylePlugin.scalastyle.in(Compile).toTask("").value,
  // Is running this on compile too much?
  (compile in Compile) <<= (compile in Compile) dependsOn compileScalastyle
)

lazy val publishSettings = bintray.Plugin.bintrayPublishSettings ++ Seq(
  licenses += ("Apache-2.0", url("http://choosealicense.com/licenses/apache/"))
)
