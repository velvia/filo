val mySettings = Seq(organization := "org.velvia.filo",
                     scalaVersion := "2.10.4")

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

lazy val deps = Seq("com.pellucid" %% "framian" % "0.3.3")

lazy val compileJavaSchema = taskKey[Unit]("Run flatc compiler to generate Java classes for schema")
lazy val compileJavaSchemaTask = compileJavaSchema := {
  val result = "flatc -j -o schema/flatbuffers/gen-java schema/flatbuffers/column.fbs".!!
  println(s"*** Generated Java classes from FlatBuffer schema\n  results: $result")
}