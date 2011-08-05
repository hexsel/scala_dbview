name := "databasedump"

version := "1.0"

organization := "com.xmatters"

resolvers += "nexus" at "http://192.168.168.68:8080/nexus/content/groups/public/"

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "0.9.29",
    "com.oracle" % "ojdbc6" % "11.2.0.2.0"
)
