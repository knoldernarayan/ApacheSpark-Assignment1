name := "ApacheSpark-Assignment1"

version := "1.0"

scalaVersion :=  "2.11.4"

libraryDependencies  ++= {
                          Seq(
			        "org.apache.spark" %% "spark-core" % "2.0.0-preview"
                             )
                        }
fork := true
