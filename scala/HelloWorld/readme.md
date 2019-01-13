build:
    sbt compile && sbt package

run:
    spark-submit --class "HelloWorld" $(find . -name helloworld*.jar)