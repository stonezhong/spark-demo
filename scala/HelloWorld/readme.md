build:
    sbt compile && sbt package

run:
    spark-submit --class "net.stonezhong.demo.HelloWorld" $(find . -name helloworld*.jar)