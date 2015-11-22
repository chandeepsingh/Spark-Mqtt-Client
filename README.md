# Spark Mqtt Client

Scala code that does the following:<br>
1. Spark streaming client which subscribes to a MQTT feed being published on a remote host<br>
2. Computes per key avg, max and min for every batch of 90 seconds<br>

For Spark running on YARN this code can be executed in the following way:<br>
spark-submit --class "com.spark.scala.MqttSparkClient" mqttsparkclient-1.0.jar

<b>Component Versions: </b><br>
CDH 5.4.8<br>
Spark 1.3.0 <br>

# Work in Progress
