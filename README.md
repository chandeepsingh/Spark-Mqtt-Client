# Spark Mqtt Client

Spark Scala code that does the following:<br>
1. Streaming client connects to a MQTT feed on a remote host<br>
2. Computes per key avg, max and min for every window of 90 seconds<br>

Can be executed in the following way for Spark running on YARN<br>
spark-submit --class "com.spark.scala.MqttSparkClient" mqttsparkclient-1.0.jar

# Work in Progress
