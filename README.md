# Spark Mqtt Client

Spark Scala code which does the following:<br>
1. Streaming client which connects to a MQTT feed on a remote host<br>
2. Computes per key avg, max and min for every window of 90 seconds<br>

Execute Spark on YARN
spark-submit --class "com.spark.scala.MqttSparkClient" mqttsparkclient-1.0.jar

Work in Progress
