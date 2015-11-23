package com.spark.scala

import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.mqtt._
import org.apache.spark.SparkConf

object MqttSparkClient {

  def main(args: Array[String]) {

    // MQTT Broker Hostname and topic
    val brokerUrl = "tcp://mqtt_broker_hostname:1883"
    val topic = "mqtt_topic"

    val sparkConf = new SparkConf().setAppName("CS-Mqtt-Spark-Client")
    val ssc = new StreamingContext(sparkConf, Seconds(90))

    // Create stream
    val input_stream = MQTTUtils.createStream(ssc, brokerUrl, topic, StorageLevel.MEMORY_ONLY_2)
    
    // Cleanse input data
    val mqtt_input = input_stream.filter(line => line.contains("MyMqttTopic"))
    val mqtt_kv = mqtt_input.flatMap(x => x.split(" ")).map(x => (x(0), x(1).toFloat)).cache()
       
    // Compute max, min and avg
    val mqtt_max = mqtt_kv.reduceByKey((x, y) => (x.max(y)))
    val mqtt_min = mqtt_kv.reduceByKey((x, y) => (x.min(y)))
    val mqtt_avg = mqtt_kv.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map{ case (x, y) => (x, y._1 / y._2.toFloat) }

    // Write to HDFS
    mqtt_max.saveAsTextFiles("hdfs://nn.cs.local:8020/user/root/mqtt_spark_max");
    mqtt_min.saveAsTextFiles("hdfs://nn.cs.local:8020/user/root/mqtt_spark_min");
    mqtt_avg.saveAsTextFiles("hdfs://nn.cs.local:8020/user/root/mqtt_spark_avg");
    
    ssc.start()
    ssc.awaitTermination()
  }
}