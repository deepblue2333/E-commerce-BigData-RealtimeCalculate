package com.example.realtimecount.users;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class UsersCount {
    public static void main(String[] args) throws InterruptedException {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UsersCount");

        // 创建StreamingContext，指定微批次间隔为5秒
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        // 连接到本地主机的9999端口，读取数据流
        JavaDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        // 解析json格式数据
        JavaDStream<JsonNode> jsonObjects = lines.map(line -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(line);
        });

        // 统计活跃用户数：在特定时间段内进行过至少一次操作的用户数量
        JavaPairDStream<String, Integer> activeUsers = jsonObjects
                .mapToPair(json -> new Tuple2<>(json.get("user_id").asText(), 1))
                .reduceByKey(Integer::sum);

        activeUsers.foreachRDD(rdd -> {
            long activeUserCount = rdd.count();
            System.out.println("Active Users in this Batch: " + activeUserCount);
        });

        JavaPairDStream<String, Integer> purchaseUsers = jsonObjects
                .filter(json -> "purchase".equals(json.get("action").asText()))
                .mapToPair(json -> new Tuple2<>(json.get("user_id").asText(), 1))
                .reduceByKey(Integer::sum);

        // 计算用户转化率：访问用户转化为购买用户的比例
        activeUsers.join(purchaseUsers)
                .mapValues(tuple -> (double) tuple._2 / tuple._1)
                .foreachRDD(rdd -> {
                    rdd.collect().forEach(tuple -> System.out.printf("User ID: %s, Conversion Rate: %.2f%n", tuple._1, tuple._2));
                });
        
        // 启动流计算
        ssc.start();

        // 等待终止
        ssc.awaitTermination();
    }
}