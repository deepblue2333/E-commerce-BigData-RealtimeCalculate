package com.example.realtimecount.sales;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SalesCount {
    public static void main(String[] args) throws InterruptedException {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SalesCount");

        // 创建StreamingContext，指定微批次间隔为5秒
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        // 连接到本地主机的9999端口，读取数据流
        JavaDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        // 解析json格式数据，统计平均客单价
        JavaDStream<JsonNode> jsonObjects = lines.map(line -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(line);
        });

        // 筛选出购买记录
        JavaDStream<JsonNode> purchaseRecords = jsonObjects.filter(json -> json.has("order_id") && "purchase".equals(json.get("action").asText()));

        // 提取出每个订单的金额并赋值初始计数
        JavaPairDStream<Long, Tuple2<Double, Long>> amountsWithCount = purchaseRecords
                .mapToPair(json -> new Tuple2<>(1L, new Tuple2<>(json.get("total").asDouble(), 1L)));

        // 计算总金额和订单总数
        JavaPairDStream<Long, Tuple2<Double, Long>> totalAmountAndCount = amountsWithCount
                .reduceByKey((t1, t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));

        // 计算平均价格
        JavaDStream<Double> averagePrice = totalAmountAndCount.map(pair -> pair._2._1 / pair._2._2);

        // 打印结果
        averagePrice.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Average Price: " + rdd.first());
            }
        });

        // 启动流计算
        ssc.start();

        // 等待终止
        ssc.awaitTermination();
    }
}
