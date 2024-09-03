package com.example.realtimecount.orders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class OrdersCount {
    public static void main(String[] args) throws InterruptedException {
        // 创建Spark配置
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("OrdersCount");

        // 创建StreamingContext，指定微批次间隔为5秒
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        // 连接到本地主机的9999端口，读取数据流
        JavaDStream<String> lines = ssc.socketTextStream("localhost", 9999);

        // 解析json格式数据
        JavaDStream<JsonNode> jsonObjects = lines.map(line -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(line);
        });

        // 筛选出订单记录
        JavaDStream<JsonNode> orderRecords = jsonObjects.filter(json -> json.has("order_id"));

        // 计算订单数量
        JavaDStream<Long> orderCount = orderRecords.count();

        // 打印订单数量
        orderCount.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Active Orders Count in this Batch: " + rdd.first());
            }
        });

        // 计算每个批次的总金额
        JavaDStream<Double> gmv = orderRecords.mapToDouble(json -> json.get("amount").asDouble()).reduce((a, b) -> a + b);
        
        // 打印每个批次的总金额
        gmv.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Total GMV in this Batch: " + rdd.first());
            }
        });

        // 计算总访问量
        JavaDStream<Long> visitCount = jsonObjects.count();
        // 计算订单转化率
        JavaDStream<Double> conversionRate = orderCount.transformToPair(count -> visitCount.map(visit -> (double) count / visit));
        // 打印订单转化率
        conversionRate.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Order Conversion Rate in this Batch: " + rdd.first());
            }
        });
        
        // 启动流计算
        ssc.start();

        // 等待终止
        ssc.awaitTermination();
    }
}