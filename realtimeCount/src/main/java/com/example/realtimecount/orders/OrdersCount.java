package com.example.realtimecount.orders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.function.PairFunction;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

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
        JavaDStream<Double> gmv = orderRecords.map(json -> json.get("price").asDouble()).reduce((a, b) -> a + b);
        // 打印每个批次的总金额
        gmv.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Total GMV in this Batch: " + rdd.first());
            }
        });
        
        // 这何尝不是一种转换率
        // 统计jsonObjects里order_id和timestamp两个标签出现的比例
        JavaDStream<Double> labelRatio = jsonObjects.mapToPair((PairFunction<JsonNode, Boolean, Boolean>) json -> {
            boolean hasOrderId = json.has("order_id");
            boolean hasTimestamp = json.has("timestamp");
            return new Tuple2<>(hasOrderId, hasTimestamp);
        }).map(tuple -> {
            int orderIdCount = tuple._1() ? 1 : 0;
            int timestampCount = tuple._2() ? 1 : 0;
            return new Tuple2<>(orderIdCount, timestampCount);
        }).reduce((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
        .map(tuple -> (double) tuple._1() / tuple._2());

        // 打印标签出现的比例
        labelRatio.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                System.out.println("Order ID to Timestamp Ratio: " + rdd.first());
            }
        });
        // 启动流计算
        ssc.start();

        // 等待终止
        ssc.awaitTermination();
    }
}