package ru.mai.dep806.bigdata.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class TopUsersSpark {

    static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private static Double DateToSec(String creationDateString){
        try {
            Date creationDate = dateFormat.parse(creationDateString);
            return ((double) creationDate.getTime()) / 1000;
        } catch (Exception e) {
            return 0.0;
        }
    }
    private static Long IdToLong(String Id){
        try {
            Long result = Long.parseLong(Id);
            return result;
        } catch (Exception e) {
            return 0L;
        }
    }
    public static void main(String[] args) throws IOException {

        String usersPath = args[0];
        String postsPath = args[1];
        String outPut = args[2];

        SparkConf sparkConf = new SparkConf().setAppName("Spark Top Users");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Long, String> users =
                sc.textFile(usersPath)
                .map(XmlUtils::parseXmlToMap)
                .filter(user ->
                        StringUtils.isNotBlank("Id") &&
                        StringUtils.isNotBlank("DisplayName"))
                .mapToPair(user -> new Tuple2<>(
                        IdToLong(user.get("Id")),
                        user.get("DisplayName")
                ))
                .filter(user ->
                        user._1 != 0L
                );

        JavaPairRDD<Long, Double> questions =
                sc.textFile(postsPath)
                .map(XmlUtils::parseXmlToMap)
                .filter(post ->
                        StringUtils.isNotBlank(post.get("CreationDate")) &&
                        StringUtils.isNotBlank(post.get("AcceptedAnswerId")))
                .mapToPair(post -> new Tuple2<>(
                        Long.parseLong(post.get("AcceptedAnswerId")),
                        DateToSec(post.get("CreationDate"))
                ));

        JavaPairRDD<Long, Tuple2<Double, Long>> answers =
                sc.textFile(postsPath)
                .map(XmlUtils::parseXmlToMap)
                .filter(post ->
                        StringUtils.isNotBlank(post.get("OwnerUserId")) &&
                                StringUtils.isNotBlank(post.get("CreationDate")) &&
                                StringUtils.isNotBlank(post.get("Id")) &&
                                "2".equals(post.get("PostTypeId")))
                .mapToPair(post -> new Tuple2<>(
                        Long.parseLong(post.get("Id")),
                        new Tuple2<>(
                                DateToSec(post.get("CreationDate")),
                                Long.parseLong(post.get("OwnerUserId"))
                        )
                ));

        questions.join(answers)
                .mapToPair(user->new Tuple2<>(
                        user._2._2._2,
                        new Tuple2<>(
                                1,
                                user._2._2._1 - user._2._1
                        )
                ))
                .filter(user->user._2._2 >= 5.0)
                .reduceByKey((a,b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2))
                .mapToPair(user-> new Tuple2<>(
                        user._1,
                        user._2._2/user._2._1
                ))
                .join(users)
                .mapToPair(user->new Tuple2<>(
                        user._2._1,
                        new Tuple2<>(
                                user._1,
                                user._2._2
                        )
                ))
                .sortByKey(true)
                .coalesce(1, true)
                .saveAsTextFile(outPut);;
    }
}
