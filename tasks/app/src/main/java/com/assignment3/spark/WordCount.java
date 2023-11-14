package com.assignment3.spark;

import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;


public class WordCount {

    static class Filter implements FlatMapFunction<String, String>
    {
        @Override
        public Iterator<String> call(String s) {
            String[] subStrings = s.split("\\s+");
            List<String> filteredWords = new ArrayList<>();

            for (String word: subStrings){
                String cleanedWord = word.toLowerCase().replaceAll("[^a-zA-Z]", "");
                if (!cleanedWord.isEmpty()){
                    filteredWords.add(cleanedWord);
                }
            }
            return filteredWords.iterator();
        }

    }

    public static void main(String[] args) {
        String textFilePath = "hdfs://192.168.23.174:9000/sparkApp/input/pigs.txt";
        // update to HDFS url for task2
        SparkConf conf = new SparkConf().setAppName("WordCountWithSpark").setMaster("spark://192.168.23.174:7077");
         // task2: update the setMaster with your cluster master URL for executing this code on the cluster
        JavaSparkContext sparkContext =  new JavaSparkContext(conf);
        JavaRDD<String> textFile = sparkContext.textFile(textFilePath);
        JavaRDD<String> words = textFile.flatMap(new Filter());

        //key value mapping
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
        //performing Reduce on the given key value pairs
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey((a, b) -> a + b);

        //print the word count
        wordCounts.foreach(pair -> {
            System.out.println("(" + pair._1() + ":" + pair._2() + ")");
        });

        //saving the output
        wordCounts.saveAsTextFile("hdfs://192.168.23.174:9000/sparkApp/output");


        sparkContext.stop();
        sparkContext.close();
    }
}

