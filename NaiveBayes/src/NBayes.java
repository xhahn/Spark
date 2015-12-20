package com.xhahn.mySparkNaiveBayes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by root on 15-10-21.
 */
public class NBayes implements Serializable{
    public static void main(String[] args) throws ClassNotFoundException{

        if(args.length < 2)
            System.err.println("you need to give trainDataPath and testDataPath!");

        SparkConf conf = new SparkConf().setAppName("NB");

        JavaSparkContext sc = new JavaSparkContext(conf);



        // the form of inputdata is :
        // nameOfClass::Token#Token#Token..#Token::numOfFile
        // ALS::hello#world#..#spark::3
        String trainDataPath = args[0];
        String testDataPath = args[1];
//        String trainDataPath = "hdfs://master:9000/xh/my/data/NaiveBayes/input/trainData.txt";
//        String testDataPath = "hdfs://master:9000/xh/my/data/NaiveBayes/input/testData.txt";
//        JavaRDD<String> trainData = sc.textFile("/home/spark/xh/data/NaiveBayes/trainData.txt");
//        JavaRDD<String> testdata = sc.textFile("/home/spark/xh/data/NaiveBayes/testData.txt");
        JavaRDD<String> trainData = sc.textFile(trainDataPath);
        JavaRDD<String> testdata = sc.textFile(testDataPath);

        MyNaiveBayes myNaiveBayes = new MyNaiveBayes(trainData);

        MyNaivebayesModle modle = myNaiveBayes.train(sc);

        modle.predict(testdata);

        sc.stop();
    }
}
