package com.xhahn.mySparkNaiveBayes;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;


public class MyNaivebayesModle implements Serializable{
    public JavaPairRDD<String,Tuple2<Iterable<Tuple2<String,Double>>,Double>> trainResult;
    public JavaSparkContext sc;
    public static Broadcast<Map<String,Tuple2<Map<String,Double>,Double>>> btrainMap;
    Map<String,Tuple2<Map<String,Double>,Double>> trainResultMap = new HashMap<String, Tuple2<Map<String, Double>, Double>>();

    public MyNaivebayesModle(JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Double>>, Double>> trainResult, JavaSparkContext sc){
        this.trainResult = trainResult;
        this.sc = sc;

        // transform RDD to Map
        //RDD<String,Tuple2<Iterable<Tuple2<String,Double>>,Double>> => RDD<Map<String,Tuple2<Map<String,Double>,Double>>> => Map<String,Tuple2<Map<String,Double>,Double>>
        trainResultMap = trainResult.mapToPair(Functions.MAP_TO_MAP).collectAsMap();
        //broadcast the trainResult
        btrainMap = sc.broadcast(trainResultMap);
    }



    public void predict(JavaRDD<String> test){

        final Map<String,Tuple2<Map<String,Double>,Double>> trainMap = btrainMap.value();

        //RDD<class=>filename,vector<token>>
        JavaPairRDD<String,Vector<String>> testData = test.mapToPair(Functions.SPLIT_TO_CLASS_TOKENS);
        //System.out.println(testData.take(2));
/*
        //RDD<realClass,preClass>
        JavaPairRDD<String,String> realAndPredict = testData.mapToPair( new PairFunction<Tuple2<String, Vector<String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Vector<String>> stringVectorTuple2) throws Exception {

                String[] t = stringVectorTuple2._1().split("=>");
                return new Tuple2<String, String>(t[0],t[1]+" => ");
                //return new Tuple2<String, String>(t[0],t[1]+" => "+predict(trainMap,stringVectorTuple2._2()));
            }
        });
        */
        //RDD<realClass,preClass>
        JavaPairRDD<String,String> realAndPredict = testData.mapToPair(Functions.PREDICT);

        JavaPairRDD<String,String> accuracy = realAndPredict.filter(Functions.IS_CORECT);
        System.out.println(accuracy.count() + " ============ " + test.count());

        //System.out.println(realAndPredict.collect());
    }


/*
    //return the name of class which get the highest predict value
    private static String predict(Map<String,Tuple2<Map<String,Double>,Double>>ttrainMap,Vector<String> testData){
        String predictClass = null;
        //get the trainResult
        Map<String,Tuple2<Map<String,Double>,Double>> trainMap = ttrainMap;
        double maxPreValue = -Double.MAX_VALUE;

        //for all the classes
        for(String classname : trainMap.keySet()) {
            Tuple2<Map<String,Double>,Double> tokenProbilityAndPc = trainMap.get(classname);
            //set initial value of preValue by P(c)
            Double preValue = tokenProbilityAndPc._2();
            //Map<token,probility>
            Map<String,Double> tokenProbility = tokenProbilityAndPc._1();
            //for all the tokens in this file,get the probility
            for (String token : testData) {
                if (tokenProbility.containsKey(token))
                    preValue += tokenProbility.get(token);
                else
                    preValue += tokenProbility.get("default");
            }
            //System.out.println(preValue+" ======== "+maxPreValue);

            //get class has the max preValue
            if(maxPreValue < preValue){
                maxPreValue = preValue;
                predictClass = classname;
            }
        }
        //System.out.println(predictClass);
        return predictClass;
    }

*/
}
