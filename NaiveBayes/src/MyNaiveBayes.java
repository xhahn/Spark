package com.xhahn.mySparkNaiveBayes;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by root on 15-10-21.
 */
public class MyNaiveBayes implements Serializable{
    private JavaRDD<String> trainData;
    public MyNaiveBayes(JavaRDD<String> trainData){
        this.trainData = trainData;
    }

    public MyNaivebayesModle train(JavaSparkContext sc){
        //RDD<calss,numberOfFile>,number of file in each class,for p(c)
        //RDD<class::token::numoffile> => RDD<class,numoffile>
        JavaPairRDD<String,Integer> numOfFile = trainData.mapToPair(Functions.SPLIT_TO_CALSS_NUMOFFILE).cache();

        //sum of files
        final int sum = numOfFile.values().reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //System.out.println(sum);

        //RDD<class,pc>,calculate pc of each class
        JavaPairRDD<String,Double> pc = numOfFile.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<String, Double>(stringIntegerTuple2._1(), Math.log(1.0 * stringIntegerTuple2._2() / sum));
            }
        });

        //RDD<class,token>,the collect of tokens in each class
        //RDD<class::token::numoffile> => RDD<class,token>
        JavaPairRDD<String,String> tokens = trainData.mapToPair(Functions.PAIR_TO_CLASS_TOKEN).flatMapToPair(Functions.FLAT_TO_CLASS_TOKEN).filter(Functions.DROP_NUMBERS).cache();
        //System.out.println(tokens.collect());

        //directory B
        final long B = tokens.values().distinct().count();
        // final Broadcast<Long> b = sc.broadcast(B);
        //System.out.println(B);

        //RDD<class,N+B>,number of token in each class,N+B
        //RDD<class,token> => RDD<class,1> => RDD<class,N> => RDD<class,B+N>
        JavaPairRDD<String,Long> NB = tokens.mapToPair(Functions.COUNT_TOTALNUMBER_OF_TOKENS_CLASS).reduceByKey(Functions.COUNT).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<String, Long>(stringIntegerTuple2._1(), stringIntegerTuple2._2() + B);
            }
        });

        //System.out.println(NB.collect());

        //RDD<class,iterable<(token,n)>>,number of each token in each class
        //RDD<class,token> => RDD<class::token,1> => RDD<class::token,n> => RDD<class,token::n> => RDD<class,iterable<(token,n)>>
        JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> n = tokens.mapToPair(Functions.COUNT_NUMBER_OF_EACHTOKEN).reduceByKey(Functions.COUNT).mapToPair(Functions.MAP_TO_TOKEN_CLASSANDNUMBER).groupByKey();
        //System.out.println(n.take(1));



        //RDD<class,iterable<(token,n)>> => RDD<class,(iterable<(token,n),N+B)>
        JavaPairRDD<String,Tuple2<Iterable<Tuple2<String,Integer>>,Long>> n_NB = n.join(NB);
        //System.out.println(pc_n_NB.take(1));

        //RDD<class,iterable<(token,probility)>>
        //RDD<class,(iterable<(token,n),N+B)> => RDD<class,iterable<(token,(n+1)/(N+B))>>
        JavaPairRDD<String,Iterable<Tuple2<String,Double>>> probility = n_NB.mapToPair(Functions.CALCULATE_PC_n_NB);

        //RDD<class,(iterable<(token,probility)>,pc)>
        JavaPairRDD<String,Tuple2<Iterable<Tuple2<String,Double>>,Double>> trainResult = probility.join(pc);


        //        JavaRDD<Tuple2 < Iterable < Tuple2 < String, Double >>, Double >> tmp = trainResult.values();
        //        System.out.println(tmp.flatMap(new FlatMapFunction<Tuple2<Iterable<Tuple2<String,Double>>,Double>, Double>() {
        //                        @Override
        //                        public Iterable<Double> call(Tuple2<Iterable<Tuple2<String, Double>>, Double> iterableDoubleTuple2) throws Exception {
        //                            Vector<Double> tmp = new Vector<Double>();
        //                            for (Tuple2<String, Double> t : iterableDoubleTuple2._1()) {
        //                                tmp.add(t._2());
        //                            }
        //                        return tmp;
        //                        }
        //                    }).reduce(new Function2<Double, Double, Double>() {
        //            @Override
        //            public Double call(Double aDouble, Double aDouble2) throws Exception {
        //                return aDouble + aDouble2;
        //            }
        //        }));
        //System.out.println(probility.take(1));


        return new MyNaivebayesModle(trainResult,sc);
    }
}
