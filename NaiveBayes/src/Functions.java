package com.xhahn.mySparkNaiveBayes;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 15-10-21.
 */
public  class Functions implements Serializable{

    public static PairFunction<String,String,Integer> SPLIT_TO_CALSS_NUMOFFILE =
            new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String s) throws Exception {
                    String[] classAndNumOfFile = s.split("::");
                    return new Tuple2<String, Integer>(classAndNumOfFile[0],Integer.parseInt(classAndNumOfFile[2]));
                }
            };

    public static PairFunction<String,String,String[]> PAIR_TO_CLASS_TOKEN =
            new PairFunction<String, String, String[]>() {
                @Override
                public Tuple2<String, String[]> call(String s) throws Exception {
                    String[] classAndToken = s.split("::");
                    String[] tokens = classAndToken[1].split("#");
                    return new Tuple2<String, String[]>(classAndToken[0],tokens);
                }
            };

    public static PairFlatMapFunction<Tuple2<String,String[]>,String,String> FLAT_TO_CLASS_TOKEN =
            new PairFlatMapFunction<Tuple2<String, String[]>, String, String>() {
                @Override
                public Iterable<Tuple2<String, String>> call(Tuple2<String, String[]> stringTuple2) throws Exception {
                    Vector<Tuple2<String,String>> classAndTokens = new Vector<Tuple2<String, String>>();
                    for(String token:stringTuple2._2()){
                        classAndTokens.add(new Tuple2(stringTuple2._1(), token));
                    }
                    return classAndTokens;
                }
            };


    public static PairFunction<Tuple2<String, String>,String, Integer> COUNT_TOTALNUMBER_OF_TOKENS_CLASS =
            new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    return new Tuple2<String, Integer>(stringStringTuple2._1(),1);
                }
            };

    public static PairFunction<Tuple2<String, String>,String, Integer> COUNT_NUMBER_OF_EACHTOKEN =
            new PairFunction<Tuple2<String, String>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    return new Tuple2<String, Integer>(stringStringTuple2._1() + "::" + stringStringTuple2._2(), 1);
                }
            };

    public static PairFunction<Tuple2<String,Integer>,String,Tuple2<String,Integer>> MAP_TO_TOKEN_CLASSANDNUMBER =
            new PairFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Tuple2<String,Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    String[] s = stringIntegerTuple2._1().split("::");
                    return new Tuple2<String, Tuple2<String, Integer>>(s[0],new Tuple2<String, Integer>(s[1],stringIntegerTuple2._2()));
                }
            };

    public static Function<Tuple2<Double,Long>,Double> CALCULATE_PCNB =
            new Function<Tuple2<Double, Long>, Double>() {
                @Override
                public Double call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                    return doubleLongTuple2._1()/doubleLongTuple2._2();
                }
            };

    public static Function2<Integer,Integer,Integer> COUNT =
            new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer+integer2;
                }
            };

    public static PairFunction<Tuple2<String,Tuple2<Iterable<Tuple2<String,Integer>>,Long>>,String,Iterable<Tuple2<String,Double>>> CALCULATE_PC_n_NB =
            new PairFunction<Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>,Long>>, String, Iterable<Tuple2<String, Double>>>() {
                @Override
                public Tuple2<String, Iterable<Tuple2<String, Double>>> call(Tuple2<String, Tuple2<Iterable<Tuple2<String, Integer>>,Long>> stringTuple2Tuple2) throws Exception {
                    Vector<Tuple2<String,Double>> probility = new Vector<Tuple2<String, Double>>();
                    for(Tuple2<String,Integer> t:stringTuple2Tuple2._2()._1()){
                        // (n+1)/(N+B)
                        double p = Math.log(1.0*(t._2()+1)/stringTuple2Tuple2._2()._2());
                        probility.add(new Tuple2<String, Double>(t._1(),p));
                    }
                    probility.add(new Tuple2<String, Double>("default",Math.log(1.0/stringTuple2Tuple2._2()._2())));
                    return new Tuple2<String, Iterable<Tuple2<String, Double>>>(stringTuple2Tuple2._1(),probility) ;
                }
            };

    public static PairFunction<String,String,Vector<String>> SPLIT_TO_CLASS_TOKENS =
            new PairFunction<String, String, Vector<String>>() {
                @Override
                public Tuple2<String, Vector<String>> call(String s) throws Exception {
                    Vector<Tuple2<String,String[]>> classTokens = new Vector<Tuple2<String, String[]>>();
                    String[] tmp = s.split("::");
                    String[] token = tmp[1].split("#");
					Vector<String> tokens = new Vector<String>();
					Pattern p = Pattern.compile("[a-zA-Z]");
					for(int i=0;i<token.length;i++){
						Matcher m = p.matcher(token[i]);
						while(m.find())
							tokens.add(token[i]);
							break;
						}
                    return new Tuple2<String, Vector<String>>(tmp[0],tokens);
                }
            };

    public static PairFunction<Tuple2<String,Tuple2<Iterable<Tuple2<String,Double>>,Double>>,String,Tuple2<Map<String,Double>,Double>> MAP_TO_MAP =
            new PairFunction<Tuple2<String, Tuple2<Iterable<Tuple2<String, Double>>, Double>>, String, Tuple2<Map<String, Double>, Double>>() {
                @Override
                public Tuple2<String, Tuple2<Map<String, Double>, Double>> call(Tuple2<String, Tuple2<Iterable<Tuple2<String, Double>>, Double>> stringTuple2Tuple2) throws Exception {
                    Map<String,Double> key = new HashMap<String, Double>();
                    //iterable<String,Double> => Map<String,Double>
                    for(Tuple2<String,Double> keyvalue:stringTuple2Tuple2._2()._1()){
                        key.put(keyvalue._1(),keyvalue._2());
                    }
                    Tuple2<Map<String,Double>,Double> value = new Tuple2<Map<String, Double>, Double>(key,stringTuple2Tuple2._2()._2());
                    return new Tuple2<String, Tuple2<Map<String, Double>, Double>>(stringTuple2Tuple2._1(),value);
                }
            };

    public static PairFunction<Tuple2<String,Vector<String>>,String,String> PREDICT =
            new PairFunction<Tuple2<String, Vector<String>>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Vector<String>> stringVectorTuple2) throws Exception {
                    String[] t = stringVectorTuple2._1().split("=>");
                    return new Tuple2<String, String>(t[0],t[1]+" => "+predict(stringVectorTuple2._2()));
                }
            };

    //return the name of class which get the highest predict value
    private static String predict(Vector<String> testData){
        String predictClass = null;
        //get the trainResult
        Map<String,Tuple2<Map<String,Double>,Double>> trainMap = MyNaivebayesModle.btrainMap.value();
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

    public static Function<Tuple2<String,String>,Boolean> DROP_NUMBERS =
            new Function<Tuple2<String, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    String isCharacter = "[a-zA-Z]";
                    Pattern pattern = Pattern.compile(isCharacter);
                    Matcher matcher = pattern.matcher(stringStringTuple2._2());
                    while (matcher.find()) {
                        return true;
                    }
                    return false;
                }
            };

    public static Function<Tuple2<String,String>,Boolean> IS_CORECT =
            new Function<Tuple2<String, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    return stringStringTuple2._1().equals(stringStringTuple2._2().split(" => ")[1]);
                }
            };


}
