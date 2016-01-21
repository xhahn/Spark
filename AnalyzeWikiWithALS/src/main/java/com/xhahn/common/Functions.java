package com.xhahn.common;


import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple6;

import java.io.*;
import java.util.*;

import static edu.umd.cloud9.collection.wikipedia.WikipediaPage.readPage;

/**
 * User: xhahn
 * Data: 2015/12/28/0028
 * Time: 14:07
 */
public class Functions implements Serializable {

    public static Function<Tuple2<LongWritable, Text>, String> getRawXmls =
            new Function<Tuple2<LongWritable, Text>, String>() {
                public String call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {
                    return longWritableTextTuple2._2().toString();
                }
            };

    public static PairFlatMapFunction<String, String, String> wikiMmlToPlainText =
            new PairFlatMapFunction<String, String, String>() {
                public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                    java.util.Vector<Tuple2<String, String>> plainText = new java.util.Vector<Tuple2<String, String>>();
                    EnglishWikipediaPage page = new EnglishWikipediaPage();

                    readPage(page, s);

                    //drop pages which is enpty or not article or redirect or disambiguation
                    if (page.isEmpty()){
                        return null;
                    }
                    else {
                        plainText.add(new Tuple2<String, String>(page.getTitle(), page.getContent()));
                        return plainText;
                    }
                }
            };
    public static Function<String, Boolean> dropNull =
            new Function<String, Boolean>() {
                public Boolean call(String v1) throws Exception {
                    return !v1.equals(null);
                }
            };
    public static Function<Tuple2<String,String>, Boolean> dropNull2 =
            new Function<Tuple2<String, String>, Boolean>() {
                public Boolean call(Tuple2<String, String> v1) throws Exception {
                    return v1!=null;
                }
            };

    public static StanfordCoreNLP createNLPPipelines() {
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        return new StanfordCoreNLP(props);
    }

    public static List<String> plainTextToLemmas(String text, HashSet<String> stopwords, StanfordCoreNLP pipeline) {
        Annotation doc = new Annotation(text);
        pipeline.annotate(doc);

        List<String> lemmas = new ArrayList<String>();
        List<CoreMap> sentences = doc.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence : sentences) {
            for (CoreMap token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                if (lemma.length() > 2 && !stopwords.contains(lemma) && isOnlyLetters(lemma)) {
                    lemmas.add(lemma.toLowerCase());
                }
            }
        }
        return lemmas;
    }

    private static boolean isOnlyLetters(String lemma) {
        int i = 0;
        while (i < lemma.length()) {
            if (!Character.isLetter(lemma.charAt(i)))
                return false;
            i++;
        }
        return true;
    }

    public static Function<Tuple2<String, List<String>>, Boolean> lessThanTWO =
            new Function<Tuple2<String, List<String>>, Boolean>() {
                public Boolean call(Tuple2<String, List<String>> v1) throws Exception {
                    return v1._2().size() > 1;
                }
            };

    public static Tuple6<JavaRDD<Vector>, Map<Integer, String>, Map<String,Integer>, Map<Long, String>,Map<String,Long>, Map<String, Double>>
    documentTermMatrix(JavaPairRDD<String, List<String>> docs, int numTerms, JavaSparkContext sc) {

        //calculate freqs of each teken in each docs
        //cashe it because it will be used several times later
        JavaPairRDD<String, HashMap<String, Integer>> docTermFreqs = docs.mapValues(getTermFreqsInDoc).cache();

        //(doc,Map<term,freqs>) => (doc) => (doc,id) => (id,doc) => Map<id,doc>
        Map<Long, String> idDocs = tomap(docTermFreqs.keys().zipWithUniqueId().mapToPair(swap).collect());

        //List(token,freqs) freqs is the number of docs that the token appear
        List<Tuple2<String, Integer>> docFreqs = docFrequencies(docTermFreqs.values(), numTerms);

        //saveDocFreqs("F:\\acm\\idea\\xhahn\\AnalyzeWikiWithALS\\out\\docfreqs.tsv", docFreqs);
        System.out.println("docFreqs: "+docFreqs.size());

        int numDocs = idDocs.size();

        Map<String, Double> idfs = inverseDocumentFrequences(docFreqs, numDocs);

        //(index,term)
        Map<Integer, String> idTerms = zipWithIndex(idfs);
        //(term,index)
        Map<String, Integer> termIds = swap(idTerms);
        //(doc,index)
        Map<String,Long> docIds = swap(idDocs);
        final Map<String, Integer> bTermIds = sc.broadcast(termIds).value();
        final Map<String, Double> bidfs = sc.broadcast(idfs).value();

        //build TF-IDF sparse vector
        JavaRDD<Vector> vecs = docTermFreqs.values().map(new Function<HashMap<String, Integer>, Vector>() {
            public Vector call(HashMap<String, Integer> v1) throws Exception {
                ArrayList<Tuple2<Integer, Double>> termScores = new ArrayList<Tuple2<Integer, Double>>();
                int totalTermsInDoc = 0;
                for (Integer numTerm : v1.values()) {
                    totalTermsInDoc += numTerm;
                }

                for (String term : v1.keySet()) {
                    if (!bTermIds.containsKey(term)) {
                        continue;
                    } else {
                        Tuple2<Integer, Double> d = new Tuple2<Integer, Double>(bTermIds.get(term), 1.0 * bidfs.get(term) * v1.get(term) / totalTermsInDoc);
                        termScores.add(d);
                    }
                }

                return Vectors.sparse(bTermIds.size(), termScores);
            }
        });
        return new Tuple6<JavaRDD<Vector>, Map<Integer, String>, Map<String,Integer>, Map<Long, String>,Map<String,Long>, Map<String, Double>>(vecs, idTerms, termIds,idDocs,docIds, idfs);

    }

    private static Map<Long, String> tomap(List<Tuple2<Long, String>> collect) {
        Map<Long,String> map = new HashMap<Long, String>();
        for(Tuple2<Long,String> t : collect){
            map.put(t._1(),t._2());
        }
        return map;
    }


    private static <T> Map<String, T> swap(Map<T, String> termIds) {
        Map<String, T> idTerms = new HashMap<String, T>();
        for (Map.Entry<T, String> entry : termIds.entrySet()) {
            idTerms.put(entry.getValue(), entry.getKey());
        }
        return idTerms;
    }

    private static Map<Integer, String> zipWithIndex(Map<String, Double> idfs) {
        Map<Integer, String> termIds = new HashMap<Integer, String>();
        int index = 0;
        for (String term : idfs.keySet()) {
            termIds.put(index, term);
            index++;
        }
        return termIds;
    }

    private static Map<String, Double> inverseDocumentFrequences(List<Tuple2<String, Integer>> docFreqs, int numDocs) {
        Map<String, Double> idfs = new HashMap<String, Double>();
        for (Tuple2<String, Integer> df : docFreqs) {
            idfs.put(df._1(), Math.log(numDocs * 1.0 / df._2()));
        }
        return idfs;
    }

    private static void saveDocFreqs(String path, List<Tuple2<String, Integer>> docFreqs) {
        try {

            File f = new File(path);
            if(f.exists())
                f.delete();
            FileOutputStream out = new FileOutputStream(path);
            PrintStream ps = new PrintStream(out);
            for (Tuple2<String, Integer> df : docFreqs) {
                ps.println(df._1() + "\t" + df._2());
            }
            ps.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * calculate number of docs that the token appear then return the top n
     * using a comarator to compare the form of tuple2,compare by each doc's number
     */
    private static List<Tuple2<String, Integer>> docFrequencies(JavaRDD<HashMap<String, Integer>> docTermFreqs, int numTerms) {
        JavaPairRDD<String, Integer> docFreqs = docTermFreqs.flatMap(getTerms).mapToPair(_1).reduceByKey(sum, 5);
        SerializableComparator<Tuple2<String, Integer>> serializableComparator = new SerializableComparator<Tuple2<String, Integer>>() {
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                return o1._2() - o2._2();
            }
        };

        return docFreqs.top(numTerms, serializableComparator);
    }

    private static Function2<Integer, Integer, Integer> sum =
            new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            };


    private static PairFunction<String, String, Integer> _1 =
            new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            };

    private static FlatMapFunction<HashMap<String, Integer>, String> getTerms =
            new FlatMapFunction<HashMap<String, Integer>, String>() {
                public Iterable<String> call(HashMap<String, Integer> stringIntegerHashMap) throws Exception {
                    java.util.Vector<String> docs = new java.util.Vector<String>();
                    for (String doc : stringIntegerHashMap.keySet()) {
                        docs.add(doc);
                    }
                    return docs;
                }
            };

    private static Function<List<String>, HashMap<String, Integer>> getTermFreqsInDoc =
            new Function<List<String>, HashMap<String, Integer>>() {
                public HashMap<String, Integer> call(List<String> v1) throws Exception {
                    HashMap<String, Integer> termFreqsInDoc = new HashMap<String, Integer>();
                    for (String term : v1) {
                        if (termFreqsInDoc.containsKey(term)) {
                            termFreqsInDoc.put(term, termFreqsInDoc.get(term) + 1);
                        } else {
                            termFreqsInDoc.put(term, 1);
                        }
                    }
                    return termFreqsInDoc;
                }
            };

    private static PairFunction<Tuple2<String, Long>, Long, String> swap =
            new PairFunction<Tuple2<String, Long>, Long, String>() {
                public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    return new Tuple2<Long, String>(stringLongTuple2._2(), stringLongTuple2._1());
                }
            };
    public static PairFunction<Tuple2<Vector, Long>, Long, Vector> SWAP =
            new PairFunction<Tuple2<Vector, Long>, Long, Vector>() {
                public Tuple2<Long, Vector> call(Tuple2<Vector, Long> stringLongTuple2) throws Exception {
                    return new Tuple2<Long, Vector>(stringLongTuple2._2(), stringLongTuple2._1());
                }
            };

    public static Function<Vector,Double> toArray =
           new Function<Vector, Double>() {
               public Double call(Vector v1) throws Exception {
                   return v1.apply(0);
               }
           };
    public static Function<Tuple2<Double,Long>,Boolean> isNaN =
            new Function<Tuple2<Double, Long>, Boolean>() {
                public Boolean call(Tuple2<Double, Long> v1) throws Exception {
                    return !v1._1().isNaN();
                }
            };
}
