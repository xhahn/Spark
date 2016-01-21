package com.xhahn.lsa;

import breeze.linalg.DenseMatrix;
import breeze.linalg.DenseVector;
import com.xhahn.common.Functions;
import com.xhahn.common.SerializableComparator;
import com.xhahn.common.XmlInputFormat;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import scala.Tuple2;
import scala.Tuple6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

/**
 * User: xhahn
 * Data: 2015/12/28/0028
 * Time: 11:29
 */
public class AnalyzeWikiWithLSA implements Serializable{

    static final int topNum = 10;

    public static void main(String[] args) {
        final String[] queryTerms = {"ashmore", "cartier", "islands"};
        final String[] queryDocs = {"Ashmore and Cartier Islands/Geography","Keeling Islands","Deed poll"};
        final String[] queryDocsForTerm = {"indenture","abatement","alphonso"};
        int k = 100;
        int numTerms = 50000;
        double sampleSize = 0.1;
        int numConcenpts = 5;
        int numEntity = 10;
        if (args.length > 0) k = Integer.parseInt(args[0]);
        if (args.length > 1) numTerms = Integer.parseInt(args[1]);
        if (args.length > 2) sampleSize = Double.parseDouble(args[2]);
        if (args.length > 3) numConcenpts = Integer.parseInt(args[3]);
        if (args.length > 4) numEntity = Integer.parseInt(args[4]);

        SparkConf conf = new SparkConf().setAppName("LSA");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //return 1:TF-IDF matrix, 2:idTerms, 3:termIds, 4:idDocs, 5:docIds, 6:idfs
        Tuple6<JavaRDD<Vector>, Map<Integer, String>,Map<String,Integer>, Map<Long, String>,Map<String,Long>, Map<String, Double>> process = processing(numTerms, sampleSize, sc);

        JavaRDD<org.apache.spark.mllib.linalg.Vector> termDocsMatrix = process._1().cache();

        //SVD
        RowMatrix mat = new RowMatrix(termDocsMatrix.rdd());
        SingularValueDecomposition<RowMatrix, Matrix> svd = mat.computeSVD(k, true, 1e-9);

        System.out.println("singular value: " + svd.s());


        //print most important concenpts of terms and docs
        ArrayList<ArrayList<Tuple2<String, Double>>> topTermsConcepts = topTermsInTopConcepts(svd, numConcenpts, numEntity, process._2());
        ArrayList<ArrayList<Tuple2<String, Double>>> topDocsConcepts = topDocsInTopConcepts(svd, numConcenpts, numEntity, process._4());
        for (int i = 0; i < numConcenpts; i++) {
            System.out.print("Concept terms: ");
            printMostImportantConcepts(topTermsConcepts.get(i));
            System.out.println();
            System.out.print("Concept docs: ");
            printMostImportantConcepts(topDocsConcepts.get(i));
            System.out.println();
            System.out.println();

        }

       //print most relevant entity of query
        for(String term : queryTerms) {
            printTopTermsForTerms(svd, term, process._3(), process._2());
        }

        for(String doc : queryDocs) {
            printTopDocsForDoc(svd, doc, process._5(), process._4());
        }
        for(String term : queryDocsForTerm) {
            printTopDocsForTerm(svd.U(), svd.V(), term, process._3(), process._4());
        }
        sc.stop();
    }

    private static void printMostImportantConcepts(ArrayList<Tuple2<String, Double>> tops) {
        Iterator<Tuple2<String, Double>> it = tops.iterator();
        System.out.print(it.next()._1());
        while (it.hasNext()) {
            System.out.print(", " + it.next()._1());
        }
    }

    private static ArrayList<ArrayList<Tuple2<String, Double>>> topDocsInTopConcepts(SingularValueDecomposition<RowMatrix, Matrix>svd, int numConcepts, int numDocs, Map<Long, String> idDocs) {
        ArrayList<ArrayList<Tuple2<String, Double>>> topDocs = new ArrayList<ArrayList<Tuple2<String, Double>>>();
        RowMatrix u = svd.U();
        for (int i = 0; i < numConcepts; i++) {
            final int index = i;
            JavaPairRDD<Double, Long> docWeights = u.rows().toJavaRDD().map(new Function<org.apache.spark.mllib.linalg.Vector, Double>() {
                public Double call(org.apache.spark.mllib.linalg.Vector v1) throws Exception {
                    return v1.toArray()[index];
                }
            }).zipWithUniqueId();

            topDocs.add(getTopDocs(docWeights.sortByKey(false).take(numDocs), idDocs));
        }

        return topDocs;
    }

    /**
     * (weight,id) => (doc,weight)
     */
    private static ArrayList<Tuple2<String, Double>> getTopDocs(List<Tuple2<Double, Long>> docWeight, Map<Long, String> idDocs) {
        ArrayList<Tuple2<String, Double>> topDocs = new ArrayList<Tuple2<String, Double>>();
        for (Tuple2<Double, Long> dw : docWeight) {
            topDocs.add(new Tuple2<String, Double>(idDocs.get(dw._2()), dw._1()));
        }
        return topDocs;
    }


    private static ArrayList<ArrayList<Tuple2<String, Double>>> topTermsInTopConcepts(SingularValueDecomposition<RowMatrix, Matrix> svd, int numConcepts, int numTerms, Map<Integer, String> idTerms) {
        ArrayList<ArrayList<Tuple2<String, Double>>> topTerms = new ArrayList<ArrayList<Tuple2<String, Double>>>();
        Matrix v = svd.V();
        ArrayList<Tuple2<String, Double>> sortedTopTerm = new ArrayList<Tuple2<String, Double>>();
        double[] arr = v.toArray();//in column major
        for (int i = 0; i < numConcepts; i++) {
            int off = i * v.numRows();//v.numRows is the number of terms
            //sort NO.i contept's top terms
            //return the top $number terms with index
            sortedTopTerm = topSortedTermsWithIndex(Arrays.copyOfRange(arr, off, off + v.numRows()), numTerms, idTerms);
            topTerms.add(sortedTopTerm);
        }
        return topTerms;
    }

    private static ArrayList<Tuple2<String, Double>> topSortedTermsWithIndex(double[] slice, int numTerms, Map<Integer, String> idTerms) {
        ArrayList<Tuple2<Integer, Double>> termWeights = new ArrayList<Tuple2<Integer, Double>>();
        int index = 0;
        for (Double termWeight : slice) {
            termWeights.add(new Tuple2<Integer, Double>(index, termWeight));
            index++;
        }
        Comparator<Tuple2<Integer, Double>> compatator = new Comparator<Tuple2<Integer, Double>>() {
            public int compare(Tuple2<Integer, Double> o1, Tuple2<Integer, Double> o2) {
                if (o1._2() < o2._2()) {
                    return 1;
                } else {
                    return o1._2() == o2._2() ? 0 : -1;
                }
            }
        };
        Collections.sort(termWeights, compatator);
        ArrayList<Tuple2<String, Double>> topTerms = new ArrayList<Tuple2<String, Double>>();
        for (int i = 0; i < numTerms; i++) {
            topTerms.add(new Tuple2<String, Double>(idTerms.get(termWeights.get(i)._1()), termWeights.get(i)._2()));
        }
        return topTerms;
    }

    private static HashSet<String> loadStopWords() {
        File f = new File("F:\\acm\\idea\\xhahn\\AnalyzeWikiWithALS\\src\\main\\resources\\stopwords.txt");
        HashSet stopWords = new HashSet();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(f));
            String line = null;
            while ((line = reader.readLine()) != null)
                stopWords.add(line);
        } catch (Exception e) {
        }
        return stopWords;
    }
    private static HashSet<String> loadStopWords(JavaSparkContext sc) {
        JavaRDD<String> stopwords = sc.textFile("hdfs://192.168.1.254:9000/user/xhahn/stopwords.txt");
        HashSet stopWords = new HashSet();

        for(String term : stopwords.collect()){
            stopWords.add(term);
        }
        return stopWords;
    }

    /**
     * Returns an RDD of rows of the document-term matrix, a Map of column indices to terms, and a
     * Map of row IDs to document titles.
     */
    private static Tuple6<JavaRDD<Vector>, Map<Integer, String>,Map<String,Integer>, Map<Long, String>,Map<String,Long>, Map<String, Double>>
    processing(int numTerms, double sampleSize, JavaSparkContext sc) {
        String hdfsPath = "hdfs://slave04:9000/user/xhahn/middle.xml";
        String localPath = "F:\\acm\\idea\\xhahn\\AnalyzeWikiWithALS\\src\\main\\resources\\small.xml";
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");
        //get content between tag <page> and </page>
        JavaPairRDD<LongWritable, Text> kvs = sc.newAPIHadoopFile(hdfsPath, XmlInputFormat.class, LongWritable.class, Text.class, conf);
        JavaRDD<String> rawXmls = kvs.map(Functions.getRawXmls).sample(false, sampleSize, 7L);
        /*
        rawXmls.repartition(1).saveAsTextFile("hdfs://slave04:9000/user/xhahn/out");
        JavaRDD<String> rawText = sc.textFile("hdfs:slave04:9000/user/xhahn/out/part-00000").sample(false, sampleSize, 7L);
        */

        /**
         * get the plain text without tags
         * using edu.umd.Coud9
         * version 1.5.0
         */
        JavaPairRDD<String, String> plainText = rawXmls.filter(Functions.dropNull).flatMapToPair(Functions.wikiMmlToPlainText).filter(Functions.dropNull2);

        final HashSet<String> bstopWords = sc.broadcast(loadStopWords(sc)).value();

        /**
         * drop stopwords and combine words with same meaning
         * using edu.stanford.nlp
         * version 3.2.0
         */
        JavaPairRDD<String, List<String>> lemmatized0 = plainText.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, List<String>>() {
            public Iterable<Tuple2<String, List<String>>> call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                StanfordCoreNLP pipeline = Functions.createNLPPipelines();
                java.util.Vector<Tuple2<String, List<String>>> lemmas = new java.util.Vector<Tuple2<String, List<String>>>();

               List<Tuple2<String,String>> t = IteratorUtils.toList(tuple2Iterator);
                for(Tuple2<String,String>tmp : t) {
                    lemmas.add(new Tuple2<String, List<String>>(tmp._1(), Functions.plainTextToLemmas(tmp._2(), bstopWords, pipeline)));
                }
                return lemmas;
            }
        });

        JavaPairRDD<String, List<String>> lemmatized = plainText.mapToPair(new PairFunction<Tuple2<String, String>, String, List<String>>() {
            public Tuple2<String, List<String>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                StanfordCoreNLP pipeline = Functions.createNLPPipelines();

                return  new Tuple2<String, List<String>>(stringStringTuple2._1(), Functions.plainTextToLemmas(stringStringTuple2._2(), bstopWords, pipeline));

            }
        });

        //drop page whit only one word
        JavaPairRDD<String, List<String>> filtered = lemmatized.filter(Functions.lessThanTWO);

        return Functions.documentTermMatrix(filtered, numTerms, sc);
    }

    /**
     * Finds the product of a matrix and a diagonal matrix represented by a vector.
     * Breeze doesn't support efficient diagonal representations,so multiply manually.
     */
    private static breeze.linalg.DenseMatrix<Double> multiplyByDiagnalMatrix(Matrix mat, org.apache.spark.mllib.linalg.Vector
        diag) {
        double[] sArry = diag.toArray();
        double[] vArr = mat.toArray();
        Double[] r = new Double[vArr.length];
        for (int i = 0; i < vArr.length; i++) {
            r[i] = new Double(vArr[i] * sArry[i / mat.numRows()]);//V*S
        }
        return new DenseMatrix<Double>(mat.numRows(), mat.numCols(), r);
    }

    /**
     * Finds the product of a distrubited matrix and a diagnoal matrix represented by a vector
     */
    private static RowMatrix multiplyByDiagnalMatrix(RowMatrix mat, org.apache.spark.mllib.linalg.Vector diag) {
        final double[] sArr = diag.toArray();
        return  new RowMatrix(mat.rows().toJavaRDD().map(new Function<org.apache.spark.mllib.linalg.Vector, org.apache.spark.mllib.linalg.Vector>() {
            public Vector call(Vector v1) throws Exception {
                double[] vArr = v1.toArray();
                for (int i = 0; i < vArr.length; i++) {
                    vArr[i] = vArr[i] * sArr[i];
                }
                return Vectors.dense(vArr);
            }
        }).rdd());
    }

    /**
     * Return a matrix where each row is divided by its length
     */
    private static breeze.linalg.DenseMatrix<Double> rowsNormalized(breeze.linalg.DenseMatrix<Double> mat){
        breeze.linalg.DenseMatrix<Double> newMat = new breeze.linalg.DenseMatrix<Double>(mat.rows(),mat.cols(),mat.data());
        for(int r=0;r<mat.rows();r++){
            double sum = 0;
            for(int c=0;c<mat.cols();c++){
                double data = mat.valueAt(r,c);
                sum += data*data;
            }
            double length = Math.sqrt(sum);
            for(int c=0;c<mat.cols();c++){
                newMat.update(r,c,mat.valueAt(r,c)/length);
            }
        }
        return newMat;
    }

    /**
     * reuturn a distributed matrix where each row is divided by its length
     */
    private static RowMatrix rowsNormalized(RowMatrix mat){
        return new RowMatrix(mat.rows().toJavaRDD().map(new Function<org.apache.spark.mllib.linalg.Vector, org.apache.spark.mllib.linalg.Vector>() {
            public Vector call(Vector v1) throws Exception {
                double[] arr = v1.toArray();
                double sum = 0;
                for (double data : arr) {
                    sum += data * data;
                }
                double length = Math.sqrt(sum);
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = arr[i] / length;
                }
                return Vectors.dense(arr);
            }
        }).rdd());
    }

    /**
     * Selects a row from a matrix.
     */
    private static Double[] row(breeze.linalg.DenseMatrix<Double> mat,int index){
        Double[] row = new Double[mat.cols()];
        for(int i=0;i<mat.cols();i++){
            row[i] = mat.valueAt(index,i);
        }
        return row;
    }

    /**
     * select a row from a distributed matrix
     */
    private static double[] row(RowMatrix normalizedUS,Long index){
        return normalizedUS.rows().toJavaRDD().zipWithUniqueId().mapToPair(Functions.SWAP).lookup(index).get(0).toArray();
    }

    /**
     * select a row from a matrix
     */
    private static double[] row(Matrix mat,int index){
        double[] tarr = mat.toArray();
        double[] arr = new double[mat.numCols()];
        for(int i=0;i<mat.numCols();i++){
            arr[i] = tarr[i*mat.numRows()+index];
        }
        return arr;
    }

    /**
     * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
     * relevance scores to the given term.
     */
    private static List<Tuple2<Double,Integer>> topTermsForTerm(breeze.linalg.DenseMatrix<Double> normalizedVS,int termId){
        // Look up the row in VS corresponding to the given term ID
        breeze.linalg.DenseVector<Double> termRowVec = new breeze.linalg.DenseVector<Double>(row(normalizedVS,termId));
        // Compute scores against every term
        ArrayList<Tuple2<Double,Integer>>scores = mutiply(normalizedVS, termRowVec);
        Collections.sort(scores, new Comparator<Tuple2<Double, Integer>>() {
            public int compare(Tuple2<Double, Integer> o1, Tuple2<Double, Integer> o2) {
                if (o1._1() < o2._1()) {
                    return 1;
                } else {
                    return o1._1() == o2._1() ? 0 : -1;
                }
            }
        });
        // Find the terms with the highest scores
        return scores.subList(0, topNum);
    }

    private static ArrayList<Tuple2<Double,Integer>> mutiply(DenseMatrix<Double> normalizedVS, DenseVector<Double> termRowVec) {
        ArrayList<Tuple2<Double,Integer>> scores = new ArrayList<Tuple2<Double, Integer>>();
        for (int r = 0; r < normalizedVS.rows(); r++) {
            double sum = 0;
            for(int c = 0;c < normalizedVS.cols(); c++){
                sum += normalizedVS.valueAt(r,c)*termRowVec.valueAt(c);
            }
            scores.add(new Tuple2<Double, Integer>(sum,r));
        }
        return scores;
    }

    /**
     * Finds docs relevant to a doc.Returns the doc IDs and scores for the docs with highest
     * relevance scores to the ginven doc
     */
    private static List<Tuple2<Double,Long>>topDocsForDoc(RowMatrix normalizedUS,Long docId){
        // Look up the row in US corresponding to the given doc ID
        double[] docRowArr = row(normalizedUS,docId);
        Matrix docRowVct = Matrices.dense(docRowArr.length, 1, docRowArr);

        // Compute scores against every doc
        RowMatrix docScores = normalizedUS.multiply(docRowVct);

        // Find the docs with the highest scores
        return docScores.rows().toJavaRDD().map(Functions.toArray).zipWithUniqueId().filter(Functions.isNaN).top(topNum, new SerializableComparator<Tuple2<Double, Long>>() {
            public int compare(Tuple2<Double, Long> o1, Tuple2<Double, Long> o2) {
                if (o1._1() > o2._1()) {
                    return 1;
                } else {
                    return o1._1() == o2._1() ? 0 : -1;
                }
            }
        });
    }

    /**
     * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
     * relevance scores to the given term.
     */
    private static List<Tuple2<Double,Long>> topDocsForTerm(RowMatrix US,Matrix V,int termId){
        double[] tmpRowArr = row(V, termId);
        Matrix tmpRowVec = Matrices.dense(tmpRowArr.length, 1, tmpRowArr);

        // Compute scores against every doc
        RowMatrix docScores = US.multiply(tmpRowVec);

        // Find the docs with the highest scores
        return docScores.rows().toJavaRDD().map(Functions.toArray).zipWithUniqueId().top(topNum, new SerializableComparator<Tuple2<Double, Long>>() {
            public int compare(Tuple2<Double, Long> o1, Tuple2<Double, Long> o2) {
                if (o1._1() > o2._1()) {
                    return 1;
                } else {
                    return o1._1() == o2._1() ? 0 : -1;
                }
            }
        });
    }

    /*
    private static List<Tuple2<Double,Long>> topDocsForTermQuery(RowMatrix US,Matrix V,breeze.linalg.SparseVector<Double> query){
        double[] r = V.toArray();
        Double[] arr = new Double[r.length];
        for (int i = 0; i < r.length; i++) {
            arr[i] = new Double(r[i]);
        }
        breeze.linalg.DenseMatrix<Double> breezeV = new breeze.linalg.DenseMatrix<Double>(V.numRows(), V.numCols(), arr);
        breezeV.t() *query;
    }
    */

    private static void printTopTermsForTerms(SingularValueDecomposition<RowMatrix, Matrix> svd, String Term,Map<String,Integer> termIds,Map<Integer,String> idTerms){

        DenseMatrix<Double> normalizedVS = rowsNormalized(multiplyByDiagnalMatrix(svd.V(), svd.s()));
        System.out.println("printRelevantTerms: "+Term);
        printIdWeights(topTermsForTerm(normalizedVS, termIds.get(Term)), idTerms);
    }

    private static void printTopDocsForDoc(SingularValueDecomposition<RowMatrix, Matrix> svd,String doc,Map<String,Long> docIds,Map<Long,String> idDocs){
        RowMatrix normalizedUS = rowsNormalized(multiplyByDiagnalMatrix(svd.U(), svd.s()));
        System.out.println("printRelevantDocs: "+doc);
        printIdWeights(topDocsForDoc(normalizedUS, docIds.get(doc)),idDocs);
    }

    private static void printTopDocsForTerm(RowMatrix US,Matrix V,String term,Map<String,Integer> termIds,Map<Long,String> idDocs){
        System.out.println("printRelevantDocs: "+term);
        printIdWeights(topDocsForTerm(US, V, termIds.get(term)),idDocs);
    }

    private static <T> void printIdWeights(List<Tuple2<Double, T>> idWeights, Map<T, String> idEntityIds) {
        ArrayList<Tuple2<String,Double>> entityScores = new ArrayList<Tuple2<String, Double>>();
        for(Tuple2<Double,T> idWeight : idWeights){
            entityScores.add(new Tuple2<String, Double>(idEntityIds.get(idWeight._2()),idWeight._1()));
        }
        System.out.println(org.apache.commons.lang.StringUtils.join(entityScores,", "));
    }
}
