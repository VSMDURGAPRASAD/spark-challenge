package edu.nwmissouri.prasad;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Comparator;


/**
 * Hello world!
 */
public final class App {
    private App() {
    }
    private static void process(String inputfile){
        SparkConf sparkConf = new SparkConf() .setMaster("local").setAppName("Challenge");
             JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
            
        //JavaSparkContext sparkContext =new JavaSparkContext();
        JavaRDD<String> inputFile = sparkContext.textFile(inputfile);
        JavaRDD<String> wordsFromFile = inputFile.flatMap( line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String, Integer> countData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)) .reduceByKey((x, y) -> (int) x + (int) y);
        JavaPairRDD<Integer, String> output = countData.mapToPair(p -> new Tuple2(p._2, p._1)).sortByKey(Comparator.reverseOrder());


        String outputFolder ="results";
        Path path = FileSystems.getDefault().getPath(outputFolder);
        FileUtils.deleteQuietly(path.toFile());
        output.saveAsTextFile(outputFolder);
        sparkContext.close();

    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide a argument (text file name).");
            System.exit(0);
          }
         
          process(args[0]);
    }
}
