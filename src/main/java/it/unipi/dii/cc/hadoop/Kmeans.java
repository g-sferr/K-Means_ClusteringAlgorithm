package it.unipi.dii.cc.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.NullWritable;

import java.util.ArrayList;
import java.util.List;

public class Kmeans
{
  public static void main(String[] args) throws Exception
  {
    //System.out.println();

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    List<Centroid> newCentroids;
    List<Centroid> oldCentroids = null;


    if (otherArgs.length < 2)
    {
      System.out.println("=======================");
      System.err.println("Usage: kmeans <input> <k> <dimension> <threshold> <centroidsFilename> <output>");
      System.out.println("=======================");
      System.exit(2);
    }

    System.out.println("=======================");
    System.out.println("args[0]: <input>=" + otherArgs[0]);
    System.out.println("=======================");
    System.out.println("args[1]: <k>=" + Config.K);
    System.out.println("=======================");
    System.out.println("args[2]: <dimension>=" + Config.DIMENSIONS);
    System.out.println("=======================");
    System.out.println("args[3]: <threshold>=" + Config.THRESHOLD);
    System.out.println("=======================");
    System.out.println("args[5]: <output>=" + otherArgs[1]);
    System.out.println("=======================");


    long start = System.currentTimeMillis();

    // Elezione di punti casuali a Centroidi
    newCentroids = Centroid.randomCentroidGenerator(otherArgs[0],
                              Config.K, Config.DIMENSIONS, conf);

    Path output = new Path(otherArgs[1]);
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    if (fs.exists(output))
    {
      System.out.println("Delete old output folder: " + output.toString());
      fs.delete(output, true);
    }

    System.out.println("=======================");
    System.out.println("FIRST CENTROIDS");
    System.out.println("=======================");

    long convergedCentroids = 0;
    long iterations = 0;
    boolean succeded = true; // Per controllare se il job è terminato correttamente

    String iterationOutputPath = "";
    String OUTPUT_FILE = otherArgs[1];

    conf.set("k", Config.K);
    conf.set("threshold", Config.THRESHOLD);
    conf.set("dimension", Config.DIMENSIONS);

    while ((convergedCentroids < Integer.parseInt(Config.K)) &&
            (iterations < Integer.parseInt(Config.MAX_ITER)))
    {
      iterations++;
      iterationOutputPath = OUTPUT_FILE + "/iteration-" + iterations;

      // Passo i centroidi ai mapper
      for ( Centroid c : newCentroids)
        conf.set("centroid_"+c.getId().toString(), c.toString());

      System.out.println("=======================");
      System.out.println("    ITERATION:    " + (iterations + 1));
      System.out.println("    CONVERGED CENTROIDS:    " + convergedCentroids);
      System.out.println("=======================");
/*
      if (fs.exists(iterationOutputPath)) {
        System.out.println("=======================");
        System.out.println("DELETE OLD OUTPUT FOLDER: " + output.toString());
        System.out.println("=======================");
        fs.delete(output, true);
      }
*/
      //Controllare se usare .getInstance oppure new Job()
      Job job = Job.getInstance(conf, "Kmeans Job " + (iterations + 1));

      //job.getConfiguration().set("centroidsFilename", otherArgs[4]);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setJarByClass(Kmeans.class);
      job.setMapperClass(KMeansMapper.class);
      job.setReducerClass(KMeansReducer.class);


      /**** _Gestione numerosità task Reducer_ ***
      int K = Integer.parseInt(otherArgs[1]); //Parametro passato contenente il valore dei k cluster scelti
      job.setCombinerClass(KMeansReducer.class); //Controllare se ha a che fare con il set dei task sotto
      job.setNumReduceTasks(K); // Un reducer per ogni cluster
      */

      job.setMapOutputKeyClass(Centroid.class);
      job.setMapOutputValueClass(Point.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(iterationOutputPath));

      succeded = job.waitForCompletion(true);

      if(!succeded)
      {
        System.err.println("Error at iteration "+iterations);
        System.exit(2);
      }

      // Sposto newCentroids in oldCenters
      for ( Centroid c : newCentroids)
        oldCentroids.add(c.copy());

      // Dopodiche recupero newCenters dal file risultato dell'iterazione corrente

      //newCentroids = updateNewCentroid();//iterazione precedente
      //newCenters = recoverResults(K, iterationOutputPath, conf);

      convergedCentroids = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED_COUNT).getValue();

      //stop = verifyStop(newCenters, oldCenters, K, EPS, MAX_ITER, i);
    }

    long end = System.currentTimeMillis();
    long elapsedTime = end - start;
    long minutes = (elapsedTime / 1000) / 60;
    long seconds = (elapsedTime / 1000) % 60;

    System.out.println("=======================");
    System.out.println(" TOTAL TIME " + minutes + " m " + seconds + "s");
    System.out.println("=======================");
    System.out.println("::FINAL CENTROIDS::");
    System.out.println("=======================");
    System.out.println("::NUMBER OF ITERATIONS:: " + iterations);
    System.out.println("=======================");
    System.out.println("::NUMBER OF CONVERGED COUNT:: " + convergedCentroids);
    System.out.println("=======================");

    // readCentroids(conf, new Path(otherArgs[4]));
  }
}



/**
 *  -- Gestione comandi hadoop --
 * */

// mvn clean package
// hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans data.txt 7 3 0.5 centroids.txt output
// cat /opt/yarn/logs/
// hadoop fs -cat output/part* | head


//THRESHOLD 0.0001
//test point n = 1.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 7 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-1k.txt 13 7 0.0001 centroids.txt output 

//test point n = 10.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 7 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-10k.txt 13 7 0.0001 centroids.txt output 


//test point n = 100.000
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 7 3 0.0001 centroids.txt output
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 13 3 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 7 7 0.0001 centroids.txt output 
//hadoop jar target/kmeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Kmeans points-100k.txt 13 7 0.0001 centroids.txt output 
