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

public class Kmeans {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length < 6) {
      System.out.println("=======================");
      System.err.println("Usage: kmeans <input> <k> <dimension> <threshold> <centroidsFilename> <output>");
      System.out.println("=======================");
      System.exit(2);
    }
    System.out.println("=======================");
    System.out.println("args[0]: <input>=" + otherArgs[0]);
    System.out.println("=======================");
    System.out.println("args[1]: <k>=" + otherArgs[1]);
    System.out.println("=======================");
    System.out.println("args[2]: <dimension>=" + otherArgs[2]);
    System.out.println("=======================");
    System.out.println("args[3]: <threshold>=" + otherArgs[3]);
    System.out.println("=======================");
    System.out.println("args[4]: <centroidsFilename>=" + otherArgs[4]);
    System.out.println("=======================");
    System.out.println("args[5]: <output>=" + otherArgs[5]);
    System.out.println("=======================");

    long start = System.currentTimeMillis();

    // Elezione di punti casuali a Centroidi
    Centroid.randomCentroidGenerator(otherArgs[0], otherArgs[1], otherArgs[2], otherArgs[4], conf);

    Path output = new Path(otherArgs[5]);
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    if (fs.exists(output)) {
      System.out.println("Delete old output folder: " + output.toString());
      fs.delete(output, true);
    }

//     Path centroidsOutput = new Path(otherArgs[4]);
//     FileSystem fs = FileSystem.get(output.toUri(),conf);

//     if (fs.exists(centroidsOutput)) {
//       System.out.println("Delete old output folder: " + output.toString());
//       fs.delete(centroidsOutput, true);
//     }

    //createCentroids(conf, new Path(otherArgs[4]), Integer.parseInt(otherArgs[2]));

    System.out.println("=======================");
    System.out.println("FIRST CENTROIDS");
    System.out.println("=======================");

    //readCentroids(conf, new Path(otherArgs[4]));

    long convergedCentroids = 0;
    int k = Integer.parseInt(args[1]);
    long iterations = 0;

    /*
    * GS: Aggiungere Condizione criterio di arresto basata su n° Iterazioni
    * */

    while (convergedCentroids < k)
    {
      System.out.println("=======================");
      System.out.println("    ITERATION:    " + (iterations + 1));
      System.out.println("    CONVERGED CENTROIDS:    " + convergedCentroids);
      System.out.println("=======================");

      if (fs.exists(output)) {
        System.out.println("=======================");
        System.out.println("DELETE OLD OUTPUT FOLDER: " + output.toString());
        System.out.println("=======================");
        fs.delete(output, true);
      }

      Job job = Job.getInstance(conf, "Kmeans Job " + (iterations + 1));

      job.getConfiguration().set("k", otherArgs[1]);
      job.getConfiguration().set("dimension", otherArgs[2]);
      job.getConfiguration().set("threshold", otherArgs[3]);
      job.getConfiguration().set("centroidsFilename", otherArgs[4]);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setJarByClass(Kmeans.class);
      job.setMapperClass(KMeansMapper.class);
      job.setReducerClass(KMeansReducer.class);
      job.setMapOutputKeyClass(Centroid.class);
      job.setMapOutputValueClass(Point.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[5]));

      job.waitForCompletion(true);

      convergedCentroids = job.getCounters().findCounter(KMeansReducer.Counter.CONVERGED_COUNT).getValue();
      iterations++;
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
