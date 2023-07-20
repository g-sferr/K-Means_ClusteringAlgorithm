package it.unipi.dii.cc.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.ArrayList;
import java.util.List;

public class Kmeans
{
  private static int convergedCentroids = 0;

  /**
   * Verify stop condition, in particular return true if:
   *  - iterations are greater or equal to MAX_ITER
   *  - the euclidean distance between oldCentroids and newCentroids is less or equal to EPS
   *  Also count the number of converged centroids
   */
  private static boolean checkConditions(List <Centroid> newCentroids, List <Centroid> oldCentroids,
                                         int K, double EPS, int MAX_ITER, int iterations)
  {
    convergedCentroids = 0;

    // check if iterations are reached the maximum number
    if (iterations >= MAX_ITER) return true;

    // For each cluster, it checks if the distance between the old and new centroids is grater than EPS
    for (int i = 0; i < K; i++)
    {
      if(newCentroids.get(i).findEuclideanDistance(oldCentroids.get(i)) > EPS)
        return false;
      else
        convergedCentroids++;
    }

    // If in all coordinates the variation is below the epsilon, then it's time to stop
    return true;
  }

  /**
   * Retrieve from the iteration files the centroids returned by reducer in previous iteration
   */
  private static List<Centroid> retrieveResults(String OUT_FILE,
                                                Configuration conf) throws IOException
  {
    List<Centroid> toReturn = new ArrayList<>();
    FileSystem hdfs = FileSystem.get(conf);

    // status contains a list with all files in the /iteration-X folder
    FileStatus[] status = hdfs.listStatus(new Path(OUT_FILE));
    String line;

    // Check _SUCCESS
    if (!status[0].getPath().getName().startsWith("_SUCCESS"))
    {
      System.err.println("Error occurred at recovery partial files");
      System.exit(1);
    }

    // it starts from the second file because the first tells if it has succeeded or failed
    for (int i = 1; i < status.length; i++)
    {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath()))))
      {
        while ((line = reader.readLine()) != null)
        {
          String[] splittedCentroid = line.split("\t");
          toReturn.add( new Centroid(splittedCentroid[1],
                  Integer.parseInt(Config.DIMENSIONS),
                  Integer.parseInt(splittedCentroid[0])) );
        }
      }
    }
    return toReturn;
  }

  /**
   * Methods to write centroids (ID tab Coordinates) into output file
   */
  private static void writeCentroids(Configuration conf, List <Centroid> centroids,
                                     String output) throws IOException
  {
    FileSystem hdfs = FileSystem.get(conf);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(output), true)));

    // Write centroid as: [id tab coordinate newLine]
    for(Centroid c : centroids)
    {
      br.write(c.getId().get() + "\t" + (new Point(c.getCoordinates())));
      br.newLine();
    }

    br.close();
    hdfs.close();
  }

  /**
   * Methods to write execution info (printInfo) into file (outputFile)
   */
  private static void writeInfo(Configuration conf, String[] printInfo, String outputFile) throws IOException
  {
    FileSystem hdfs = FileSystem.get(conf);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(outputFile), true)));

    for(String s : printInfo)
    {
      br.write(s);
      br.newLine();
    }

    br.close();
    hdfs.close();
  }

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    List<Centroid> newCentroids;

    if (otherArgs.length < 3)
    {
      System.out.println("=======================");
      System.err.println("Usage: Kmeans <input_file_name> <output_folder_name> <n°_reducers>");
      System.out.println("=======================");
      System.exit(2);
    }

    // Parameters taken from configuration file and command line
    System.out.println("\n\n=======================");
    System.out.println(" args[0]: <input_file> = " + otherArgs[0]);
    System.out.println("=======================");
    System.out.println(" args[1]: <output_folder> = " + otherArgs[1]);
    System.out.println("=======================");
    System.out.println(" args[2]: <N°_of_reducers> = " + otherArgs[2]);
    System.out.println("=======================");
    System.out.println(" K: " + Config.K);
    System.out.println("=======================");
    System.out.println(" Dimension: " + Config.DIMENSIONS);
    System.out.println("=======================");
    System.out.println(" Max_Iterations: " + Config.MAX_ITER);
    System.out.println("=======================");
    System.out.println(" Threshold: " + Config.THRESHOLD);
    System.out.println("=======================\n\n");

    int reducerNumber = Integer.parseInt(otherArgs[2]);

    long start = System.currentTimeMillis();
    String OUTPUT_FILE = otherArgs[1];

    // Generate initial k random centroids
    newCentroids = Centroid.randomCentroidGenerator(otherArgs[0], Config.K, Config.DIMENSIONS, conf);
    // Generate initial k static centroids
    //newCentroids = Centroid.staticCentroidLoader ("static_centroids.txt", Config.DIMENSIONS, conf);

    Path output = new Path(otherArgs[1]);
    FileSystem fs = FileSystem.get(output.toUri(), conf);

    // Delete output folder
    if (fs.exists(output))
    {
      System.out.println("Delete old output folder: " + output);
      fs.delete(output, true);
    }

    // Writes initial centroids into a file .txt
    writeCentroids(conf, newCentroids, OUTPUT_FILE+"/1_initial_rand_centroids.txt");

    System.out.println("\n\n\n===============================================");
    System.out.println("\n    ***** K-MEANS ALGORITHM STARTED *****\n");
    System.out.println("===============================================\n\n");

    boolean stop = false;
    boolean succeded; // Boolean value to check status of execution
    int iterations = 0;

    String iterationOutputPath;

    conf.set("k", Config.K);
    conf.set("threshold", Config.THRESHOLD);
    conf.set("dimension", Config.DIMENSIONS);

    while (!stop)
    {
      List<Centroid> oldCentroids = new ArrayList<>();

      iterations++;
      iterationOutputPath = OUTPUT_FILE + "/iteration-" + iterations;

      // Pass centroids to mapper
      newCentroids.forEach(c -> conf.set("centroid_" + c.getId().toString(), c.toString()));

      System.out.println("\n=======================");
      System.out.println("    ITERATION:    " + (iterations));
      System.out.println("    CONVERGED CENTROIDS:  " + convergedCentroids + "/" + Config.K);
      System.out.println("=======================");
      System.out.println("\n    CURRENT CENTROIDS:");
      System.out.println("\n" + newCentroids);
      System.out.println("\n=======================\n");

      Job job = Job.getInstance(conf, "Kmeans Job " + (iterations));
      job.setJarByClass(Kmeans.class);

      //This Below are not necessary. The format is always Text, not needed to specify it
      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      // set mapper/reducer
      job.setMapperClass(KMeansMapper.class);
      job.setCombinerClass(KMeansReducer.class);
      job.setReducerClass(KMeansReducer.class);

      // *************************************************
      // ******* Pay attention to the following Note *****
      // *************************************************
      /*
      - Important:To maintain this type of combiner usage,
      by passing in job.setCombinerClass() the reducer directly,
      a correction must be made to the value of the partial sums
      for a more accurate result than the current one without this
      consideration. The app Works correctly, but is better to apply
      this update to the Source Code. One needs to add an attribute
      in the Point class that allows to keep track of the numElements
      in the various partial sums to eventually perform a weighted sum of these
      partial sums to obtain the precise and correct value of the centroid.

      - Otherwise define this behavior in a specific class "Combiner"
      and adapt it to the project implementation in such a way as to get
      the correct result and then pass this class into job.setCombinerClass()
      here in the main() and reorganize the KMeansReducer class behavior accordingly.
       */

      // set the number of reducer to parameter passed as input
      job.setNumReduceTasks(reducerNumber);

      // define reducer's output key-value
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Point.class);

      // define I/O
      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(iterationOutputPath));

      succeded = job.waitForCompletion(true);
      if(!succeded) // check if job finished successfully or not
      {
        System.err.println("Error at iteration "+iterations);
        System.exit(2);
      }

      // move newCentroids to oldCentroids
      newCentroids.forEach(c ->  oldCentroids.add(c.copy()));

      // retrieve new Centers from the previous iteration result file
      newCentroids = retrieveResults(iterationOutputPath, conf);

      int K = Integer.parseInt(Config.K);
      stop = checkConditions(newCentroids, oldCentroids, K,
              Double.parseDouble(Config.THRESHOLD),
              Integer.parseInt(Config.MAX_ITER), iterations);

    }
    // Write final centroids into file .txt
    writeCentroids(conf, newCentroids, OUTPUT_FILE+"/2_final_centroids.txt");

    long end = System.currentTimeMillis();
    long elapsedTime = end - start;
    long minutes = (elapsedTime / 1000) / 60;
    long seconds = (elapsedTime / 1000) % 60;

    String[] infos = {
            "\n  <<< With respect to the following parameter configuration >>>",
            "\t\t===============================",
            "\t\t\tCluster (K): " + Config.K,
            "\t\t\tDimension: " + Config.DIMENSIONS,
            "\t\t\tThreshold: " + Config.THRESHOLD,
            "\t\t\tMax_Iterations: " + Config.MAX_ITER,
            "\t\t\tNumber Of Reducers: " + reducerNumber,
            "\t\t===============================",
            "\n1)  Total Execution Time: " + minutes + " min " + seconds + " sec" + " ( "+((end - start)/1000)+" s )",
            "2)  Total Iterations: " + iterations,
            "3)  Number of Converged Centroids: " + convergedCentroids + "\n"
    };

    // Write final general and performance information into file .txt
    writeInfo(conf, infos, OUTPUT_FILE+"/3_info_results.txt");

    // print final information
    System.out.println("\n=======================");
    System.out.println("::TOTAL EXECUTION TIME:: " + minutes + " min " + seconds + " sec");
    System.out.println("=======================");
    System.out.println("\n::FINAL CENTROIDS::");
    System.out.println("\n" + newCentroids);
    System.out.println("\n=======================");
    System.out.println("::TOTAL ITERATIONS:: " + iterations);
    System.out.println("=======================");
    System.out.println("::NUMBER OF CONVERGED CENTROIDS:: " + convergedCentroids);
    System.out.println("=======================\n");
  }
}