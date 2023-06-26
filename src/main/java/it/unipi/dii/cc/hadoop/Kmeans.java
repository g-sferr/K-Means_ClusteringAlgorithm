package it.unipi.dii.cc.hadoop;

import java.io.*;
import java.util.StringTokenizer;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
  private static boolean checkConditions(List <Centroid> newCentroids, List <Centroid> oldCentroids,
                                        int K, double EPS, int MAX_ITER, int iterations)
  {
    double distance;

    // Controllo se ho raggiunto il massimo numero di iterazioni
    if (iterations >= MAX_ITER) return true;

    //  Per ogni cluster, comparo i vecchi e i nuovi centroidi
    for (int i = 0; i < K; i++)
    {
      distance = newCentroids.get(i).findEuclideanDistance(oldCentroids.get(i));
      // Se uno dei centroidi differisce dal vecchio di piu di EPS non mi fermo
      if(distance > EPS)
        return false;
    }

    // Se in tutte le coordinate la variazione e' sotto la epsilon, allora e' tempo di fermarsi
    return true;
  }

  // Recupera dai file di iterazione i centroidi restituiti dal reducer dell'iterazione precedente
  private static List<Centroid> retrieveResults(String OUT_FILE,
                                                Configuration conf) throws IOException
  {
    List<Centroid> toReturn = new ArrayList<>();

    Path path = new Path(OUT_FILE);
    FileSystem hdfs = FileSystem.get(conf);

    // status contiene una lista con tutti i file nella cartella /iteration-X
    FileStatus[] status = hdfs.listStatus(path);
    String line;

    // Check _SUCCESS
    if (!status[0].getPath().getName().startsWith("_SUCCESS"))
    {
      System.err.println("Error occurred at recovery partial files");
      System.exit(1);
    }

    // Parto dal secondo file perché il primo mi dice se ho avuto successo o se ho fallito
    for (int i = 1; i < status.length; i++)
    {
      Path file = status[i].getPath();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(file))))
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

  private static void writeCentroids(Configuration conf, List <Centroid> centroids,
                                          String output) throws IOException
  {
    FileSystem hdfs = FileSystem.get(conf);
    FSDataOutputStream outstream = hdfs.create(new Path(output), true);
    BufferedWriter br = new BufferedWriter(new OutputStreamWriter(outstream));

    // Write centroid
    for(Centroid c : centroids)
    {
      br.write(c.getId().get() + "\t" + (new Point(c.getCoordinates())).toString());
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
    List<Centroid> oldCentroids = new ArrayList<>();


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
    String OUTPUT_FILE = otherArgs[1];


    // Elezione di punti casuali a Centroidi
    newCentroids = Centroid.randomCentroidGenerator(otherArgs[0],
                              Config.K, Config.DIMENSIONS, conf);

    // Li metto in un file per confrontarli successivamente con i finali
    //writeCentroids(conf, newCentroids, OUTPUT_FILE+"/initialRand_Centroids.txt");

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

    boolean stop = false;
    boolean succeded = true; // Per controllare se il job è terminato correttamente
    long convergedCentroids = 0;
    int iterations = 0;

    String iterationOutputPath = "";

    conf.set("k", Config.K);
    conf.set("threshold", Config.THRESHOLD);
    conf.set("dimension", Config.DIMENSIONS);

    while (!stop)
    {
      iterations++;
      iterationOutputPath = OUTPUT_FILE + "/iteration-" + iterations;

      // Passo i centroidi ai mapper
      for ( Centroid c : newCentroids)
        conf.set("centroid_" + c.getId().toString(), c.toString());

      System.out.println("=======================");
      System.out.println("    ITERATION:    " + (iterations));
      System.out.println("    CONVERGED CENTROIDS:    " + convergedCentroids);
      System.out.println("=======================");

      Job job = Job.getInstance(conf, "Kmeans Job " + (iterations));

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      job.setJarByClass(Kmeans.class);
      job.setMapperClass(KMeansMapper.class);
      job.setCombinerClass(KMeansReducer.class);
      job.setReducerClass(KMeansReducer.class);

      /**** _Gestione numerosità task Reducer_ *** */
      int K = Integer.parseInt(Config.K); //Parametro passato contenente il valore dei k cluster scelti
      job.setNumReduceTasks(K); // Un reducer per ogni cluster

      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Point.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(iterationOutputPath));

      succeded = job.waitForCompletion(true);

      if(!succeded)
      {
        System.err.println("Error at iteration "+iterations);
        System.exit(2);
      }

      // Sposto newCentroids in oldCentroids
      for ( Centroid c : newCentroids)
        oldCentroids.add(c.copy());

      // Dopodiche recupero newCenters dal file risultato dell'iterazione corrente

      newCentroids = retrieveResults(iterationOutputPath, conf);

      stop = checkConditions(newCentroids, oldCentroids, K,
                            Double.parseDouble(Config.THRESHOLD),
                            Integer.parseInt(Config.MAX_ITER), iterations);

    }

    writeCentroids(conf, newCentroids, OUTPUT_FILE+"/finalCentroids.txt");

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

  }
}