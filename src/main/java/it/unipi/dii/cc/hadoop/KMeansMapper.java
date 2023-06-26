package it.unipi.dii.cc.hadoop;

import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
  private final List<Centroid> centroids = new ArrayList<>();
  private Text word = new Text();
  private int configurationDimension;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException
  {
      Configuration conf = context.getConfiguration();

      configurationDimension = Integer.parseInt(conf.get("dimension"));
      int k = Integer.parseInt(conf.get("k"));

      for(int i = 0; i < k; i++) // es: 0;0.1,0.2
      {
          String[] splittedCentroid = conf.get("centroid_" + i).split(";");
          centroids.add( new Centroid(splittedCentroid[1], configurationDimension,
                                        Integer.parseInt(splittedCentroid[0])) );
      }
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException,
                                                                InterruptedException
  {

    Point point = new Point(value.toString(), configurationDimension);

    Centroid closestCentroid = null;
    Double minimumDistance = Double.MAX_VALUE;

    for (Centroid c1 : centroids) // Trovo il Centroide più vicino al puntoPassato al mapper
    {
      Double distance = c1.findEuclideanDistance(point);

      if (distance < minimumDistance)
      {
        minimumDistance = distance;
        closestCentroid = c1.copy();
      }
    }
    context.write(closestCentroid.getId(), point);
  }
}