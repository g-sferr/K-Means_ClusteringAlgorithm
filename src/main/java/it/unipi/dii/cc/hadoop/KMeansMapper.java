package it.unipi.dii.cc.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point>
{
  private final List<Centroid> centroids = new ArrayList<>();
  private int configurationDimension; // dimension: number of components

  @Override
  protected void setup(Context context)
  {
      Configuration conf = context.getConfiguration();
      configurationDimension = Integer.parseInt(conf.get("dimension"));
      int k = Integer.parseInt(conf.get("k"));

      for(int i = 0; i < k; i++)
      {
          String[] splittedCentroid = conf.get("centroid_" + i).split(";");  // es: 0;0.1,0.2
          centroids.add( new Centroid(splittedCentroid[1], configurationDimension, Integer.parseInt(splittedCentroid[0])) );
      }
  }

  // For each point passed in 'value', we map it to the closest centroid.
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    Point point = new Point(value.toString(), configurationDimension);

    Centroid closestCentroid = new Centroid();
    Double minimumDistance = Double.MAX_VALUE;

    for (Centroid c1 : centroids) // we found the closest centroid to the passed point.
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