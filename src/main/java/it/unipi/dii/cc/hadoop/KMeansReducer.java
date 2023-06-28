package it.unipi.dii.cc.hadoop;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;


public class KMeansReducer extends Reducer<IntWritable, Point, IntWritable, Point>
{
    private static int dimension;
    @Override
    protected void setup(Context context)
    {
        Configuration conf = context.getConfiguration();
        dimension = Integer.parseInt(conf.get("dimension"));
    }

    // For each point in the iterable list, we compute the new meanPoint named 'meanCentroid'
    @Override
    public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException
    {
      Centroid meanCentroid = new Centroid(dimension);
      long numElements = 0;

      for (Point currentPoint : values)
      {
        meanCentroid.add(currentPoint);
        numElements++;
      }

      meanCentroid.setId(key);
      meanCentroid.calculateMean(numElements);

      Point point = new Point(meanCentroid.getCoordinates());

      context.write(key, point);
    }
  }