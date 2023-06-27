package it.unipi.dii.cc.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Point>
{
  private List<DoubleWritable> coordinates;

  Point() {
    this.coordinates = new ArrayList<>();
  }
  Point(final int n)
  {
    this.coordinates = new ArrayList<>();

    for (int i = 0; i < n; i++)
      this.coordinates.add(new DoubleWritable(0.0));
  }

  // Create a new Point with the passed coordinates as List
  Point(final List<DoubleWritable> coordinatesList)
  {
    this.coordinates = new ArrayList<>();

    for (final DoubleWritable element : coordinatesList)
        this.coordinates.add(new DoubleWritable(element.get()));
  }

  // Create a new Point with the passed coordinates as splitted String by ','
  Point (String coordinatesReceived, int configurationDimension)
  {
    this.coordinates = new ArrayList<>();
    String[] splittedCoordinates = coordinatesReceived.split(",");
    for(int i = 0; (i < splittedCoordinates.length) && (splittedCoordinates.length < configurationDimension); i++)
    {
      coordinates.add(new DoubleWritable(Double.parseDouble(splittedCoordinates[i])));
    }

    /* DA ELIMINARE
    Text word = new Text();
    StringTokenizer itr = new StringTokenizer(coordinatesReceived, ",");

    while (itr.hasMoreTokens() && (coordinates.size() < configurationDimension))
    {
      word.set(itr.nextToken());
      double coordinate = Double.parseDouble(word.toString());
      coordinates.add(new DoubleWritable(coordinate));
    }*/
  }

  @Override
  public void write(final DataOutput out) throws IOException
  {
    out.writeInt(this.coordinates.size());

    for (final DoubleWritable value : this.coordinates)
        out.writeDouble(value.get());
  }

  @Override
  public void readFields(final DataInput in) throws IOException
  {
    final int size = in.readInt();
    this.coordinates = new ArrayList<>();

    for (int i = 0; i < size; i++)
        this.coordinates.add(new DoubleWritable(in.readDouble()));
  }

  @Override
  public int compareTo(Point o) // Checks if 'this' and 'o' are the same point. Returns 0 if equals, else -1
  {
    if(this.coordinates.size() != o.getCoordinates().size())
      return -1;

    int notEqual = 0;

    for(int i = 0; i < this.coordinates.size(); i++)
    {
      if(Double.compare(o.getCoordinates().get(i).get(), this.getCoordinates().get(i).get()) != 0)
        notEqual = 1;
    }

    return (notEqual);
  }

  // Conversion of a Point object in a Comma Separated String
  @Override
  public String toString()
  {
    int count = 0;
    String elements = "";
    for (final DoubleWritable element : this.coordinates)
    {
      elements += element.get() + (count < (this.coordinates.size() - 1) ? "," : "");
      count++;
    }
    return elements;
  }

  List<DoubleWritable> getCoordinates() {   return this.coordinates;    }
}
