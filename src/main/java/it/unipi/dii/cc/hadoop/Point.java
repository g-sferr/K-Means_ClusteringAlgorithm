package it.unipi.dii.cc.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Point implements WritableComparable<Centroid> {
  private List<DoubleWritable> coordinates;

  Point() {
    this.coordinates = new ArrayList<DoubleWritable>();
  }

  Point(final int n) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < n; i++)
      this.coordinates.add(new DoubleWritable(0.0));

  }

  // Declare a new list and the we add to the list every coordinates
  Point(final List<DoubleWritable> coordinatesList) {
    this.coordinates = new ArrayList<DoubleWritable>();

    // coordinatesList.forEach(element -> this.coordinates.add(new DoubleWritable(element.get())));

    for (final DoubleWritable element : coordinatesList) {
        this.coordinates.add(new DoubleWritable(element.get()));
    }
  }

  Point (String coordinatesReceived, int configurationDimension)
  {
    this.coordinates = new ArrayList<DoubleWritable>();

    // List<String> s = Arrays.stream(coordinatesReceived.split(",", configurationDimension)).toList();
    // s.forEach(coordinate ->  coordinates.add(new DoubleWritable(Double.parseDouble(coordinate))));

    Text word = new Text();
    StringTokenizer itr = new StringTokenizer(coordinatesReceived, ",");
    while (itr.hasMoreTokens() && (coordinates.size() < configurationDimension))
    {
      word.set(itr.nextToken());
      Double coordinate = Double.valueOf(word.toString());
      coordinates.add(new DoubleWritable(coordinate));
    }
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(this.coordinates.size());
    // this.coordinates.forEach(value -> out.writeDouble(value.get()));

    for (final DoubleWritable value : this.coordinates) {
        out.writeDouble(value.get());
    }
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final int size = in.readInt();
    this.coordinates = new ArrayList<DoubleWritable>();

    for (int i = 0; i < size; i++) {
        this.coordinates.add(new DoubleWritable(in.readDouble()));
    }
  }

  @Override
  public String toString() {
    String elements = "";

    for (final DoubleWritable element : this.coordinates) {
        elements += element.get() + ";";
    }

    return elements;
  }

  @Override
  public int compareTo(Centroid o) {
    return 0;
  }

  List<DoubleWritable> getCoordinates() {
    return this.coordinates;
  }

  void setCoordinates(final List<DoubleWritable> newCoordinates) {
    this.coordinates = new ArrayList<DoubleWritable>();

    for (final DoubleWritable value : newCoordinates) {
        this.coordinates.add(new DoubleWritable(value.get()));
    }
  }

}


/* ***************************************************************** */
// Classe Point del progetto MapReduce
/*
public class Point implements Writable {

    private int attribute_n;
    private double[] attributes = null;
    private int points_assigned;

    // Funzione che calcola la distanza euclidea tra due punti
    public static double euclideanDistance(Point a, Point b) {
        double distance = 0.0;
        for (int i = 0; i < a.getAttributes().length; i++) {
            double diff = a.getAttributes()[i] - b.getAttributes()[i];
            distance += diff * diff;
        }
        return Math.sqrt(distance);
    }

    // Costruttore utilizzato nel caso in cui in qualsiasi momento un cluster non avesse alcun punto assegnato
    public Point()
    {
        points_assigned = 0;
    }

    // Creo un punto data la dimensione. All'inizio ogni coordinata e' 0
    public Point(int d){
        attribute_n = d;
        attributes = new double[attribute_n];
        for(int i = 0; i< attribute_n; i++) attributes[i] = 0;
        points_assigned = 0;
    }

    // Creo un punto da una stringa CSV
    public Point(String s){
        String[] fields = s.split(",");

        attribute_n = fields.length;
        points_assigned = 1;
        attributes = new double[attribute_n];

        for (int i = 0; i < attribute_n; i++) {
            attributes[i] = Double.parseDouble(fields[i]);
        }
    }

    // Creo un punto da un vettore di double
    public Point(double[] src){
        attribute_n = src.length;
        attributes = new double[attribute_n];
        for(int i = 0; i<attribute_n; i++) this.attributes[i] = src[i];
    }

    public double[] getAttributes()
    {
        return attributes;
    }

    public double getFeature(int i){
        return attributes[i];
    }

    public void preMul(){
        for(int i = 0; i < attribute_n; i++) attributes[i] *= points_assigned;
    }

    // this += toSum
    public void sum(Point toSum)
    {
        for (int i = 0; i < this.attribute_n; i++){
            this.attributes[i] += toSum.attributes[i] * toSum.points_assigned;

        }

        this.points_assigned += toSum.points_assigned;
    }
*/
/*
    public void avg()
    {
        for(int i = 0; i < attribute_n; i++)
        attributes[i] = attributes[i] / points_assigned;
    }

// Crea un nuovo punto copiando un punto gia esistente
public static Point copy(Point toCopy)
        {
        if(toCopy == null) return new Point();

        Point toReturn = new Point(toCopy.attributes);
        toReturn.points_assigned = toCopy.points_assigned;

        return toReturn;
        }

// Serializzazione
public void write(DataOutput out) throws IOException
        {
        out.writeInt(attribute_n);
        for(int i = 0; i < attribute_n; i++) out.writeDouble(attributes[i]);
        out.writeInt(points_assigned);
        }

// Deserializzazione
public void readFields(DataInput in) throws IOException
        {
        attribute_n = in.readInt();
        attributes = new double[attribute_n];

        for(int i = 0; i < attribute_n; i++) attributes[i] = in.readDouble();
        points_assigned = in.readInt();
        }

// Comparare due Punti per calcolare eps in % tra questi ultimi
public double comparator(Point o)
        {

        // Soluzione che controlla la variazione con le singole coordinate (piu' preciso quindi meno efficiente)
        double diff = 0;

        for (int i = 0; i < attribute_n; i++) {

        diff += Math.abs((this.attributes[i] - o.attributes[i]) * 100 / (Math.abs(this.attributes[i]) + 1e-9));

        }

        return (diff / attribute_n);

        }

@Override
public String toString()
        {
        StringBuilder toReturn = new StringBuilder();

        for(int i = 0; i < this.attribute_n; i++)
        {
        toReturn.append(attributes[i]);
        if(i < this.attribute_n - 1) toReturn.append(",");
        }

        return toReturn.toString();
        }
 */
