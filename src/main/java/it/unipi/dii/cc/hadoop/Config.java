package it.unipi.dii.cc.hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

    public static final String INPUT_FILE = load("INPUT_FILE");
    public static final String K = load("K");
    public static final String DIMENSIONS = load("DIMENSIONS");
    public static final String THRESHOLD = load("THRESHOLD");
    public static final String CENTROID_FILENAME = load("CENTROID_FILENAME");
    public static final String MAX_ITER = load("MAX_ITER");
    public static final String OUTPUT = load("OUTPUT");

    /**
     * Read the value associated to the key
     * @param key the property key.
     * @return value associated to the key
     */
    private static String load(String key) {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return prop.getProperty(key);
    }

}