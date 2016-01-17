/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Contains the mapper and reducer for phase 1 of creating the heat map.
 */
public class AssenHeatMapPhase1 {
    
    /**
     * Accepts a row from the "sensordata" table and maps it to a an (interval,MAC) key 
     * with the raw row as value. Timestamps in seconds are mapped to 10 minute intervals.
     * Data that was recorded before or after the festival is ignored.
     */
    public static class HeatMapPhase1Mapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tokens = value.toString().split(",");
                int time = Integer.parseInt(tokens[2]);
                String address = tokens[3];
                
                if (AssenData.isDuringFestival(time)){
                    int interval = time / 600; // 600 seconds = 10 min
                    String outputKey = interval + "," + address;
                    context.write(new Text(outputKey), value);
                }
            } catch (NumberFormatException e) {
                // Ignore header row
            }
        }
    }
    
    /**
     * Takes all CSV rows belonging to a certain device and interval, and determines with
     * which sensor it had the strongest connection during that time.
     */
    public static class HeatMapPhase1Reducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int interval = Integer.parseInt(key.toString().split(",")[0])*600;
            
            // For every sensor that the device was connected with, sum the mean RSSIs.
            Map<Integer, Double> rssiBySensor = new HashMap<>();
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                int sensorId = Integer.parseInt(tokens[1]);
                double rssi = Double.parseDouble(tokens[4]);
                
                Double prevSum = rssiBySensor.get(sensorId);
                if (prevSum == null) {
                    prevSum = 0d;
                }
                
                prevSum = prevSum + (100 + rssi);
                
                rssiBySensor.put(sensorId, prevSum);
            }
            
            // Find the sensor with the highest total RSSI
            int maxSensorID = 0;
            double maxRssi = Double.MIN_VALUE;
            for (int sensor : rssiBySensor.keySet()) {
                double rssi = rssiBySensor.get(sensor);
                if (rssi > maxRssi) {
                    maxSensorID = sensor;
                    maxRssi = rssi;
                }
            }
            
            // Output format: (SensorID, Interval)  1
            String outputKey = maxSensorID + "," + interval;
            context.write(new Text(outputKey), new Text("1"));
        }  
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: assenMapReduce <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Team 4 Assen HeatMap Phase1");
        job.setJarByClass(AssenHeatMapPhase1.class);
        job.setMapperClass(HeatMapPhase1Mapper.class);
        job.setReducerClass(HeatMapPhase1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
