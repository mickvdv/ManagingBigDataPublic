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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Contains the mapper and reducer for phase 2 of the heat map.
 */
public class AssenHeatMapPhase2 {
    
    /**
     * This mapper is really just a dummy mapper, we actually need to reduce the result from phase1
     * right away.
     */
    public static class HeatMapPhase2Mapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            
            // key = sensor,interval
            String outputKey = tokens[0];
            String outputValue = tokens[1]; // dit is "1";
            
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }
    
    /**
     * Counts how many devices were connected with a given sensor at a a given interval.
     */
    public static class HeatMapPhase2Reducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            
            String[] tokens = key.toString().split(",");
            String sensorId = tokens[0];
            String interval = tokens[1] + "";
            
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            
            String outputKey = sensorId + "," + interval;
            
            context.write(new Text(outputKey), new Text(count + ""));
            
        }  
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: assenMapReduce <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Team 4 Assen HeatMap Phase2");
        job.setJarByClass(AssenHeatMapPhase2.class);
        job.setMapperClass(HeatMapPhase2Mapper.class);
        job.setReducerClass(HeatMapPhase2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
}
