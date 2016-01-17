/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Contains the mapper and reducer for calculating the duration of stays at the festival.
 */
public class AssenVisitorStay {
    
    /**
     * Maps rows from the "sensordata" table to a (MAC, timestamp) key-value pair.
     */
    public static class VisitorStayMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tokens = value.toString().split(",");
                Integer timestamp = Integer.parseInt(tokens[2]);
                if (AssenData.isDuringFestival(timestamp)) {
                    String address = tokens[3];
                    context.write(new Text(address), new IntWritable(timestamp));
                }
            } catch(NumberFormatException e) {
                // Ignore header line
            }
        }
    }
    
    /**
     * Takes all timestamps from a device, and determines the duration of "stays" of that 
     * device. A stay is defined as a sorted set of timestamps where each timestamp is no higher 
     * than 1 hour after the previous timestamp.
     */
    public static class VistorStayReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Sort the timestamps
            SortedSet<Integer> timeStamps = new TreeSet<>();
            for (IntWritable value : values) {
                timeStamps.add(value.get());
            }
            
            // Determine stay durations
            int lastTimeStamp = 0;
            int lastDuration = 0;
            for (int timeStamp : timeStamps) {
                if (lastTimeStamp == 0) {
                    lastTimeStamp = timeStamp;
                } else {
                    int delta = timeStamp - lastTimeStamp;
                    if (delta > 60*60 && lastDuration > 0) {
                        context.write(key, new IntWritable(lastDuration));
                        lastDuration = 0;
                    } else {
                        lastDuration += delta;
                    }
                    lastTimeStamp = timeStamp;
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: assenMapReduce <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Team 4 Assen Visitor");
        job.setJarByClass(AssenVisitorStay.class);
        job.setMapperClass(VisitorStayMapper.class);
        job.setReducerClass(VistorStayReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
