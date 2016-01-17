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
 * Outputs various stats about visitor stays. Not an actual map reduce implementation.
 */
public class AssenVisitorStayStats {
    
    /**
     * Maps the entire table to one key.
     */
    public static class VisitorStayStatsMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] tokens = value.toString().split("\t");
                Integer duration = Integer.parseInt(tokens[1]);
                context.write(new Text("key"), new IntWritable(duration));
            } catch(NumberFormatException e) {
                // Don't map
            }
        }
    }
    
    /**
     * Determines max and average stay.
     */
    public static class VistorStayStatsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int sum = 0;
            int maxVal = 0;
            
            for (IntWritable value : values) {
                int duration = value.get();
                if (duration > 0) {
                    count++;
                    sum += duration;
                    maxVal = duration > maxVal ? duration : maxVal;
                }
            }
            
            context.write(new Text("Maximum duration"), new IntWritable(maxVal));
            context.write(new Text("Average duration"), new IntWritable(sum/count));
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
        job.setJarByClass(AssenVisitorStayStats.class);
        job.setMapperClass(VisitorStayStatsMapper.class);
        job.setReducerClass(VistorStayStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
