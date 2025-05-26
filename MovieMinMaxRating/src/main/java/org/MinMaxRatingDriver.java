package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MinMaxRatingDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] _args = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(_args.length!=2){
            System.out.println("Argument Error | Argument Should Be <input> <output>");
            System.exit(1);
        }

        Job job = new Job(conf, "Movie_Min_Max_Ratings");
        job.setJarByClass(MinMaxRatingDriver.class);
        job.setMapperClass(MinMaxRatingMapper.class);
        job.setReducerClass(MinMaxRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxTuple.class);

        FileInputFormat.addInputPath(job, new Path(_args[0]));
        FileOutputFormat.setOutputPath(job, new Path(_args[1]));

        boolean status = job.waitForCompletion(true);
        if(!status){
            System.out.println(job.getJobName() + " Job Failed to Complete.");
            System.exit(-1);
        }

        System.out.println(job.getJobName() + " Job Completed Successfully.");
        System.exit(0);
    }
}
