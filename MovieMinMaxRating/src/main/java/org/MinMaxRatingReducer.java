package org;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MinMaxRatingReducer extends Reducer<Text, FloatWritable, Text, MinMaxTuple> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float minRating = 1000;
        float maxRating = -1;
        float rating;

        for(FloatWritable val: values){
            rating = val.get();

            if(rating<minRating){
                minRating = rating;
            }

            if (rating>maxRating){
                maxRating = rating;
            }
        }

        context.write(key, new MinMaxTuple(minRating,maxRating));
    }
}
