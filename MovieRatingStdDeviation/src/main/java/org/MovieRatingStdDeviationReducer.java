package org;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MovieRatingStdDeviationReducer extends Reducer<Text, FloatWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        List<Float> ratings = new ArrayList<Float>();
        int count = 0;
        float sum = 0;
        double avg;
        double sumOfSquares = 0;
        double stdDev;

        for(FloatWritable val : values){
            count +=1;
            sum += val.get();
            ratings.add(val.get());
        }

        if(count!=0){
            avg = sum/(double)count;

            for(float rating : ratings){
                sumOfSquares += Math.pow((rating-avg),2);
            }

            stdDev = Math.sqrt(sumOfSquares/(double)count);
        }else{
            stdDev = 0;
        }

        context.write(key, new DoubleWritable(stdDev));
    }
}
