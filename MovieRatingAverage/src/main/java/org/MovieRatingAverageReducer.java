package org;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MovieRatingAverageReducer extends Reducer<Text, FloatWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<FloatWritable> iterator = values.iterator();
        int count = 0;
        float sum = 0;
        double avg;

        while(iterator.hasNext()){
            count+=1;
            sum += iterator.next().get();
        }

        if(count==0){
            avg = 0;
        }else{
            avg = sum/(double)count;
        }

        context.write(key, new DoubleWritable(avg));
    }

}
