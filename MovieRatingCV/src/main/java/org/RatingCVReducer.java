package org;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RatingCVReducer extends Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        List<Float> ratingsList = new ArrayList<>();
        double stdDev;
        double mean;
        double CV;
        int count = 0;

        try{
            // calculate mean
            float ratingsSum = 0;
            for(FloatWritable val: values){
                ratingsSum += val.get();
                ratingsList.add(val.get());
                count++;
            }

            if (count==0 || ratingsSum==0){
                CV = 0;
            }else{
                mean = ratingsSum/(double)count;

                double sumOfSquares = 0;
                for (Float rating: ratingsList){
                    sumOfSquares += Math.pow(rating-mean,2);
                }
                stdDev = Math.sqrt(sumOfSquares/count);

                CV = (stdDev/mean)*100;
            }

            context.write(key, new Text(String.format("%.4f",CV)+"%"));

        }catch (Exception e){
            System.out.println("Error Happened in Reducer. Message: "+ e.getMessage());
        }
    }
}
