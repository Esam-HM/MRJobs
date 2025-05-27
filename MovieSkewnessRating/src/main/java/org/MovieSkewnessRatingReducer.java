package org;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MovieSkewnessRatingReducer extends Reducer<Text, FloatWritable, Text, Text> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        // mean, std-dev, mode
        // sum/count , ((E(xi-mean)^2)/count)^(1/2), hashmap

        List<Float> ratingsList = new ArrayList<>();
        Map<Float,Integer> ratingsFrequentMap = new HashMap<>();
        double mean;
        double skewness;
        double stdDev;
        float mode;
        int count = 0;

        try{
            // calculate mean
            float _rating;
            float sum = 0;
            for(FloatWritable val: values){
                _rating = val.get();
                ratingsList.add(_rating);
                sum += _rating;
                count++;
                ratingsFrequentMap.put(
                        _rating, ratingsFrequentMap.getOrDefault(_rating,0)+1
                );
            }
            if(count == 0){
                mean = 0;
                stdDev = 0;
            }else{
                mean = sum/(double)count;

                // calculate stdDev
                double sumOfSquares=0;
                for(Float rating: ratingsList){
                    sumOfSquares += Math.pow(rating-mean,2);
                }
                stdDev = Math.sqrt(sumOfSquares/count);
            }

            if(stdDev == 0){
                skewness = 0;
            }else{
                // calculate mode
                mode = 0;
                float maxFrequentRating=-1;
                for(Float rating : ratingsFrequentMap.keySet()){
                    if(ratingsFrequentMap.get(rating)>maxFrequentRating){
                        mode = rating;
                        maxFrequentRating = ratingsFrequentMap.get(rating);
                    }
                }

                skewness = (mean-mode)/stdDev;
            }
            // write to local storage
            context.write(key, new Text(String.format("%.4f",skewness)));

        }catch (ArithmeticException e){
            System.out.println("Arithmetic Error happened in reducer. Message: " + e.getMessage());
        }catch (Exception e){
            System.out.println("Error happened in reducer. Message: " + e.getMessage());
        }
    }
}