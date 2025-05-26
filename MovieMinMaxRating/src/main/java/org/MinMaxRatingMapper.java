package org;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringReader;

public class MinMaxRatingMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private static final CSVFormat format = CSVFormat.DEFAULT.builder()
            .setHeader("no","userId","movie","rating","genre")
            .setSkipHeaderRecord(false)
            .setQuote('"').build();

    public void map(Object key, Text value, Context context) throws InterruptedException {
        String line = value.toString();

        if(line.contains("Movie_Name")){
            return;
        }

        try(CSVParser parser = new CSVParser(new StringReader(line), format)){
            CSVRecord record = parser.getRecords().get(0);

            context.write(
                    new Text(record.get("movie")),
                    new FloatWritable(Float.parseFloat(record.get("rating")))
            );

        }catch (IOException e){
            System.err.println("Mapper IO-Error in Reading line: "+ line + " | ErrorMessage: "+ e.getMessage());
        }catch (Exception ee){
            System.err.println("General-Error" + ee.getMessage() +  "Parsing CSV file in mapper" + line);
        }
    }

}
