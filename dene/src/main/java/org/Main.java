package org;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;

public class Main {
    public static void main(String[] args){
        //System.out.println("Hello World");

        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://192.168.56.102:9000");

            FileSystem fs = FileSystem.get(conf);

            // list files
            RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
            while (files.hasNext()) {
                System.out.println(files.next().getPath());
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}