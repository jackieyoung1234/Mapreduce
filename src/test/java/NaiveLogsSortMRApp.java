import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class NaiveLogsSortMRApp {

    /*
    ** test positive number
     */
    @Test
    public void naiveLogsMapperTest() throws IOException{
       Text value = new Text("20478");
       new MapDriver<LongWritable,Text,IntWritable,NullWritable>()
               .withMapper(new LogFilesSort.LogsSortMapper())
               .withInput(new LongWritable(0),value)
               .withOutput(new IntWritable(20478),NullWritable.get())
               .runTest();
    }

    /*
    ** test negative number
     */
    @Test
    public void naiveLogsMapperTest2() throws IOException{
       Text value = new Text("-20478");
       new MapDriver<LongWritable,Text,IntWritable,NullWritable>()
               .withMapper(new LogFilesSort.LogsSortMapper())
               .withInput(new LongWritable(0),value)
               .withOutput(new IntWritable(-20478),NullWritable.get())
               .runTest();
    }

    
    /*
    ** reducer test
     */
    @Test
    public void naiveLogsReducerTest() throws IOException{
       new ReduceDriver<IntWritable,NullWritable,Text,NullWritable>()
               .withReducer(new LogFilesSort.LogsSortReducer())
               .withInput(new IntWritable(7),
                       Arrays.asList(NullWritable.get(),NullWritable.get())
               )
               .withOutput(
                       new Text("0 : at time 7 2 item(s) recorded."),
                       NullWritable.get()
               )
               .runTest();
    }

}
