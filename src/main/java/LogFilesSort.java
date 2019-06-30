import java.io.IOException;

import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogFilesSort {
    private static final String INPUT_PATH = "LogsSortInput";
    private static final String OUTPUT_PATH = "LogsSortOutput";

    private static int COUNTER = 0;
    private static int TEMP = 0;
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"LogFilesSort");
        FileSystem fs = FileSystem.get(conf);

        job.setJarByClass(LogFilesSort.class);
        job.setMapperClass(LogsSortMapper.class);
        job.setReducerClass(LogsSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);

        //Hadoop automatically creat the output folder
        //if it already exists, job will fail
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class LogsSortMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            try {
                context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
            } catch(NumberFormatException n){
               return;
            }
        }
    }
    public static class LogsSortReducer extends Reducer<IntWritable, NullWritable, Text, NullWritable>{
        Text reducerOutputKey = new Text();
        @Override
        protected void reduce(IntWritable k, Iterable<NullWritable> vList, Context context)throws IOException, InterruptedException {
<<<<<<< HEAD
            int size = IterableUtils.size(vList);
            reducerOutputKey.set(""+COUNTER+" : at time "+k.toString()+" "+size+" item(s) recorded.");
            context.write(reducerOutputKey,NullWritable.get());
            COUNTER++;
=======
            reducerOutputKey.set(""+(COUNTER+= IterableUtils.size(vList))+" :"+k.toString());
            context.write(reducerOutputKey,NullWritable.get());
            //for(NullWritable niv : values){
            //    if (key.get() > TEMP) {
            //        COUNTER++;//全局排序变量
            //        TEMP = key.get();//记录当前临时值
            //    }
            //    String kk = COUNTER + "\t" + key.toString();
            //    reducerOutputKey.set(kk);
            //    context.write(reducerOutputKey, NullWritable.get());
            //}
>>>>>>> parent of ab2932a... 2
        }

    }
}
