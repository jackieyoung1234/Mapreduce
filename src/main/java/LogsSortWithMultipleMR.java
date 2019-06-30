import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;

public class LogsSortWithMultipleMR extends Configured implements Tool{
    private final String INPUT_PATH;// = "LogsSortInput";
    private static final String OUTPUT1_PATH = "job1_temp";
    private static final String OUTPUT2_PATH = "job2_metaInfo";
    private final String OUTPUT3_PATH;// = "SortOutput";

    public LogsSortWithMultipleMR(){
        INPUT_PATH = "LogsSortInput";
        OUTPUT3_PATH = "SortOutput";
    }
    public LogsSortWithMultipleMR(String inpath, String outpath){
       INPUT_PATH = inpath;
       OUTPUT3_PATH = outpath;
    }
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new LogsSortWithMultipleMR(), args);
        System.exit(result);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        //output path check
        FileSystem f = FileSystem.get(conf);
        Path job1TempPath = new Path(OUTPUT1_PATH);
        Path job2MetaInfo = new Path(OUTPUT2_PATH);
        Path sortOutput = new Path(OUTPUT3_PATH);
        outputPathCheck(f, Arrays.asList(job1TempPath,job2MetaInfo,sortOutput));

        //use multiple mappers reducers to process big files
        //in a parallel way
        Job job = Job.getInstance(conf);
        job.setNumReduceTasks(4);
        job.setJarByClass(LogsSortWithMultipleMR.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setMapperClass(PreSortWithMultipleMr.class);
        job.setReducerClass(BigNumFileSortMR_Reducer.class);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, job1TempPath);

        //use addition mr to calculate the meta info of every intermediate file
        //and put it into distributed cache
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(LogsSortWithMultipleMR.class);
        job2.setMapperClass(MetaInfoMapper.class);
        job2.setReducerClass(MetaInfoReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job2, job1TempPath);
        FileOutputFormat.setOutputPath(job2, job2MetaInfo);

        //Sort with meta info
        Job job3 = Job.getInstance(conf);
        job3.setJarByClass(LogsSortWithMultipleMR.class);
        job3.setMapperClass(SortWithMetaInfoMapper.class);
        //no reducer is used in this job
        job3.setNumReduceTasks(0);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(NullWritable.class);

        //put meta info into cache
        DistributedCache.addCacheFile(new URI(OUTPUT2_PATH+"/part-r-00000"),conf);

        FileInputFormat.setInputPaths(job3, job1TempPath);
        FileOutputFormat.setOutputPath(job3, sortOutput);

        //using controlledjob to build dependencies between jobs
        ControlledJob cjob1 = new ControlledJob(job.getConfiguration());
        ControlledJob cjob2 = new ControlledJob(job2.getConfiguration());
        ControlledJob cjob3 = new ControlledJob(job3.getConfiguration());
        JobControl jobController = new JobControl("jc");
        cjob1.setJob(job);
        cjob2.setJob(job2);
        cjob3.setJob(job3);
        jobController.addJob(cjob1);
        jobController.addJob(cjob2);
        jobController.addJob(cjob3);
        cjob2.addDependingJob(cjob1);
        cjob3.addDependingJob(cjob2);

        Thread th = new Thread(jobController);
        th.start();
        while(!jobController.allFinished()){
            th.sleep(1000);
        }
        //wait until job th is completed
        jobController.stop();
        return 0;
    }

    //helper method for output path check
    private void outputPathCheck(FileSystem f, List<Path> pathList) throws IOException{
        for(Path p : pathList){
            if(f.exists(p)){
                f.delete(p,true);
            }
        }
    }

    //use multiple mr to process big files
    public static class PreSortWithMultipleMr extends Mapper<LongWritable, Text, IntWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            try {
                context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
            } catch(NumberFormatException n){
                return;
            }
        }
    }
    public static class BigNumFileSortMR_Reducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> vlist, Context context)throws IOException, InterruptedException {
            for(NullWritable v : vlist){
                context.write(key, NullWritable.get());
            }
        }
    }

    // mapreducer for metainfo
    public static class MetaInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        HashSet<Integer> valuesInSplit = new HashSet<>();
        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            Long splitLength = context.getInputSplit().getLength();
            String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
            valuesInSplit.add(Integer.parseInt(value.toString()));
            if (splitLength == 0) {
                context.write(new Text(filename), new IntWritable(0));
            }else{
                context.write(new Text(filename), new IntWritable(valuesInSplit.size()));
            }
        }
    }
    public static class MetaInfoReducer extends Reducer<Text, IntWritable, Text, Text>{
        int index = 1;
        Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
            // meta info format
            // filename:start index of this file
            v.set("  " + index );
            context.write(key,v);
            index += IterableUtils.size(values);
        }
    }

    //final step:
    //use meta info to process multiple intermediate files
    public static class SortWithMetaInfoMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        Text outputKey = new Text();
        HashMap<String, Integer> filename2index = new HashMap<>();
        int last = 0;
        int index = 0;
        int count = 0;
        @SuppressWarnings("deprecation")
        @Override
        protected void setup(Context context)throws IOException, InterruptedException {
            BufferedReader bf = new BufferedReader(new FileReader(new File(OUTPUT2_PATH+"/part-r-00000")));
            String readline = null;
            while((readline = bf.readLine()) != null){
                String[] split = readline.split("\\s+");
                //split[0]: current split name
                //split[1]: start index for this split
                filename2index.put(split[0], Integer.parseInt(split[1]));
            }
            IOUtils.closeStream(bf);
            InputSplit inputSplit = context.getInputSplit();
            FileSplit fileSplit = (FileSplit)inputSplit;
            String name = fileSplit.getPath().getName();
            if (fileSplit.getLength() > 0) {

                index = filename2index.get(name);
            }

        }

        @Override
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
            int cur = Integer.parseInt(value.toString());
            //only when this number is not the first one in the current split (global counter != intial value)
            //and cur value != last processed value
            //then adding 1 to index
            if(++count!=1&&cur!=last){
                index++;
            }
            outputKey.set("group "+index+" : at time "+cur+" one log item recorded.");
            context.write(outputKey, NullWritable.get());
            //update
            last = cur;
        }
    }
    //don't need reducer for final step
    //because log event text in filtered out from the data
    public static class BigNumFileSortMR3_Reducer extends Reducer<Text, IntWritable, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
        }
    }
}
