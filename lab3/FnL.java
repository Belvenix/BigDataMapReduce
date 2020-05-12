import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


// >>> Don't Change
public class FnL extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new FnL(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Title Count");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(FnLMap.class);
        job.setReducerClass(FnLReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(FnL.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

// <<< Don't Change

    public static class FnLMap extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split(",");

            if(!str[11].equals("") && str[11].contains("/") && str[11].length()>10){
                String date[] = str[11].toString().split("/| |:");
                String v = str[0]+" "+str[1]+" "+str[2];
                
                if(Integer.parseInt(date[0])>12){
                    String d = date[1]+"/"+date[0]+"/"+date[2]+" "+ date[3]+":"+date[4];
                    context.write(new Text(v), new Text(d));
                }
                else{
                    context.write(new Text(v), new Text(str[11]));
                }
            }
        }
    }

    public static class FnLReduce extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // TODO
            DateTimeFormatter f = DateTimeFormatter.ofPattern("M/d/yyyy H:m");
            int sum= 0;
            LocalDateTime min = null;
            LocalDateTime max = null;
            for (Text val : values){
                LocalDateTime appDate = LocalDateTime.parse(val.toString().trim(), f);
                if(sum==0){
                    min = appDate;
                    max = appDate;
                }
                else{
                    if(min.compareTo(appDate)>0){
                        min = appDate;
                    }
                    else if (max.compareTo(appDate)<0){
                        max = appDate;
                    }
                }   
                sum+=1;
            }
            String n = key.toString() + ", oldest date: "+min.toString() + ", newest date: "+ max.toString();
            context.write(new Text(n), new IntWritable(sum));
        }
    }
}