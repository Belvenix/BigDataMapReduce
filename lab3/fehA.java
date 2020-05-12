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
import org.apache.hadoop.io.LongWritable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.text.DateFormat;
import java.util.concurrent.TimeUnit;

// >>> Don't Change
public class fehA extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new fehA(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Title Count");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setMapperClass(fehAMap.class);
        job.setReducerClass(fehAReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(fehA.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

// <<< Don't Change
    
    

    public static class fehAMap extends Mapper<Object, Text, IntWritable, LongWritable> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split(",");

            try{
                SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                Date appointmentDate = frmtIn.parse(str[11]);
                SimpleDateFormat frmtOut = new SimpleDateFormat("HH");
                int hour = Integer.parseInt(frmtOut.format(appointmentDate));
                Date arrivalDate = frmtIn.parse(str[6]);

                long diff = getDateDiff(arrivalDate, appointmentDate);
                if(Math.abs(diff) < 24*60 && !str[11].equals("") && !str[6].equals("")){
                    context.write(new IntWritable(hour), new LongWritable(diff));
                }
            }
            catch(ParseException e){;}
        }
        private long getDateDiff(Date end, Date start) {
            long diffInMillies = end.getTime() - start.getTime();
            TimeUnit tu = TimeUnit.MINUTES;
            return tu.convert(diffInMillies, TimeUnit.MILLISECONDS);
        }
    }

    public static class fehAReduce extends Reducer<IntWritable, LongWritable, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // TODO
            int sumA = 0;
            int sumB = 0;
            double sumC = 0;
            int sumD = 0;
            double sumE =0;
            double sumF=0;
            for (LongWritable val : values){
                sumA+=1;
                if(val.get() > 0){
                  sumB+=1;
                  sumC-=(double)val.get();  
                }
                else if(val.get() < 0){
                  sumD+=1;
                  sumE-=(double)val.get();  
                }   
                sumF+=Math.abs((double)val.get());
            }
            if(sumB!=0){
                sumC = Math.round((sumC/((double)sumB))*10.0)/10.0;
            }
            if(sumD!=0){
                sumE = Math.round((sumE/((double)sumD))*10.0)/10.0;
            }
            if(sumA!=0){
                sumF=(Math.round((sumF/((double)sumA))*10.0)/10.0);
            }
           

            String result = Integer.toString(sumA) + "\t" + Integer.toString(sumB)+"\t"+Double.toString(sumC)+"\t"+Integer.toString(sumD)+"\t"+Double.toString(sumE)+"\t"+Double.toString(sumF); 
            context.write(key, new Text(result));
        }
    }
}