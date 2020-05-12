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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.text.DateFormat;
import java.util.concurrent.TimeUnit;

// >>> Don't Change
public class fehB extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new fehB(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Title Count");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(fehBMap.class);
        job.setReducerClass(fehBReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(fehB.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

// <<< Don't Change
    
    

    public static class fehBMap extends Mapper<Object, Text, IntWritable, Text> {
        
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
                if(Math.abs(diff) < 24*60 && diff >= 0 && !str[11].equals("") && !str[6].equals("")){
                    context.write(new IntWritable(hour), new Text(line));
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

    public static class fehBReduce extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // TODO
           int sum = 0;
            for (Text val : values){
                sum+=1;
            }
            context.write(key, new IntWritable(sum));
        }
    }
}