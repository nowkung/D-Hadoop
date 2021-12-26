import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class RecordError {
    public static class HadoopMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text balance = new Text(String.valueOf(-1));

        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context
        ) throws IOException, InterruptedException {
            String[] transaction = value.toString().split("[|]");
            if(Double.parseDouble(balance.toString())>-1){
                double balanceBefore = Double.parseDouble(balance.toString());
                double deposit = Double.parseDouble(transaction[2]);
                double withdraw = Double.parseDouble(transaction[3]);
                double balanceNow = Double.parseDouble(transaction[4]);
                double balanceAfter = balanceBefore+deposit-withdraw;
                if(balanceAfter != balanceNow){
                    context.write(new Text("Error"),value);
                }

            }
            balance.set(transaction[4]);
        }
    }

    public static class HadoopReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterator<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            while (values.hasNext()) {
                result.set(values.next());
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Record Error");
        job.setJarByClass(RecordError.class);
        job.setMapperClass(HadoopMapper.class);
        job.setCombinerClass(HadoopReducer.class);
        job.setReducerClass(HadoopReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
