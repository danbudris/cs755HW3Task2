package edu.bu.cs755;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// successfully returns the error rate as a percentage of total rides
public class Task2 {

    public static class GetMedallionErrors extends Mapper<Object, Text, Text, DoubleWritable>{

        // Set the variable which will be the value in the output map
        private final static DoubleWritable one = new DoubleWritable(1);
        private final static DoubleWritable zero = new DoubleWritable(0);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            // only process records with exactly 17 fields, thus discarding some malformed records
            if  (fields.length == 17) {
                // if the record contains GPS errors (blank fields or all zeros), set the value to 1
                if (fields[6].equals("0.000000") || fields[7].equals("0.000000") || fields[8].equals("0.000000") || fields[9].equals("0.000000") || fields[6].equals("") || fields[7].equals("") || fields[8].equals("") || fields[9].equals("")) {
                    context.write(new Text(fields[0]), one);
                }
                // if it does not have errors, set the value to 0
                else {
                    context.write(new Text(fields[0]), zero);
                }
            }
        }
    }

    public static class ErrRatePercentageReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            // Store the number of error records
            double errSum = 0;
            // Store the total number of records
            double totalSum = 0;
            for (DoubleWritable val : values) {
                // increment the total number of trips this taxi has taken
                totalSum += 1;
                // increment the error counter; non-errors are equal to 0, errors equal to 1
                errSum += val.get();
            }
            double answer = new Double(errSum/totalSum);
            /* Print lines for debugging
            System.out.println(key);
            System.out.println("ErrSum" + Double.toString(errSum));
            System.out.println("TotalSum" + Double.toString(totalSum));
            System.out.println("ErrPercentage" + Double.toString(errSum/totalSum));
            */
            // set the result to the percentage of error records in the total records for the given medallion number
            if (answer != 0) {
                result.set(answer);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job =  new Job(conf, "task2");
        job.setJarByClass(Task2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(Task2.GetMedallionErrors.class);
        job.setReducerClass(ErrRatePercentageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}