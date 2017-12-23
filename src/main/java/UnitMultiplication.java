import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString().trim();
            String[] array = s.split("\t");

            if (array.length < 2)
                throw new RuntimeException();


            String[] ends = array[1].trim().split(",");
            int total = ends.length;

            for (String k : ends)
                context.write(new Text(array[0]), new Text( k + "=" + 1.0/total));
            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String s = value.toString().trim();

            String[] array = s.split("\t");

            if (array.length < 2) {
                throw new RuntimeException();
            }

            context.write(new Text(array[0]), new Text(array[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            double prob = 0.0;
            List<String> list = new ArrayList<String>();
            for (Text value: values) {
                if (!value.toString().contains("=")) {
                    prob = Double.parseDouble(value.toString());
                } else {
                    list.add(value.toString());
                }
            }
            for (String k : list) {
                String[] s = k.split("=");
                context.write(new Text(s[0]), new Text("" + Double.parseDouble(s[1])*prob));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class,conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class,conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
