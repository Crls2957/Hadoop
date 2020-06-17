package hadoop.project.mr;

import hadoop.project.utils.ContentUtils;
import hadoop.project.utils.IPParser;
import hadoop.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;


public class PageTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration=new Configuration();
        FileSystem fileSystem=FileSystem.get(configuration);
        Path outputPath=new Path("output/v1/PageTest");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }
        Job job=Job.getInstance(configuration);
        job.setJarByClass(PageTest.class);

        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReduce2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input/flow/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("output/v1/PageTest"));

        job.waitForCompletion(true);
    }

    static class MyMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable ONE = new LongWritable(1);
        private LogParser logParser;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser=new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log=value.toString();
            Map<String,String> infos=logParser.parse(log);
            String url=infos.get("url");
            if(StringUtils.isNotBlank(url)){
                String id=ContentUtils.getPageID(url);
                context.write(new Text(id),ONE);
            }
        }
    }


    static class MyReduce2 extends Reducer<Text,LongWritable,Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count=0;
            for(LongWritable value:values){
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }

}
