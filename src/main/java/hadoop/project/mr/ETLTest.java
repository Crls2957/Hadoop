package hadoop.project.mr;

import hadoop.project.utils.ContentUtils;
import hadoop.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;


public class ETLTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Configuration configuration=new Configuration();
        FileSystem fileSystem=FileSystem.get(new URI("hdfs://192.168.130.131:8020"),configuration,"hadoop");
        /*Path outputPath=new Path("input/elt");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }*/
        Path outputPath=new Path("/tmp/external/partition");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }
        Job job=Job.getInstance(configuration);
        job.setJarByClass(ETLTest.class);

        job.setMapperClass(MyMapper3.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);


        /*FileInputFormat.setInputPaths(job,new Path("input/flow/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("input/elt"));*/
        FileInputFormat.setInputPaths(job,new Path("hdfs://192.168.130.131:8020/project/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.130.131:8020/tmp/external/partition"));

        job.waitForCompletion(true);
    }

    static class MyMapper3 extends Mapper<LongWritable, Text, NullWritable, Text> {
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
            String ip=infos.get("ip");
            String country=infos.get("country");
            String province=infos.get("province");
            String city=infos.get("city");
            String url=infos.get("url");
            String time=infos.get("time");
            String pageid=ContentUtils.getPageID(url);

            StringBuffer buffer=new StringBuffer();
            buffer.append(ip).append("\t");
            buffer.append(country).append("\t");
            buffer.append(province).append("\t");
            buffer.append(city).append("\t");
            buffer.append(url).append("\t");
            buffer.append(time).append("\t");
            buffer.append(pageid).append("\t");
            context.write(NullWritable.get(),new Text(buffer.toString()));
        }
    }


}
