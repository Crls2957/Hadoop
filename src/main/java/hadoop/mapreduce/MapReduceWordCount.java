package hadoop.mapreduce;

import hadoop.hdfs.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapReduceWordCount {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration configuration=new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.130.131:8020");

        Job job=Job.getInstance(configuration);


        job.setJarByClass(MapReduceWordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        job.setCombinerClass(WordCountReduce.class);

        //Mapper Input and Output Type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //Reduce Input and Output Type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fileSystem=FileSystem.get(new URI("hdfs://192.168.130.131:8020"),configuration,"hadoop");
        Path outputPath=new Path("/test/output/reduce");
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }
        FileInputFormat.setInputPaths(job,new Path("/test/wordCount.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/test/output/reduce"));

        boolean result=job.waitForCompletion(true);
        System.out.println("RESULT:");
        FSDataInputStream in=fileSystem.open(new Path("/test/output/reduce/part-r-00000"));
        IOUtils.copyBytes(in,System.out,1024);
        fileSystem.close();
        System.exit(result?0:-1);

    }
}
