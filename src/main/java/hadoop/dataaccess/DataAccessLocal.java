package hadoop.dataaccess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class DataAccessLocal {

    public static void deleteFile(String path){
        File outPutFile=new File(path);
        if(outPutFile.exists()){
            if(outPutFile.isDirectory()){
                File[] files=outPutFile.listFiles();
                for(File f:files){
                    deleteFile(f.getPath());
                }
            }
            outPutFile.delete();
        }
    }
    public static void main(String[] args) throws Exception{
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        job.setMapperClass(DataAccessMapper.class);
        job.setReducerClass(DataAccessReduce.class);
        
        job.setPartitionerClass(DataAccessPartition.class);
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataAccess.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataAccess.class);



        deleteFile("access/output");

        FileInputFormat.setInputPaths(job,new Path("access/input"));
        FileOutputFormat.setOutputPath(job,new Path("access/output"));

        job.waitForCompletion(true);

        File file=new File("access/output/part-r-00000");
        BufferedReader bufferedReader=new BufferedReader(new FileReader(file));
        String info;
        while((info=bufferedReader.readLine())!=null){
            System.out.println(info);
        }
        bufferedReader.close();
    }
}
