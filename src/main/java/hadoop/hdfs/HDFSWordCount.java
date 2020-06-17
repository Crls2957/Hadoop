package hadoop.hdfs;


import hadoop.hdfs.content.GetMap;
import hadoop.hdfs.impl.MapperImpl;
import hadoop.hdfs.constant.Constants;
import hadoop.hdfs.utils.ParamsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HDFSWordCount {

    public static void main(String[] args) throws Exception{
        System.out.println("*************START**************");
        Properties properties= ParamsUtils.getProperties();
        Path inputPath=new Path(properties.getProperty(Constants.INPUT_PATH));
        Path outputPath=new Path(properties.getProperty(Constants.OUTPUT_PATH));
        FileSystem fileSystem=FileSystem.get(new URI(properties.getProperty(Constants.HDFS_PATH)),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> remoteIterator=fileSystem.listFiles(inputPath,false);
        GetMap getMap=new GetMap();
        Class<?> clazz=Class.forName(properties.getProperty(Constants.MAPPER_PATH));
        Mapper mapper= (MapperImpl) clazz.newInstance();
        while (remoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus=remoteIterator.next();
            FSDataInputStream fsDataInputStream=fileSystem.open(locatedFileStatus.getPath());
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fsDataInputStream));
            String line="";
            while ((line=bufferedReader.readLine())!=null){
                mapper.map(line,getMap);
            }

            bufferedReader.close();
            fsDataInputStream.close();
        }
        Map<Object,Object> objectObjectMap=getMap.getMap();
        FSDataOutputStream fsDataOutputStream=fileSystem.create(new Path(outputPath,new Path("wordCount.out")));
        Set<Map.Entry<Object,Object>> entries=objectObjectMap.entrySet();
        for(Map.Entry<Object,Object> entrySet:entries){
            fsDataOutputStream.writeBytes((entrySet.getKey().toString()+"\t"+entrySet.getValue())+"\n");
        }
        fsDataOutputStream.close();

        System.out.println("*************FINISH*************");
        System.out.println("RESULT:");
        FSDataInputStream in=fileSystem.open(new Path(properties.getProperty(Constants.RESULT_PATH)));
        IOUtils.copyBytes(in,System.out,1024);
        fileSystem.close();
    }
}
