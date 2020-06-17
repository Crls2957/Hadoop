package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static hadoop.hdfs.utils.ParamsUtils.getProperties;

public class hdfsAPITest {
    private static final String HDFS_PATH="hdfs://hadoop000:8020";
    Configuration configuration=null;
    FileSystem fileSystem=null;
    @Before
    public void startFlag() throws Exception{
        configuration=new Configuration();
        fileSystem=FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
        System.out.println("************START************");
    }
    @Test
    public void testProperties() throws Exception{
        System.out.println(getProperties().getProperty("RESULT_PATH"));
    }
    @Test
    public void listStatus() throws Exception{

        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator=fileSystem.listFiles(new Path("/"),true);

        while (fileStatusRemoteIterator.hasNext()){
            LocatedFileStatus file=fileStatusRemoteIterator.next();
            String isDir=file.isDirectory()?"Dir":"File";
            String permission=file.getPermission().toString();
            short replication=file.getReplication();
            long length=file.getLen();
            String path=file.getPath().toString();
            String user=file.getOwner();
            String group=file.getGroup();
            System.out.println(isDir+"\t"+permission+"\t"
                    +replication+"\t"+user+"\t"
                    +group+"\t"+length+"\t"+path);
        }
        /*FileStatus[] fileStatus=fileSystem.listStatus(new Path("/"));
        for(FileStatus file:fileStatus){
            String isDir=file.isDirectory()?"Dir":"File";
            String permission=file.getPermission().toString();
            short replication=file.getReplication();
            long length=file.getLen();
            String path=file.getPath().toString();
            String user=file.getOwner();
            String group=file.getGroup();
            System.out.println(isDir+"\t"+permission+"\t"
                    +replication+"\t"+user+"\t"
                    +group+"\t"+length+"\t"+path);
        }*/

    }

    @After
    public void finishFlag(){
        configuration=null;
        fileSystem=null;
        System.out.println("***********FINISH************");
    }
}
