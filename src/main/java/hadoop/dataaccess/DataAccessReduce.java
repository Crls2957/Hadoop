package hadoop.dataaccess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataAccessReduce extends Reducer<Text,DataAccess,Text,DataAccess> {
    @Override
    protected void reduce(Text key, Iterable<DataAccess> values, Context context) throws IOException, InterruptedException {
        long ups=0;
        long downs=0;
        for(DataAccess dataAccess:values){
            ups+=dataAccess.getUp();
            downs=dataAccess.getDown();
        }
        context.write(key,new DataAccess(key.toString(),ups,downs));
    }
}
