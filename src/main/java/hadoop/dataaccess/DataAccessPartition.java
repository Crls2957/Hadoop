package hadoop.dataaccess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DataAccessPartition extends Partitioner<Text,DataAccess> {

    @Override
    public int getPartition(Text text, DataAccess dataAccess, int i) {
        if(text.toString().startsWith("13")){
            return 0;
        }
        else if(text.toString().startsWith("15")){
            return 1;
        }
        else {
            return 2;
        }
    }
}
