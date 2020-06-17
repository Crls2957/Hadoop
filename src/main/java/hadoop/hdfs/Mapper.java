package hadoop.hdfs;

import hadoop.hdfs.content.GetMap;
import org.apache.hadoop.io.BinaryComparable;

public interface Mapper {

    void map(String line, GetMap getMap);
}
