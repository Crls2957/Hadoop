package hadoop.hdfs.impl;

import hadoop.hdfs.content.GetMap;
import hadoop.hdfs.Mapper;
import org.apache.hadoop.io.BinaryComparable;

public class MapperImpl implements Mapper {
    @Override
    public void map(String line, GetMap getMap) {
        String [] words=line.toLowerCase().split("\\s");
        for(String word:words){
            Object value=getMap.get(word);
            if(value==null){
                getMap.write(word,1);
            }else {
                int v=Integer.parseInt(value.toString());
                getMap.write(word,v+1);
            }
        }
    }
}
