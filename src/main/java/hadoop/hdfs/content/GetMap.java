package hadoop.hdfs.content;

import java.util.HashMap;
import java.util.Map;

public class GetMap {

    private Map<Object,Object> cacheMap=new HashMap<>();

    public Map<Object,Object> getMap(){
        return cacheMap;
    }

    public void write(Object key,Object value){
        cacheMap.put(key,value);
    }

    public Object get(Object key){
        return cacheMap.get(key);
    }
}
