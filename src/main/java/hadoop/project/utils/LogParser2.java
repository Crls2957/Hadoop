package hadoop.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class LogParser2 {
    IPParser ipParser=IPParser.getInstance();
    public Map<String,String> parse(String log){
        Map<String,String> map=new HashMap<>();
        if(StringUtils.isNotBlank(log)){
            String[] splits=log.split("\t");
            String ip=splits[0];
            IPParser.RegionInfo regionInfo=ipParser.analyseIp(ip);
            String country="-";
            String province="-";
            String city="-";
            if(regionInfo!=null){
                country=regionInfo.getCountry();
                province=regionInfo.getProvince();
                city=regionInfo.getCity();
            }
            String url=splits[4];
            String time=splits[5];
            map.put("ip",ip);
            map.put("country",country);
            map.put("province",province);
            map.put("city",city);
            map.put("url",url);
            map.put("time",time);
        }
        return map;
    }
}
