package hadoop.project.mr2;

import hadoop.project.utils.IPParser;
import org.junit.Test;

public class IPTest {
    @Test
    public void TestIP(){
        IPParser.RegionInfo regionInfo=IPParser.getInstance().analyseIp("117.136.62.216");
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
        System.out.println(regionInfo.getCity());
    }
}
