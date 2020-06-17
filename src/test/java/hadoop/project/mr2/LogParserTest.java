package hadoop.project.mr2;

import hadoop.project.utils.ContentUtils;
import hadoop.project.utils.IPParser;
import hadoop.project.utils.LogParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.Map;

public class LogParserTest {
    LogParser logParser;

    @Before
    public void setup(){
        logParser=new LogParser();
        System.out.println("START:");
    }
    @Test
    public void testLogParser(){
        String log="20946835322^Ahttp://www.yihaodian.com/1/?tracker_u=2225501&type=3^Ahttp://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235^A1号店^A1^ASKAPHD3JZYH9EE9ACB1NGA9VDQHNJMX1NY9T^A^A^A^A^APPG4SWG71358HGRJGQHQQBXY9GF96CVU^A2225501^A\\N^A124.79.172.232^A^Amsessionid:YR9H5YU7RZ8Y94EBJNZ2P5W8DT37Q9JH,unionKey:2225501^A^A2013-07-21 09:30:01^A\\N^Ahttp://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235^A1^A^A\\N^Anull^A-10^A^A^A^A^AMozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0; .NET4.0C; InfoPath.2; .NET4.0E)^AWin32^A^A^A^A^A^A上海市^A1^A^A2013-07-21 09:30:01^A上海市^A^A66^A^A^A^A^A\\N^A\\N^A\\N^A\\N^A2013-07-21";
        Map<String,String> map=logParser.parse(log);
        for(Map.Entry<String,String> entry:map.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }

    }
    @Test
    public void testSplit(){
        String id=ContentUtils.getPageID("http://www.1mall.com/cms/view.do?topicId=23481");
        System.out.println(id);
    }
    @After
    public void finish(){
        logParser=null;
        System.out.println("FINISH");
    }
}
