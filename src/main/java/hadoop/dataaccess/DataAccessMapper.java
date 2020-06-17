package hadoop.dataaccess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataAccessMapper extends Mapper<LongWritable, Text,Text,DataAccess> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines=value.toString().split("\t");
        String num=lines[1];
        long up=Long.parseLong(lines[(lines.length-3)]);
        long down=Long.parseLong(lines[lines.length-2]);
        context.write(new Text(num),new DataAccess(num,up,down));
    }
}
