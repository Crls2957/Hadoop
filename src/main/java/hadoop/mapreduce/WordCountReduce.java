package hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        Iterator<IntWritable> intWritableIterator=values.iterator();

        while (intWritableIterator.hasNext()){
            IntWritable value=intWritableIterator.next();
            count+=value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
