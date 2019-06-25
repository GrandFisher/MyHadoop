package step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer 输入的Key的类型
 * Reducer 输入的value的类型
 * Reducer 输出的Key的类型
 * Reducer 输出的value的类型
 */
public class Reducer1 extends Reducer<Text, Text,Text,Text> {

    private Text outKey=new Text();
    private Text outValue=new Text();


    //key:列号  value:[ 行号_值]
    @Override
    public void reduce(Text key,Iterable<Text> values,Context context)
        throws IOException,InterruptedException{

        StringBuilder sb=new StringBuilder();
        for( Text text:values){
            sb.append(text+",");
        }
        String line=null;
        if(sb.toString().endsWith(",")){
            line=sb.substring(0,sb.length()-1);
        }


        outKey.set(key);
        outValue.set(line);

        context.write(outKey,outValue);

    }

}
