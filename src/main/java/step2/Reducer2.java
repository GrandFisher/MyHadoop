package step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reducer2 extends Reducer<Text, Text,Text,Text> {



    private Text outKey=new Text();
    private Text outValue=new Text();


    //key:列号  value:[ 行号_值]
    @Override
    public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{
        StringBuilder sb=new StringBuilder();
        for(Text value:values){
            sb.append(value).append(",");
        }

        System.out.println(sb);
        String result=null;
        if(sb.toString().endsWith(",")){
            result=sb.substring(0,sb.length()-1);
        }
        System.out.println("print result");
        System.out.println(result);
        outKey.set(key);
        outValue.set(result);

        context.write(outKey,outValue);

    }
}
