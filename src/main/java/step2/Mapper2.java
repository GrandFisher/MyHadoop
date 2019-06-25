package step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Mapper2 extends Mapper<LongWritable,Text,Text,Text> {

    private Text outKey =new Text();
    private Text outValue =new Text();
    private List<String> cacheList= new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileReader fr=new FileReader("matrix2");
        BufferedReader br=new BufferedReader(fr);
        String line=null;
        while ((line=br.readLine())!=null){
            cacheList.add(line);
        }
        fr.close();
        br.close();

    }

    /**
     *
     * @param key  行号
     * @param value  tab分割  列值 列值
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException,InterruptedException{
        //行
        String row_matrix1=value.toString().split("\t")[0];
        String[] column_value_array_matrix1=value.toString().split("\t")[1].split(",");

        for(String line:cacheList){
            // line 右侧矩阵每一行
            String row_matrix2=line.split("\t")[0];
            String [] column_value_array_matrix2=line.split("\t")[1].split(",");
            int result=0;
            for( String col_value_matrix1:column_value_array_matrix1){
                String column_matrix1 =col_value_matrix1.split("_")[0];
                String value_matrix1 =col_value_matrix1.split("_")[1];
                for (String column_value_matrix2 : column_value_array_matrix2) {

                    if(column_value_matrix2.startsWith(column_matrix1+"_")){
                        String value_matrix2=column_value_matrix2.split("_")[1];
                        result+=Integer.valueOf(value_matrix1)*Integer.valueOf(value_matrix2);
                    }

                }
            }

            outKey.set(row_matrix1);
            outValue.set(row_matrix2+"_"+result);
            context.write(outKey,outValue);
        }

    }
}
