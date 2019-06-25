package step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MR1 {

    //输入输出路径
    private static String inPath="/matrix/step1input/matrix2.txt";
    private static String outPath="/matrix/step1output";

    //默认端口号8020
    private static String hdfs ="hdfs://localhost/";

    public  int run(){
        try {
            //创建job配置类
            Configuration conf=new Configuration();

            //设置hdfs的地址
            conf.set("fs.defaultFS",hdfs);

            //设置job主类
            Job job=Job.getInstance(conf,"step1");

            //设置job的Mapper类和Reducer类
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);

            //设置mapper的输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            //设置Reducer输出的类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


            //设置输入和输出路径
            FileSystem fs= FileSystem.get(conf);
            Path inputPath=new Path(inPath);


            if(fs.exists(inputPath)){

                FileInputFormat.addInputPath(job,inputPath);
            }


            Path outputPath=new Path(outPath);
            //输出路径必须为空
            fs.delete(outputPath,true);

            FileOutputFormat.setOutputPath(job,outputPath);
            return job.waitForCompletion(true)?1:-1;

        }catch (IOException | ClassNotFoundException | InterruptedException e){
            e.printStackTrace();
        }
        return -1;


    }


    public static void main(String[] args) {
        int result=-1;
        result=new MR1().run();

        if(result==1){
            System.out.println("step1 运行成功");
        }else if(result==-1){
            System.out.println("step1 运行失败");
        }

    }
}
