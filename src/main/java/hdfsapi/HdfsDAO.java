package hdfsapi;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class HdfsDAO {


    /**
     * Configuration 类，封装客户端或服务器的配置
     * FileSystem类  是一个文件系统对象，用来对文件进行操作
     * FsDataInputStream FSDataOutputStream 分别是HDFs的输入流和输出流，通过open 和create 方法操作
     */

    private static final String HDFS = "hdfs://localhost/";

    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HDFS);
        return conf;
    }
    public static void createFolder(Path path) {
        createFolder(path,null);
    }

    public static void createFolder(Path path,Configuration conf) {
        if(conf==null){
            conf = getConf();
        }
        try {
            FileSystem fs=FileSystem.get(conf);
            if(!fs.exists(path)){
                fs.mkdirs(path);
                System.out.println("创建目录成功");
            }else {
                System.out.println("目录已存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("创建目录失败");
        }
    }


    public static void uploadFile(Path src, Path dst) {
        uploadFile(src, dst,null);
    }

    public static void uploadFile(Path src, Path dst,Configuration conf) {
        if(conf==null){
            conf = getConf();
        }
        try {
            FileSystem fs = FileSystem.get(conf);
            //这并不需要判断dst 是否为空，因为dst 是本地磁盘而非hdfs，所以没用
            fs.copyFromLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("上传文件成功");
    }

    public static void downloadFile(Path src, Path dst){
        downloadFile(src, dst,null);
    }
    public static void downloadFile(Path src, Path dst,Configuration conf) {
        if(conf==null){
            conf = getConf();
        }
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(dst,true);
            fs.copyToLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("源码还没看明白，如果报PathIsDirectoryException的错误，请查看该目录是否已经存在，存在则报错");
        }

        System.out.println("下载文件成功");
    }

    public static void listFile(Path path){
        listFile(path,null);
    }

    public static void listFile(Path path,Configuration conf) {
        if(conf==null){
             conf = getConf();
        }

        try {
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] fileStatuses = fs.listStatus(path);
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    System.out.println("当前路径是：" + fileStatus.getPath());
                    listFile(fileStatus.getPath());
                } else {
                    System.out.println("当前路径是：" + fileStatus.getPath());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
