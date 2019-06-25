import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static hdfsapi.HdfsDAO.*;

public class TestHdfsDao {


    @Test
    public void test(){
        Path createPath = new Path("/test2/output");
        Path src = new Path("/Users/wenxuanda/Codes/ShellCodes/num.txt");
        Path dst = new Path("/test2/");
        Path downSrc = new Path("/test1/output");
        Path downDst = new Path("/Users/wenxuanda/Codes/ShellCodes/");
        createFolder(createPath);
        uploadFile(src, dst);
        downloadFile(downSrc, downDst);
        listFile(new Path("/"));
    }
}
