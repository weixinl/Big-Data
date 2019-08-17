import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;

public class g1 
{
    public static void main(String[] args)
    {	
    	for(int i = 0;i<10;i++)
    	{
	        try
	    	{
	            String fileName = "hw1/g1/test"+i;
	            Configuration conf = new Configuration();
	            conf.set("fs.defaultFS", "hdfs://Master:9000");
	            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
	            FileSystem fs = FileSystem.get(conf);
	            Path file = new Path(fileName);
	            if(fs.exists(file))
	            {
	                System.out.println("File exists:"+fileName);
	            }
	            else
	            {
	                System.out.println("File does not exist");
	                createFile(fs,file);
	            }
	            readFile(fs,file);
	            fs.close();
	        }
	    	catch(Exception e)
	    	{
	            e.printStackTrace();
	        }
    	}
    }

    public static void createFile(FileSystem fs, Path file) throws IOException
    {
//    	System.out.println("create file");
    	Random random_obj=new Random();
    	int repeat_cnt=random_obj.nextInt(100);
        byte[] buff = "Hello".getBytes();
        int buff_len=buff.length;
        FSDataOutputStream os =fs.create(file);
        for(int i=0;i<repeat_cnt;i++)
        {
        	os.write(buff,0, buff_len);
        }
//        //System.out.println("Create:"+file.getName());
        os.close();
    }

    public static void readFile(FileSystem fs, Path file) throws IOException
    {
//    	System.out.println("read file");
            FSDataInputStream in = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(in));
            String content = d.readLine();
            System.out.println(content);
            d.close();
            in.close();
    }

}