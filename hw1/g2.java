import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/*
 * This is for the second group homework 
 * 
 * Structure of the file index
 * 
 *  
 *	0		4			how many file already in this Big file	
 *							each file:
 *								[flie offset]		1 int
 *								[file length]		1 int
 *								[name (byte)length]	1 int
 *								[file name]			128 char ~ 256 byte in Java
 *
 *
 *
	the file part:N is the number of small file
*/

public class g2 
{
	static int file_num_now;
	static String[] f_name;
	static int[] file_off;
	static int[] file_len;
	static String directory_path; 
	public static void main(String[] args)
	{
		for(int i=0;i<args.length;i++)
		{
			System.out.println(args[i]);
			System.out.println(args[i].length());
		}
		f_name = new String[2048];
		file_off = new int[2048];
		file_len = new int[2048];
		directory_path = new String();
    		try
    		{
    			String root_path="/home/lwx/Documents/hw1/";
    			String hdfs_fileName = "hw1/g2/Group_file";
    			String local_fileName = root_path+"local_index";
    			Configuration conf = new Configuration();
    			conf.set("fs.defaultFS", "hdfs://Master:9000");
    			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    			FileSystem fs = FileSystem.get(conf);
    			Path hdfs_file_path = new Path(hdfs_fileName);
    			File local_index_file_obj=new File(local_fileName);
    			if(!local_index_file_obj.exists())
    			{   
    				//this hdfs file path not exist
    				//create index local file
    				System.out.println("create local index file");
    				create_local_index_file(local_fileName);
    			}
    			//readFileIndex(local);
    	    	if(args[0].equalsIgnoreCase("build"))
    	    	{
    	    		System.out.println("build");
    	    		read_dir(args[1]);
    	//    		for(int j =0; j<file_num_now;j++)
    	 //   				System.out.println(f_name[j]);
    	    		save_File(fs,hdfs_file_path,local_fileName);
    	    	}
    	    	else if(args[0].equalsIgnoreCase("query"))
    	    	{
    	    		readFileIndex(local_fileName);
    	    		queryFile(fs,hdfs_file_path,args[1]);
    	    	}
    	    	
    			fs.close();
    		}
    		catch (Exception e)
    		{
    			e.printStackTrace();
    		}	
    }
	public static int byteArrayToInt(byte[] b) {   
		return   b[3] & 0xFF |   
		            (b[2] & 0xFF) << 8 |   
		            (b[1] & 0xFF) << 16 |   
		            (b[0] & 0xFF) << 24;   
		}   
	public static byte[] intToByteArray(int a) {   
		return new byte[] {   
		        (byte) ((a >> 24) & 0xFF),   
		        (byte) ((a >> 16) & 0xFF),      
		        (byte) ((a >> 8) & 0xFF),      
		        (byte) (a & 0xFF)   
		    };   
		}
	public static byte[] getBytes(char[] chars) {
        Charset cs = Charset.forName("UTF-8");
        CharBuffer cb = CharBuffer.allocate(chars.length);
        cb.put(chars);
        cb.flip();
        ByteBuffer bb = cs.encode(cb);
        return bb.array();
    }	
    public static void create_local_index_file(String local_index_path) throws IOException
    {
    	
    	File local_index_file_obj =new File(local_index_path);
        //if file doesn't exists, then create it
        if(!local_index_file_obj.exists())
        {
        	local_index_file_obj.createNewFile();
        }
        FileWriter fileWritter = new FileWriter(local_index_file_obj.getName());
//        fileWritter.write(0 + "\n"); 
        fileWritter.close();
    }
    public static void readFileIndex(String file) throws IOException{     	
    		
    	
    		File ifile = new File(file);  
    		//InputStream  in = new FileInputStream(ifile);  
            //BufferedReader d = new BufferedReader(new InputStreamReader(in));         
           // d.read(char_2, 0, 2);
    		BufferedReader in = new BufferedReader(new FileReader(ifile));
    		file_num_now = Integer.valueOf(in.readLine()).intValue();
            //= byteArrayToInt(getBytes(char_2));            
            //d.read(char_2, 4, 2);in.readline();        
            for(int i = 0; i<file_num_now; i++)
            {
            	//int off_now = 8 + i*268; 
            	//d.read(char_2, off_now + 0, 2);
            	file_off[i] = Integer.valueOf(in.readLine()).intValue();
            	//d.read(char_2, off_now + 4, 2);
            	file_len[i] = Integer.valueOf(in.readLine()).intValue();
            	f_name[i] = in.readLine();
            	/*d.read(char_2, off_now + 8, 2);
            	int name_len = byteArrayToInt(getBytes(char_2));
            	int charlen;
            	if(name_len % 2 ==0)
            		charlen = name_len/2;
            	else
            	    charlen = name_len/2 + 1;
            	d.read(tmpc, off_now + 12, charlen);
            	
            	for(int j=0;j < charlen;j++)
            	{
            		tmpb[2*j] = (byte) ((tmpc[j] & 0xFF00) >> 8);
            		tmpb[2*j+1] = (byte) (tmpc[j] & 0xFF);
            	}
            	if(name_len % 2 !=0)
            		tmpb[name_len-1]=(byte) ((tmpc[charlen] & 0xFF00) >> 8);
            	
            	f_name[i] = Arrays.toString(tmpb);
            	*/
            }        
            //d.close();
            in.close();         
    }
    public static void queryFile(FileSystem fs,  Path hdfs_file_path, String name) throws IOException
    {
      	
    	int i = 0;
        for(; i < file_num_now; i++)
        {
        	if(f_name[i].equals(name))
        		break;        	
        }
        if(i==file_num_now)
        {
        	System.out.println(name+" doesn't exist");
        	return;
        }
    	FSDataInputStream in = fs.open(hdfs_file_path);    	
    	FileOutputStream  out = new FileOutputStream(new File("fetched_"+name));
        BufferedReader d = new BufferedReader(new InputStreamReader(in));         
        byte[] tmpb = new byte[4096];   
        int t;
        in.seek(file_off[i]);
        for(int j = 0; j<file_len[i]; j+=4096)
        {    	
        	if(j + 4096 <= file_len[i])
        		t = 4096;
        	else
        		t = file_len[i] -j;
        	in.read(tmpb, 0, t);
    		out.write(tmpb,0,t);
        }        
        out.close();
        d.close();
        in.close();         
	}
    public static void save_File(FileSystem fs, Path hdfs_file_path,String local_index) throws IOException{
        	
    	FSDataOutputStream os =fs.create(hdfs_file_path);
    	
    	File local_index_obj =new File(local_index);

        FileWriter fileWriter = new FileWriter(local_index_obj.getName(),true);
        System.out.println("will write index into index file : "+local_index);
        System.out.println("file num now: "+file_num_now);
        fileWriter.write(file_num_now + "\n");  
        fileWriter.flush();
        
        
    	/*
    	byte[] Total_byte = intToByteArray(200);
    	os.write(Total_byte,0,Total_byte.length);   
    	
    	byte[] now_byte = intToByteArray(0);   	
    	os.write(now_byte,4,now_byte.length); */ 
 
    	
    	//int file_off_counter = 0;
        
        
    	
   	 	for(int i = 0; i<file_num_now; i++)
   	 	{
  	 		fileWriter.write(file_off[i] + "\n");   
  	 		fileWriter.write(file_len[i] + "\n");  
  	 		fileWriter.write(f_name[i] + "\n");  
   	 	}
   	 	fileWriter.close();
   	 	
        byte[] buffer = new byte[4096];
        
        for(int i = 0; i<file_num_now; i++)
   	 	{
        	//byte[] buffer = new byte[4096];
            File f = new File(directory_path + f_name[i]);
            int t;
            //System.out.println(i+" len="+file_len[i]);
            InputStream  in = new FileInputStream(f);
            for(int j = 0; j < file_len[i]; j+=4096)
            {
            	if(j + 4096 <= file_len[i])
            		t = 4096;
            	else
            		t = file_len[i] -j;
            	//System.out.println(j+"j t"+t);
                in.read( buffer, 0, t);
                os.write(buffer, 0, t);  
                //file_off_counter += t;
            }
            in.close();
   	 	}
        
        System.out.println("Save file in hdfs:"+hdfs_file_path.getName());
        os.close();
    }
    public static void read_dir(String dir_path) throws FileNotFoundException, IOException
    {

    	file_num_now = 0;
        File file = new File(dir_path);
        if (!file.isDirectory()) 
        {
                System.out.println("Not a directory\n");
        } 
        else if (file.isDirectory()) 
        {
        	int offset_cnt=0;
        	System.out.println("a directory\n");
        	directory_path = dir_path;
            String[] filelist = file.list();
            System.out.println("filelist len:"+filelist.length);
            for (int i = 0; i < filelist.length; i++) 
            {              
            	 f_name[i]=filelist[i];
               	 File readfile = new File(dir_path + filelist[i]);
               	 file_len[i]=(int)readfile.length(); 
               	 file_off[i]=offset_cnt;
               	 offset_cnt+=file_len[i];
           }
           file_num_now = filelist.length;          
        }

}
}