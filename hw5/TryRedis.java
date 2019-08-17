package hw5;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
public class TryRedis 
{
	public static void load_csv(String csv_path)
	{
		try
		{
	        //连接本地Redis服务
	        Jedis jedis = new Jedis("localhost");
	        System.out.println("连接成功");
	        //查看服务运行状态
	        System.out.println("服务正在运行: "+jedis.ping());
	        
	        System.out.println("read csv into redis");
			BufferedReader br = new BufferedReader(new FileReader(csv_path));
//			ArrayList<String> cols=new ArrayList<String>();
			String[] cols=null;
			int col_num=0;
			int line_cnt=0;
			
			String line_str=null;
			while(true)
			{
				line_str=br.readLine();
				if(line_str==null)
					break;
				String[] items=line_str.split(";");
				col_num=items.length;
				for(int col_i=0;col_i<col_num;++col_i)
				{
					String tmp_item=items[col_i].replaceAll("\"", " ");
					items[col_i]=tmp_item.trim();
				}
				if(line_cnt==0)
				{
					//first line
					cols=items;
					++line_cnt;
					continue;
				}
				// not first line
				// write into hashtable
				Map<String,String> line_map=new HashMap<String, String>();
				for(int col_i=0;col_i<col_num;++col_i)
				{
					line_map.put(cols[col_i], items[col_i]);
				}
				jedis.hmset(String.valueOf(line_cnt), line_map);
				++line_cnt;
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}


	}
	
	public static void redis_query_school()
	{
		try
		{
			int line_num=1044;
			//连接本地Redis服务
	        Jedis jedis = new Jedis("localhost");
	        System.out.println("连接成功");
	        //查看服务运行状态
	        System.out.println("服务正在运行: "+jedis.ping());
	        
	        System.out.println("statistics of student number in each school");
	        
	        Map<String,Integer> school2num=new HashMap<String,Integer>();
	        for(int i=1;i<=line_num;++i)
	        {
	        	String tmp_school=jedis.hget(String.valueOf(i), "school");
	        	if(tmp_school==null)
	        		continue;
	        	if(school2num.containsKey(tmp_school))
	        	{
	        		int pre_num=school2num.get(tmp_school);
	        		school2num.put(tmp_school, pre_num+1);
	        	}
	        	else
	        	{
	        		school2num.put(tmp_school, 1);
	        	}
	        } 
	        Set set=school2num.entrySet();
	        Iterator iterator = set.iterator();
	        while(iterator.hasNext()) 
	        {
	           Map.Entry tmp_entry = (Map.Entry)iterator.next();
	           System.out.print("School "+ tmp_entry.getKey() + "  ");
	           System.out.print("Student number: "+tmp_entry.getValue());
	           System.out.println();
	        }
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
    public static void main(String[] args) 
    {
//    	load_csv("student.csv");
    	redis_query_school();
        
    }
    
    
}