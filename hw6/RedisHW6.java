package hw6;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.*;
public class RedisHW6
{
	public static int triple_num=1000000;
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
	
	public static void load_triples(String triples_path)
	{
		try
		{
	        //连接本地Redis服务
	        Jedis jedis = new Jedis("localhost");
	        System.out.println("连接成功");
	        //查看服务运行状态
	        System.out.println("服务正在运行: "+jedis.ping());
	        
	        System.out.println("read triples into redis hashtable");
			BufferedReader br = new BufferedReader(new FileReader(triples_path));
//			ArrayList<String> cols=new ArrayList<String>();

			int line_cnt=0;
			String line_str=null;
			String[] cols= {"s","p","o"};
			while(true)
			{
				line_str=br.readLine();
				if(line_str==null)
					break;
				String[] items=line_str.split(" ");
				int item_num=items.length;
				if(item_num<1)
					break;
				for(int col_i=0;col_i<3;++col_i)
				{
					items[col_i]=items[col_i].trim();
				}

				// write into hashtable
				Map<String,String> line_triple=new HashMap<String, String>();
				for(int col_i=0;col_i<3;++col_i)
				{
					line_triple.put(cols[col_i], items[col_i]);
				}
				jedis.hmset(String.valueOf(line_cnt), line_triple);
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
	
	public static void query_1(String _s)
	{

		//连接本地Redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("连接成功");
        //查看服务运行状态
        System.out.println("服务正在运行: "+jedis.ping());
        String query_pattern="<"+_s+",?p,?o>";
        System.out.println("query 1: "+query_pattern);
        int matched_triple_num=0;
		try
		{
			for(int id=0;id<triple_num;++id)
			{
				String key=String.valueOf(id);
				String tmp_s=jedis.hget(key, "s");
				if(tmp_s.equals(_s))
				{
					// this triple matched
					String tmp_p=jedis.hget(key, "p");
					String tmp_o=jedis.hget(key, "o");
					String output_str="<"+tmp_s+","+tmp_p+","+tmp_o+">";
					System.out.println(output_str);
					++matched_triple_num;
				}
			}
			System.out.println("matched triple num: "+String.valueOf(matched_triple_num));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void query_2(String _o)
	{
		//连接本地Redis服务
        Jedis jedis = new Jedis("localhost");
        System.out.println("连接成功");
        //查看服务运行状态
        System.out.println("服务正在运行: "+jedis.ping());
        String query_pattern="<?s,?p,"+_o+">";
        System.out.println("query 2: "+query_pattern);
        int matched_triple_num=0;
		try
		{
			for(int id=0;id<triple_num;++id)
			{
				String key=String.valueOf(id);
				String tmp_o=jedis.hget(key, "o");
				if(tmp_o.equals(_o))
				{
					// this triple matched
					String tmp_s=jedis.hget(key, "s");
					String tmp_p=jedis.hget(key, "p");
					String output_str="<"+tmp_s+","+tmp_p+","+tmp_o+">";
					System.out.println(output_str);
					++matched_triple_num;
				}
			}
			System.out.println("matched triple num: "+String.valueOf(matched_triple_num));
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void query_3(String _p1,String _p2)
	{
        Jedis jedis = new Jedis("localhost");
        System.out.println("连接成功");
        System.out.println("服务正在运行: "+jedis.ping());
        String query_pattern="<?s,"+_p1+",?o> "+"<?s,"+_p2+",?o> ";
        System.out.println("query 2: "+query_pattern);
        ArrayList<String> array_p1=new ArrayList<String>();
        ArrayList<String> array_p2=new ArrayList<String>();
		try
		{
			for(int id=0;id<triple_num;++id)
			{
				String key=String.valueOf(id);
				String tmp_p=jedis.hget(key, "p");
				if(tmp_p.equals(_p1))
				{
					String tmp_s=jedis.hget(key, "s");
					array_p1.add(tmp_s);
				}
				if(tmp_p.equals(_p2))
				{
					String tmp_s=jedis.hget(key, "s");
					array_p2.add(tmp_s);
				}
			}
			String[] slist_1=new String[]{};
			slist_1=(String[])array_p1.toArray(slist_1);
			String[] slist_2=new String[]{};
			slist_2=(String[])array_p2.toArray(slist_2);

			Arrays.sort(slist_1);
			Arrays.sort(slist_2);
			// remove duplicated element
			
			// union
			int len1=slist_1.length;
			int len2=slist_2.length;
			int id1=0;
			int id2=0;
			int matched_s_num=0;
			while(id1<len1&&id2<len2)
			{
				String s1=slist_1[id1];
				String s2=slist_2[id2];
				if(s1.equals(s2))
				{
					System.out.println(s1);
					++matched_s_num;
					++id1;
					++id2;
				}
				else if(s1.compareTo(s2)<0)
				{
					++id1;
				}
				else 
				{
					++id2;
				}
			}
			System.out.println(String.valueOf(matched_s_num)+" matched subject");
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void query_3_optimize(String _p1,String _p2)
	{
		Jedis jedis = new Jedis("localhost");
        System.out.println("连接成功");
        System.out.println("服务正在运行: "+jedis.ping());
        String query_pattern="<?s,"+_p1+",?o> "+"<?s,"+_p2+",?o> ";
        System.out.println("query 3: "+query_pattern);
        int matched_s_num=0;
        Set<String> keys = jedis.keys("*"); 
        Iterator<String> it=keys.iterator() ;   
        while(it.hasNext())
        {   
            String tmp_s = it.next();   
            String tmp_o1=jedis.hget(tmp_s,_p1);  
            String tmp_o2=jedis.hget(tmp_s, _p2);
            if(tmp_o1!=null&&tmp_o2!=null)
            {
            	// s matched
            	System.out.println(tmp_s);
            	++matched_s_num;
            }
        }
        System.out.println("matched_s_num: "+String.valueOf(matched_s_num));
	}
	
	public static void struct_q3(String _triples_path)
	{
		try
		{
	        Jedis jedis = new Jedis("localhost");
	        System.out.println("连接成功");
	        System.out.println("服务正在运行: "+jedis.ping());
	        
	        System.out.println("read triples into redis hashtable");
			BufferedReader br = new BufferedReader(new FileReader(_triples_path));
			

			String line_str=null;
			while(true)
			{
				line_str=br.readLine();
				if(line_str==null)
					break;
				String[] items=line_str.split(" ");
				int item_num=items.length;
				if(item_num<1)
					break;
				for(int col_i=0;col_i<3;++col_i)
				{
					items[col_i]=items[col_i].trim();
				}
				// write into hashtable
				Map<String,String> p2o=new HashMap<String, String>();
				p2o.put(items[1],items[2]);
				jedis.hmset(items[0], p2o);
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void query_4(String _o)
	{
		try
		{
	        Jedis jedis = new Jedis("localhost");
	        System.out.println("连接成功");
	        System.out.println("服务正在运行: "+jedis.ping());
	        System.out.println("query 4");
	        Map<String, Integer> matched_s2num = new HashMap<String, Integer>();
	        for(int id=0;id<triple_num;++id)
			{
				String key=String.valueOf(id);
				String tmp_o=jedis.hget(key,"o");
				if(tmp_o.equals(_o))
				{
					String tmp_s=jedis.hget(key, "s");
					// count += 1
					if(matched_s2num.containsKey(tmp_s))
					{
						int prev_cnt=matched_s2num.get(tmp_s);
						matched_s2num.put(tmp_s, prev_cnt+1);
					}
					else
					{
						matched_s2num.put(tmp_s, 1);
					}
				}
			}
	        String max_num_s="None";
	        int max_matched_num=0;
	        for(String matched_s : matched_s2num.keySet())
	        {
	        	int tmp_num=matched_s2num.get(matched_s);
	        	if(tmp_num>max_matched_num)
	        	{
	        		max_matched_num=tmp_num;
	        		max_num_s=matched_s;
	        	}
	        }
	        System.out.printf("subject with most object %s is %s\n",_o,max_num_s);
	        System.out.printf("the number is %d\n", max_matched_num);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
    public static void main(String[] args) 
    {
    	String triples_path="yago_simplified.txt";
//    	load_triples(triples_path);
//      query_1("David_Kaiser");
//    	query_2("Eidak");
//    	query_3("isLeaderOf","isCitizenOf");
//    	struct_q3(triples_path);
//    	query_3_optimize("isLeaderOf","isCitizenOf");
    	query_4("United_States");
    }
}