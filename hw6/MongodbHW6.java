package hw6;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import redis.clients.jedis.Jedis;
public class MongodbHW6
{
	public static void load_csv(String _csv_path)
	{
		try
		{
			// 连接到 mongodb 服务
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            // 连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            //创建集合
            mongoDatabase.createCollection("student");
            //选择集合
            MongoCollection<Document> collection = mongoDatabase.getCollection("student");
            
			List<Document> documents = new ArrayList<Document>();  
			
            BufferedReader br = new BufferedReader(new FileReader(_csv_path));
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
				Document document=new Document();
				for(int item_i=0;item_i<col_num;++item_i)
				{
					document.append(cols[item_i], items[item_i]);
				}
	            documents.add(document);
			}
			collection.insertMany(documents); 
            
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
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            mongoDatabase.createCollection("yago");
            MongoCollection<Document> collection = mongoDatabase.getCollection("yago");
            
			List<Document> documents = new ArrayList<Document>();  
			
			BufferedReader br = new BufferedReader(new FileReader(triples_path));
//			ArrayList<String> cols=new ArrayList<String>();

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

				Document document=new Document();
				for(int item_i=0;item_i<3;++item_i)
				{
					document.append(cols[item_i], items[item_i]);
				}
	            documents.add(document);
			}
            collection.insertMany(documents); 
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public static void test_op()
	{
		try
        {   

            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  

//            mongoDatabase.createCollection("student");

            MongoCollection<Document> collection = mongoDatabase.getCollection("student");
            Document document = new Document("name", "ZhangSan").append("major", "CS").append("score", 59);  
            List<Document> documents = new ArrayList<Document>();  
            documents.add(document);  
            collection.insertMany(documents); 
            //检索文档
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            while(mongoCursor.hasNext())
            {  
            	Document doc=mongoCursor.next();
                System.out.println(doc.toJson());  
                System.out.println(doc.get("name"));
            }  
            System.out.println();
            //更新文档
            collection.updateMany(Filters.eq("score", 59), new Document("$set",new Document("likes",61)));
            findIterable = collection.find();  
            mongoCursor = findIterable.iterator();  
            while(mongoCursor.hasNext())
            {  
                System.out.println(mongoCursor.next());  
            }  
            System.out.println();
            collection.deleteMany(new Document("score",59));
            
            //删除文档
            //删除符合条件的第一个文档  
            collection.deleteOne(Filters.eq("score", 61));
            //删除所有符合条件的文档  
            collection.deleteMany(Filters.eq("score", 61));
            findIterable = collection.find();  
            mongoCursor = findIterable.iterator();  
            while(mongoCursor.hasNext())
            {  
                System.out.println(mongoCursor.next());  
            }  
            System.out.println();
        }
        catch(Exception e)
        {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
		
	}
	
	public static void query_1(String _s)
	{
		try
		{
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            MongoCollection<Document> collection = mongoDatabase.getCollection("yago");
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            int matched_triple_num=0;
            while(mongoCursor.hasNext())
            {  
            	Document doc=mongoCursor.next();
            	String tmp_s=(String)doc.get("s");
            	if(tmp_s.equals(_s))
            	{
            		// matched
            		String tmp_p=(String)doc.get("p");
            		String tmp_o=(String)doc.get("o");
            		System.out.printf("<%s,%s,%s>\n",tmp_s,tmp_p,tmp_o);
            		++matched_triple_num;
            	}
            } 
            System.out.println("matched triple num:"+String.valueOf(matched_triple_num));
		}
		catch(Exception e)
        {
            e.printStackTrace();
        }
	}
	
	public static void query_2(String _o)
	{
		try
		{
			System.out.println("query 2");
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            MongoCollection<Document> collection = mongoDatabase.getCollection("yago");
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            int matched_triple_num=0;
            while(mongoCursor.hasNext())
            {  
            	Document doc=mongoCursor.next();
            	String tmp_o=(String)doc.get("o");
            	if(tmp_o.equals(_o))
            	{
            		// matched
            		String tmp_s=(String)doc.get("s");
            		String tmp_p=(String)doc.get("p");
            		System.out.printf("<%s,%s,%s>\n",tmp_s,tmp_p,tmp_o);
            		++matched_triple_num;
            	}
            } 
            System.out.println("matched triple num:"+String.valueOf(matched_triple_num));
		}
		catch(Exception e)
        {
            e.printStackTrace();
        }
	}
	
	public static void query_3(String _p1,String _p2)
	{
		try
		{
			System.out.println("query 3");
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            MongoCollection<Document> collection = mongoDatabase.getCollection("yago");
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            ArrayList<String> s_array_1=new ArrayList<String>();
            ArrayList<String> s_array_2=new ArrayList<String>();
            while(mongoCursor.hasNext())
            {  
            	Document doc=mongoCursor.next();
            	String tmp_p=(String)doc.get("p");
            	if(tmp_p.equals(_p1))
            	{
            		String tmp_s=(String)doc.get("s");
            		s_array_1.add(tmp_s);
            	}
               	if(tmp_p.equals(_p2))
            	{
            		String tmp_s=(String)doc.get("s");
            		s_array_2.add(tmp_s);
            	}
            } 
            String[] slist_1=new String[]{};
			slist_1=(String[])s_array_1.toArray(slist_1);
			String[] slist_2=new String[]{};
			slist_2=(String[])s_array_2.toArray(slist_2);
			Arrays.sort(slist_1);
			Arrays.sort(slist_2);
			int len_prev_1=slist_1.length;
			int len_prev_2=slist_2.length;
			ArrayList<String> sarray_1=new ArrayList<String>();
			ArrayList<String> sarray_2=new ArrayList<String>();
			sarray_1.add(slist_1[0]);
			for(int i=1;i<len_prev_1;++i)
			{
				if(slist_1[i].equals(slist_1[i-1]))
					continue;
				sarray_1.add(slist_1[i]);
			}
			sarray_2.add(slist_2[0]);
			for(int i=1;i<len_prev_2;++i)
			{
				if(slist_2[i].equals(slist_2[i-1]))
					continue;
				sarray_2.add(slist_2[i]);
			}
			slist_1=(String[])sarray_1.toArray(slist_1);
			slist_2=(String[])sarray_2.toArray(slist_2);
			int len1=slist_1.length;
			int len2=slist_2.length;
			int id1=0;
			int id2=0;
			int matched_s_num=0;
			BufferedWriter bw=new BufferedWriter(new FileWriter("matched_s.txt"));
			while(id1<len1&&id2<len2)
			{
				String s1=slist_1[id1];
				String s2=slist_2[id2];
				if(s1==null)
					break;
				if(s2==null)
					break;
				if(s1.equals(s2))
				{
					System.out.println(s1);
					bw.write(s1+"\n");
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
			bw.close();
			System.out.println(String.valueOf(matched_s_num)+" matched subject");
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
			System.out.println("query 4");
            MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");  
            MongoCollection<Document> collection = mongoDatabase.getCollection("yago");
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            Map<String, Integer> matched_s2num = new HashMap<String, Integer>();
            while(mongoCursor.hasNext())
            {  
            	Document doc=mongoCursor.next();
            	String tmp_o=(String)doc.get("o");
            	if(tmp_o.equals(_o))
            	{
            		String tmp_s=(String)doc.get("s");
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
	
   public static void main( String args[] )
   {
	   String triples_path="yago_simplified.txt";
//	   load_triples(triples_path);
//	   test_op();
//	   query_1("David_Kaiser");
//	   query_2("Eidak");
	   query_3("isLeaderOf","isCitizenOf");
//   		query_4("United_States");
   }
}