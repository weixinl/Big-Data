package hw5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
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
public class TryMongodb
{
	public static void test_op()
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
            //插入文档
            Document document = new Document("name", "ZhangSan").append("major", "CS").append("score", 59);  
            List<Document> documents = new ArrayList<Document>();  
            documents.add(document);  
            collection.insertMany(documents); 
            //检索文档
            FindIterable<Document> findIterable = collection.find();  
            MongoCursor<Document> mongoCursor = findIterable.iterator();  
            while(mongoCursor.hasNext())
            {  
                System.out.println(mongoCursor.next());  
            }  
            //更新文档
            collection.updateMany(Filters.eq("score", 59), new Document("$set",new Document("likes",61)));
            //删除文档
            //删除符合条件的第一个文档  
            collection.deleteOne(Filters.eq("score", 61));  
            //删除所有符合条件的文档  
            collection.deleteMany (Filters.eq("score", 61));  
        }
        catch(Exception e)
        {
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
		
	}
	
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
	
	
   public static void main( String args[] )
   {
        load_csv("student.csv");
   }
}