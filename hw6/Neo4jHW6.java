package hw6;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Neo4jHW6  implements AutoCloseable
{
    // Driver objects are thread-safe and are typically made available application-wide.
    Driver driver;

    public Neo4jHW6(String uri, String user, String password)
    {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    private void addStudent(String name, String major, List<String> skills)
    {
        // Sessions are lightweight and disposable connection wrappers.
        try (Session session = driver.session())
        {
            // Wrapping Cypher in an explicit transaction provides atomicity
            // and makes handling errors much easier.
            try (Transaction tx = session.beginTransaction())
            {
                tx.run("merge (a:student {name: {x}, major:{y}, skills:{z}})", parameters("x",name,"y", major, "z",skills));
                tx.success();
            }
        }
    }
    private void addCourse(String name, int year)
    {
        // Sessions are lightweight and disposable connection wrappers.
        try (Session session = driver.session())
        {
          // autocommit transaction
        	session.run("create (a:course {name: {x}, year:{y}})", parameters("x", name, "y", year));
        }
    }
    private void addElect(String sname, String cname, int score)
    {
        try (Session session = driver.session())
        {
            session.run("match (s{name:{sname}}),(c:course{name:{cname}}) "+
                    "create (s)-[r:elect{score:{score}}]->(c);",
                    parameters("sname", sname, "cname",cname,"score",score));
        }
    }
    
    private StatementResult execute_read(String statement)
    {
    	try (Session session = driver.session(AccessMode.READ))
    	{
    		return session.run(statement);
    	}
    }
    
    private StatementResult execute(String statement)
    {
    	try (Session session = driver.session())
    	{
    		return session.run(statement);
    	}
    }

    public void close ()throws Exception
    {
        // Closing a driver immediately shuts down all open connections.
        driver.close();
    }
    
    public void remove_all()
    {
    	StatementResult result=execute_read("match(a) detach delete a;");
    	while(result.hasNext())
        {
        	Record record = result.next();
        	Value node = record.get("s");
        	System.out.println(node.get("name"));
        }
    	
    }

    public void load_csv(String _csv_path)
    {
    	try
    	{
//    		neo_obj.remove_all();
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
				String create_str="create (a:student {";
				for(int col_i=0;col_i<col_num;++col_i)
				{
					create_str+=cols[col_i]+":"+"\""+items[col_i]+"\"";
					if(col_i!=col_num-1)
						create_str+=",";
				}
				create_str+="})";
				Session session = driver.session();
				session.run(create_str);
				
			}
    	}
    	catch(Exception e)
    	{
    		
    	}
    }
    
    public void clean_all()
    {
    	String command_str="match (n) detach delete n";
    	Session session = driver.session();
		session.run(command_str);
    }
    
    public void test_op()throws Exception
    {
        try
        {	//加入学生和课程节点
 	        addStudent("ZhangSan", "CS", Arrays.asList("Redis", "MongoDB", "Cassandra"));
 	        addStudent("LiSi", "AI", Arrays.asList("C++","Python","Java"));
 	        addCourse("BigData",2019);
 	        //加入选课关系
 	        addElect("ZhangSan", "BigData", 59);
 	        //修改并打印不及格学生的姓名
 	        String command_str="match (s:student)-[r:elect]->(c:course{name:\"BigData\"}) "+
						"where r.score<60 "+
						"set r.old_score=r.score, r.score=59 "+
						"return  s,c";
 	        System.out.println(command_str);
 	        StatementResult result = execute_read(command_str);
 	        while(result.hasNext())
 	        {
 	        	Record record = result.next();
 	        	Value node = record.get("c");
 	        	System.out.println(node.get("name"));
 	        }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
    }
    
    public void my_test()
    {
        try
        {	//加入学生和课程节点
 	
 	        //修改并打印不及格学生的姓名
 	        String command_str="match (s:school) return s";
 	        System.out.println(command_str);
 	        Session session=driver.session();
 	        StatementResult result = session.run(command_str);
 	        if(!result.hasNext())
 	        	System.out.println("no match");
 	        while(result.hasNext())
 	        {
 	        	Record record = result.next();
 	        	Value node = record.get("c");
 	        	System.out.println(node.get("name"));
 	        }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
    }
    
    public void load_triples(String _triples_path)
    {
    	try
		{
			BufferedReader br = new BufferedReader(new FileReader(_triples_path));
			String line_str=null;
			int line_cnt=0;
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
				String tmp_s=items[0];
				String tmp_p=items[1];
				String tmp_o=items[2];
				String command_find_s="match (s:subject{name:\""+tmp_s+"\"}) return s";
				StatementResult result = execute(command_find_s);
				boolean s_found;
	 	        if(!result.hasNext())
	 	        	s_found=false;
	 	        else 
	 	        	s_found=true;
				if(!s_found)
				{
					String command_create_s="create (s:subject{name:\""+tmp_s+"\"})";
					execute(command_create_s);
				}
				String command_find_o="match (o:object{name:\""+tmp_o+"\"}) return o";
				StatementResult result_o = execute(command_find_o);
				boolean o_found;
	 	        if(!result.hasNext())
	 	        	o_found=false;
	 	        else 
	 	        	o_found=true;
	 	        if(!o_found)
	 	        {
	 	        	String command_create_o="create (o:object{name:\""+tmp_o+"\"})";
	 	        	execute(command_create_o);
	 	        }
				String command_create_p="match (s:subject{name:\""+tmp_s+"\"})"
	 	        +",(o:object{name:\""+tmp_o+"\"}) ";
				command_create_p+="create (s)-[p:predicate{name:\""+tmp_p+"\"}]->(o)";
				execute(command_create_p);
				++line_cnt;
				if(line_cnt%100==0) 
					System.out.printf("have read %d lines\n",line_cnt);
			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
    	
    }
    
    public void yago_csv_load()
    {
    	String command_str="load csv from 'file:///yago_2.csv' as line\n";
    	command_str+="merge (s:subject{name:line[0]})\n";
    	command_str+="merge (o:object{name:line[2]})";
    	execute(command_str);
    	command_str="load csv from 'file:///yago_2.csv' as line\n";
    	command_str+="match (s:subject{name:line[0]}),";
    	command_str+="(o:object{name:line[2]})\n";
    	command_str+="create (s)-[p:predicate{name:line[1]}]->(o)";
    	execute(command_str);
    }
    
    public void query_1(String _s)
    {
    	try
	    {
	    	String query_str="match (s:subject{name:\""+_s+"\"})-[p]->(o)\n";
	    	query_str+="return s,p,o";
	    	StatementResult result=execute(query_str);
	    	if(!result.hasNext())
	    	{
		        	System.out.println("no match");
		        	return;
	    	}
	        while(result.hasNext())
	        {
	        	Record record = result.next();
	        	Value node = record.get("s");
	        	System.out.print(node.get("name"));
	        	System.out.print(" ");
	        	node = record.get("p");
	        	System.out.print(node.get("name"));
	        	System.out.print(" ");
	        	node = record.get("o");
	        	System.out.print(node.get("name"));
	        	System.out.println(" ");
	        }
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    public void query_2(String _o)
    {
    	try
	    {
	    	String query_str="match (s)-[p]->(o:object{name:\""+_o+"\"})\n";
	    	query_str+="return s,p,o";
	    	StatementResult result=execute(query_str);
	    	if(!result.hasNext())
	    	{
		        	System.out.println("no match");
		        	return;
	    	}
	        while(result.hasNext())
	        {
	        	Record record = result.next();
	        	Value node = record.get("s");
	        	System.out.print(node.get("name"));
	        	System.out.print(" ");
	        	node = record.get("p");
	        	System.out.print(node.get("name"));
	        	System.out.print(" ");
	        	node = record.get("o");
	        	System.out.print(node.get("name"));
	        	System.out.println(" ");
	        }
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    public void query_3(String _p1,String _p2)
    {
    	try
	    {
    		System.out.println("query 3");
	    	String query_str="match (s)-[p1:predicate{name:\""+_p1+"\"}]->(o1),";
	    	query_str+="(s)-[p2:predicate{name:\""+_p2+"\"}]->(o2)";
	    	query_str+="return s";
	    	StatementResult result=execute(query_str);
	    	if(!result.hasNext())
	    	{
		        	System.out.println("no match");
		        	return;
	    	}
	        while(result.hasNext())
	        {
	        	Record record = result.next();
	        	Value node = record.get("s");
	        	System.out.print(node.get("name"));
	        	System.out.println(" ");
	        }
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    public void query_4(String _o)
    {
    	try
	    {
    		System.out.println("query 4");
	    	String query_str="match (s)-[p]->(o:object{name:\""+_o+"\"})\n";
	    	query_str+="return s";
	    	StatementResult result=execute(query_str);
	    	if(!result.hasNext())
	    	{
		        	System.out.println("no match");
		        	return;
	    	}
	    	 Map<String, Integer> matched_s2num = new HashMap<String, Integer>();
	        while(result.hasNext())
	        {
	        	Record record = result.next();
	        	Value node = record.get("s");
	        	String tmp_s=String.valueOf(node.get("name"));
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
    
    
    public static void main(String... args) throws Exception
    {
    	Neo4jHW6 neo_obj = new Neo4jHW6("bolt://localhost:7687", "neo4j", "123456");
//    	neo_obj.load_csv("student.csv");
//  	   neo_obj.clean_all();
//    	neo_obj.yago_csv_load();
//	   neo_obj.load_triples(triples_path);

//	   neo_obj.my_test();
//	   neo_obj.query_1("David_Kaiser");
//	   neo_obj.query_2("Eidak");
//	   neo_obj.query_3("isLeaderOf","isCitizenOf");
   		neo_obj.query_4("United_States");
 	   neo_obj.close();
    }
}
