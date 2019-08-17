package hw5;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Value;

public class TryNeo4j  implements AutoCloseable
{
    // Driver objects are thread-safe and are typically made available application-wide.
    Driver driver;

    public TryNeo4j(String uri, String user, String password)
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
    public static void test_op()throws Exception
    {
        try( TryNeo4j example = new TryNeo4j("bolt://localhost:7687", "neo4j", "123456"))
        {	//加入学生和课程节点
 	        example.addStudent("ZhangSan", "CS", Arrays.asList("Redis", "MongoDB", "Cassandra"));
 	        example.addStudent("LiSi", "AI", Arrays.asList("C++","Python","Java"));
 	        example.addCourse("BigData",2019);
 	        //加入选课关系
 	        example.addElect("ZhangSan", "BigData", 59);
 	        //修改并打印不及格学生的姓名
 	        StatementResult result = example.execute_read("match (s:student)-[r:elect]->(c:course{name:\"BigData\"}) "+
 							"where r.score<60 "+
 							"set r.old_score=r.score, r.score=59 "+
 							"return s");
 	        while(result.hasNext())
 	        {
 	        	Record record = result.next();
 	        	Value node = record.get("s");
 	        	System.out.println(node.get("name"));
 	        }
        }
    }
    public static void main(String... args) throws Exception
    {
    	TryNeo4j neo_obj = new TryNeo4j("bolt://localhost:7687", "neo4j", "123456");
    	neo_obj.load_csv("student.csv");
    }
}
