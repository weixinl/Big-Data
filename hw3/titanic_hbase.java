import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class titanic_hbase 
{
	static Configuration configuration;
    static Connection connection;
    static Admin admin;

    public static void init()
    {
        configuration  = HBaseConfiguration.create();
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        try
        {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    //关闭连接
    public static void close()
    {
        try
        {
            if(admin != null)
            {
                admin.close();
            }
            if(null != connection)
            {
                connection.close();
            }
        }catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    
	static void create_table(String table_name,String col_family_name)throws Exception
	{
	      // 配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");

        Connection connection = ConnectionFactory.createConnection(conf);
        
        // 管理员
        Admin hbaseAdmin = connection.getAdmin();

        
        // 表名称
        TableName tableName = TableName.valueOf(table_name);

        // 表描述器
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);

        tableDesc.addFamily(new HColumnDescriptor(col_family_name));// 添加列族

        // 创建一个表，同步操作
        hbaseAdmin.createTable(tableDesc);

        System.out.println("创建表" + table_name + "成功");
	}

	 public static void query_row(String tableNameString,String rowNameString) throws IOException 
	 {
        System.out.println("---------------按行建查询表中数据--------");

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");

        Connection connection = ConnectionFactory.createConnection(conf);
        //取得数据表对象
        Table table=connection.getTable(TableName.valueOf(tableNameString));

        //新建一个查询对象作为查询条件
        Get get = new Get(rowNameString.getBytes());

        //按行查询数据
        Result result = table.get(get);

        byte[] row = result.getRow();
        System.out.println("row key is:" +new String(row));

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells) 
        {
            byte[] familyArray = cell.getFamilyArray();
            byte[] qualifierArray = cell.getQualifierArray();
            byte[] valueArray = cell.getValueArray();

            System.out.println("row value is:"+ new String(familyArray) +
                    new String(qualifierArray) + new String(valueArray));
        }
        System.out.println("---------------查行键数据结束----------");
	  }
	
	 public static void showCell(Result result)
	 {
	        Cell[] cells = result.rawCells();
	        for(Cell cell:cells)
	        {
	            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
	            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
	            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
	            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
	            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
	        }
	 }
	 
	 static void getData(String tableName,String rowKey,String colFamily,String col)throws  IOException
	 {
	        init();
	        Table table = connection.getTable(TableName.valueOf(tableName));
	        Get get = new Get(rowKey.getBytes());
	        get.addColumn(colFamily.getBytes(),col.getBytes());
	        Result result = table.get(get);
	        showCell(result);
	        table.close();
	        close();
	 }
	 
	 static int get_row_survived(String table_name,String row_key,String col_family,String col)
			 throws IOException
	 {
		 	
	        Table table = connection.getTable(TableName.valueOf(table_name));
	        Get get = new Get(row_key.getBytes());
	        get.addColumn(col_family.getBytes(),col.getBytes());
	        Result result = table.get(get);
	        Cell[] cells = result.rawCells();
	        int if_survived=0;
	        for(Cell cell:cells)
	        {

//	            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
	        	String survived=new String(CellUtil.cloneValue(cell));
	        	if_survived=Integer.valueOf(survived);
	        }
	        return if_survived;
	 }
	 static int count_survived(String table_name)
	 {
		 init();
		 int survived_cnt=0;
		 try
		 {

			 int row_num=891;
			 String col_family="info";
			 String col="survived";
			 for(int row_id=1;row_id<=row_num;++row_id)
			 {
				 String row_key=String.valueOf(row_id);
				 int tmp_survived=get_row_survived(table_name,row_key,col_family,col);
				 survived_cnt+=tmp_survived;
			 }
			
		 }
		 catch(Exception e)
		 {
			 e.printStackTrace();
		 }
		 close();
		 return survived_cnt;
	 }

    public static void main(String[] agrs) throws Exception 
    {
    	getData("titanic","1","info","age");
    	int survived_num=count_survived("titanic");
    	System.out.printf("survived number: %d",survived_num);
  
    }
}