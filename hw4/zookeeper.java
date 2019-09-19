import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

public class SLock 
{
    private final ZooKeeper zk;
    private final String root = "/slocks", prefix = "lock-";
    private final String lockPrefix = root + "/" + prefix;
    final static String hostport = "127.0.0.1:2181";
    // write clock: only one thread can lock
    static String w_lock = null;
    static ArrayList<String> r_locks=new ArrayList<String>();
    String lockNode=null;
    
    boolean is_reader;
    
    
    public SLock(boolean _is_reader) throws IOException 
    {
        zk = new ZooKeeper(hostport, 15000, (event) -> {});
        this.is_reader=_is_reader;
    }
    
    public void writer_lock() throws KeeperException, InterruptedException 
    {
        this.lockNode = zk.create(lockPrefix, " ".getBytes(), Ids.OPEN_ACL_UNSAFE, 
        		CreateMode.EPHEMERAL_SEQUENTIAL);
        while (true) 
        {
        	// 不断循环直到自己是最小的节点
            List<String> children = zk.getChildren(root, false);
            String closest = closest(children, lockNode.substring(lockNode.lastIndexOf("/") + 1));
            System.out.println(children);
            System.out.println(lockNode);
            System.out.println(closest);
            if (closest.equals(lockNode)) 
            {
            	if(r_locks.isEmpty())
            	{
	                w_lock = lockNode;
	                break;
            	}
            }
            final CountDownLatch signal = new CountDownLatch(1);
            Stat exists = zk.exists(closest, new Watcher()   
            {
                @Override
                public void process(WatchedEvent event) 
                {
                    // 监视的节点被删除
                    if (event.getType() == Event.EventType.NodeDeleted) 
                    {
                        signal.countDown();
                    }
                    
                }
            });
            // 返回null说明节点不存在，反之存在阻塞等待。
            if (exists != null) 
            {
                signal.await();// 阻塞直到signal.countDown执行。
            }
            while(!r_locks.isEmpty())
            {

            	Stat r_exists = zk.exists(r_locks.get(0), new Watcher()   
                {
                    @Override
                    public void process(WatchedEvent event) 
                    {
                        // 监视的节点被删除
                        if (event.getType() == Event.EventType.NodeDeleted) 
                        {
                            signal.countDown();
                        }
                        
                    }
                });
                // 返回null说明节点不存在，反之存在阻塞等待。
                if (r_exists != null) 
                {
                    signal.await();// 阻塞直到signal.countDown执行。
                }
            }
        }
    }
    
    public void reader_lock()  throws KeeperException, InterruptedException 
    {
        this.lockNode = zk.create(lockPrefix, " ".getBytes(), Ids.OPEN_ACL_UNSAFE, 
        		CreateMode.EPHEMERAL_SEQUENTIAL);

        while(true)
        {
            if(!r_locks.isEmpty())
            {
            	r_locks.add(lockNode);
            	return;
            }
           if(w_lock==null)
           {
        	 	r_locks.add(lockNode);
            	return;
           }
            final CountDownLatch signal = new CountDownLatch(1);
            Stat w_exists = zk.exists(w_lock, new Watcher()   
            {
                @Override
                public void process(WatchedEvent event) 
                {
                    // 监视的节点被删除
                    if (event.getType() == Event.EventType.NodeDeleted) 
                    {
                        signal.countDown();
                    }
                    
                }
            });
            // 返回null说明节点不存在，反之存在阻塞等待。
            if (w_exists != null) 
            {
                signal.await();// 阻塞直到signal.countDown执行。
            }
            
        }
    }
    
    private String closest(List<String> children, String lockNode) 
	{
        // 寻找比自己小的节点，如果没有返回自己。
        int nodeVal = valueOf(lockNode);
        int closestVal = -1;
        String closestNode = lockNode;
        for (String child : children) 
        {
            int childVal = valueOf(child);
            if (childVal < nodeVal && childVal > closestVal)
            {
                closestVal = childVal;
                closestNode = child;
            }
        }
        return root + "/" + closestNode;
    }

    private int valueOf(String node) 
	{
//    	System.out.println("enter valueOf");
//    	System.out.println(node);
        // 返回该节点的序号(后缀)
        int i = node.lastIndexOf(prefix);
//        System.out.println(i);
        String substring = node.substring(i + prefix.length());
        int val_substring=Integer.valueOf(substring);
//        System.out.println(val_substring);
        return val_substring;
        
    }
    
    public void lock()throws KeeperException, InterruptedException 
    {
    	if(this.is_reader)
    		reader_lock();
    	else
    		writer_lock();
    }
    
    public void reader_unlock() throws InterruptedException, KeeperException 
	{
        zk.delete(lockNode, -1);
        r_locks.remove(lockNode);
    }
    public void writer_unlock()  throws InterruptedException, KeeperException 
    {
    	zk.delete(lockNode, -1);
    	w_lock=null;
    }
    public void unlock()  throws InterruptedException, KeeperException 
    {
    	if(this.is_reader)
    		reader_unlock();
    	else
    		writer_unlock();
    }
    
    
    public static void main(String[] args) throws IOException 
	{
        //测试
        for (int i = 0; i < 10; i++) 
        {
            // 并发子线程
            new Actor(i).start();
        }
        System.in.read();
        
    }
}

class Actor extends Thread 
{	
	int id;
	SLock lock;
	public Actor(int id) throws IOException
	{
	    this.id = id;
	    boolean is_reader=true;
	    if(id%2==0)
	    {
	    	is_reader=false;
	    }
	    lock = new SLock(is_reader);

	}

	public void run() 
	{
	    try 
	    {
	        System.out.println("等待锁 " + id);
	        lock.lock();
	        System.out.println("获取到锁 " + id);
	        Thread.sleep(1000);
	        lock.unlock();
	        System.out.println("释放锁 " + id);
	    } 
	    catch (InterruptedException | KeeperException e) 
	    {
	        e.printStackTrace();
	    }
	}
}

