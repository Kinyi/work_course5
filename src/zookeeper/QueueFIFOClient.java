package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 利用zookeeper实现先进先出队列，该文件充当生产者产生节点
 */
public class QueueFIFOClient {

	public static void main(String[] args) throws Exception {
		Watcher watcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.err.println(event);
			}
		};
		ZooKeeper zk = new ZooKeeper("192.168.80.100,192.168.80.101,192.168.80.102", 9999, watcher);
		zk.create("/queue-fifo/", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		zk.close();
	}

}
