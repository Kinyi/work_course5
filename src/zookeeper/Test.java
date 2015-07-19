package zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

public class Test {

	public static void main(String[] args) throws Exception {
		Watcher watcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.err.println(event);
			}
		};
		ZooKeeper zk = new ZooKeeper("192.168.80.100,192.168.80.101,192.168.80.102", 9999, watcher);
		zk.create("/queue-fifo", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		zk.close();
	}

}
