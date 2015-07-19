package zookeeper;

import java.util.List;
import java.util.TreeMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 利用zookeeper实现先进先出队列，该文件充当消费者消费最先访问的节点
 */
public class QueueFIFOConsumer {

	public static void main(String[] args) throws Exception {
		TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
		Watcher watcher = new Watcher() {
			
			@Override
			public void process(WatchedEvent event) {
				System.err.println(event);
			}
		};
		ZooKeeper zk = new ZooKeeper("192.168.80.100,192.168.80.101,192.168.80.102", 9999, watcher);
		
		List<String> children = zk.getChildren("/queue-fifo", watcher);
		for (String child : children) {
			treeMap.put(Integer.parseInt(child), child);
		}
		
		String firstValue = treeMap.firstEntry().getValue();
		
		zk.delete("/queue-fifo/"+firstValue, -1);
		
		zk.close();
	}
}
