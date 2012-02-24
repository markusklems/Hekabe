package com.hekabe.dashboard.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.hekabe.cassandra.cluster.CassandraCluster;
import com.hekabe.cassandra.cluster.CassandraMultiRegionCluster;
import com.hekabe.cassandra.cluster.EC2CassandraCluster;
import com.hekabe.cassandra.cluster.StaticCassandraCluster;
import com.hekabe.dashboard.server.util.ProgessObserver;
import com.hekabe.dashboard.shared.IpExchange;
import com.hekabe.dashboard.shared.NewClusterExchange;
import com.hekabe.dashboard.shared.parameter.AwsEC2RegionParameter;
import com.hekabe.dashboard.shared.parameter.EC2AMIParameter;
import com.hekabe.dashboard.shared.parameter.EC2InstanceTypeParameter;
import com.hekabe.dashboard.shared.parameter.InstanceStatusParameter;
import com.hekabe.dashboard.shared.parameter.StringParameter;
import com.hekabe.dashboard.shared.parameter.YamlParameters;
import com.hekabe.dashboard.shared.util.Util;

public class NewClusterImpl {

	private static final String DONE = "DONE";
	private NewClusterExchange ex;
	private ProgessObserver observer;
	private String tag;
	private String tagMr;
	
	/**
	 * 
	 * @param ex
	 * @param observer
	 */
	public NewClusterImpl(NewClusterExchange ex, ProgessObserver observer) {
		this.ex = ex;
		this.observer = observer;
	}
	
	/**
	 * Process to start a new Cluster
	 */
	public void startInstance(AWSCredentials credentials) {
		observer.setStatus("Loading credentials");	
				
		AmazonEC2 ec2 = null;
		EC2CassandraCluster ec2Cluster = null;
		StaticCassandraCluster staticCluster = null;
		
		observer.setProgress(10);
		
		long timestamp = System.currentTimeMillis();
		ex.setTimestamp(timestamp);
		
		StringBuilder sb = new StringBuilder(ex.getUsername());
		sb.append("_").append(timestamp);
		tag = sb.toString();
		tagMr = sb.append("_mr").toString();
		
		// start first instance
		if(ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
			
			ec2 = new AmazonEC2Client(credentials);
			ec2.setEndpoint(getEndpoint(ex.getRegion()));
			
			observer.setStatus("Starting AWS EC2 cluster");
			ec2Cluster = EC2CassandraCluster.initializeCluster(
					ec2, 
					getImageID(ex.getRegion()), 
					getInstanceType(ex.getInstanceSize()), 
					ex.getNumberOfInstances(), 
					tag, 
					tag, 
					tag, 
					ex.isMultiRegionCluster());

			observer.setProgress(70);
			if(ex.isMultiRegionCluster()) observer.setProgress(40);
			observer.setStatus("Starting Cassandra on AWS EC2 cluster");
			
			if(!ex.isMultiRegionCluster()) {
				setYamlConfigParameters(ec2Cluster);
				ec2Cluster.executeActionOnAllInstances("updateCassandra");
			}
			ec2Cluster.executeActionOnAllInstances("startCassandra");
			
			ex.setIps(getEC2PublicIPs(ec2Cluster));
			
		} else if(ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			observer.setStatus("Starting 1&1 cluster");
			
			HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(ex.getIps(), null);
			staticCluster = StaticCassandraCluster.initializeCluster(ipPasswordMap, ex.isMultiRegionCluster());
			
			observer.setProgress(70);
			if(ex.isMultiRegionCluster()) observer.setProgress(40);
			observer.setStatus("Starting Cassandra on 1&1 cluster");
			
			staticCluster.executeActionOnAllInstances("startCassandra");
			if(!ex.isMultiRegionCluster()) {
				setYamlConfigParameters(staticCluster);
				staticCluster.executeActionOnAllInstances("updateCassandra");
			}
			
		}
		
		// Check if multiregion cluster is requested and if, create second cluster.
		if(ex.isMultiRegionCluster()) {
			observer.setProgress(50);
			observer.setStatus("Initializing multiregion cluster");
			
			AmazonEC2 mrEc2 = null;
			EC2CassandraCluster mrEc2Cluster = null;
			StaticCassandraCluster mrStaticCluster = null;
			
			if(ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
				observer.setStatus("Starting second cluster on AWS EC2");
				mrEc2 = new AmazonEC2Client(credentials);
				mrEc2.setEndpoint(getEndpoint(ex.getMrRegion()));
				
				mrEc2Cluster = EC2CassandraCluster.initializeCluster(
						mrEc2, 
						getImageID(ex.getMrRegion()), 
						getInstanceType(ex.getMrInstanceSize()), 
						ex.getMrNumberOfInstances(), 
						tag,
						tagMr,
						tag, 
						ex.isMultiRegionCluster());
				
//				setYamlConfigParameters(mrEc2Cluster);
				ex.setMrIps(getEC2PublicIPs(mrEc2Cluster));
			} else if(ex.getMrProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
				observer.setStatus("Starting second cluster on 1&1");
		
				HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(ex.getMrIps(), null);
				mrStaticCluster = StaticCassandraCluster.initializeCluster(ipPasswordMap, ex.isMultiRegionCluster());
//				setYamlConfigParameters(mrStaticCluster);
			} 
			
			observer.setProgress(70);
			
			CassandraMultiRegionCluster mrCluster = null;
			
			if(ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
				mrCluster = CassandraMultiRegionCluster.createMultiRegionCluster(ec2Cluster);
				setYamlConfigParameters(ec2Cluster);
				ec2Cluster.executeActionOnAllInstances("updateCassandra");
			} else if(ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
				mrCluster = CassandraMultiRegionCluster.createMultiRegionCluster(staticCluster);
				setYamlConfigParameters(staticCluster);
				staticCluster.executeActionOnAllInstances("updateCassandra");
			}
			
			observer.setProgress(80);
			observer.setStatus("Connecting both clusters");
			if(ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
				mrCluster.addCluster(mrEc2Cluster);
				setYamlConfigParameters(mrEc2Cluster);
				mrEc2Cluster.executeActionOnAllInstances("updateCassandra");
			} else if(ex.getMrProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
				mrCluster.addCluster(mrStaticCluster);
				setYamlConfigParameters(mrStaticCluster);
				mrStaticCluster.executeActionOnAllInstances("updateCassandra");
			}
			
			try {
				TimeUnit.SECONDS.sleep(30);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			observer.setProgress(90);
			observer.setStatus("Rebalancing tokens");
			mrCluster.rebalanceTokens();
		}
		
		if(!ex.isMultiRegionCluster()) {
			if(ec2Cluster != null) ec2Cluster.rebalanceTokens();
			if(staticCluster != null) staticCluster.rebalanceTokens();
		}
		
		observer.setProgress(100);
		observer.setStatus(DONE);
	}

//	private String getEC2PublicIPs(EC2CassandraCluster cluster) {
//		StringBuilder ipString = new StringBuilder();
//		for(String ip : cluster.getPublicIps()) {
//			ipString.append(ip).append(";");
//		}
//		ipString.deleteCharAt(ipString.length()-1);
//		return ipString.toString();
//	}
	
	/**
	 * Returns an ArrayList of IpExchange objects given by an EC2CassandraCluster
	 * @param cluster
	 * @return
	 */
	private ArrayList<IpExchange> getEC2PublicIPs(EC2CassandraCluster cluster) {
		ArrayList<IpExchange> ips = new ArrayList<IpExchange>();
		for(String ip : cluster.getPublicIps()) {
			ips.add(new IpExchange(ip, null, InstanceStatusParameter.RUNNING));
		}		
		return ips;
	}

	/**
	 * if compare fails, default value is <EC2InstanceTypeParameter.LARGE>
	 * @return Endpoint
	 */
	private String getInstanceType(String instanceSize) {
		String instanceType;
		
		if(instanceSize.equals(StringParameter.LARGE)) {
			instanceType = EC2InstanceTypeParameter.LARGE;
		} else if(instanceSize.equals(StringParameter.EXTRA_LARGE)) {
			instanceType = EC2InstanceTypeParameter.EXTRA_LARGE;
		} else if(instanceSize.equals(StringParameter.HIGH_MEMORY_EXTRA_LARGE)) {
			instanceType = EC2InstanceTypeParameter.HIGH_MEMORY_EXTRA_LARGE;
		} else if(instanceSize.equals(StringParameter.HIGH_MEMORY_DOUBLE_EXTRA_LARGE)) {
			instanceType = EC2InstanceTypeParameter.HIGH_MEMORY_DOUBLE_EXTRA_LARGE;
		} else if(instanceSize.equals(StringParameter.HIGH_MEMORY_QUADRUPLE_EXTRA_LARGE)) {
			instanceType = EC2InstanceTypeParameter.HIGH_MEMORY_QUADRUPLE_EXTRA_LARGE;
		} else if(instanceSize.equals(StringParameter.HIGH_CPU_EXTRA_LARGE)) {
			instanceType = EC2InstanceTypeParameter.HIGH_CPU_EXTRA_LARGE;
		} else {
			instanceType = EC2InstanceTypeParameter.LARGE;
		}
			
		return instanceType;
	}

	/**
	 * if compare fails, default value is AMI ID for US EAST VIRGINIA
	 * @return Ami ID
	 */
	private String getImageID(String region) {
		String amiID = null;
		
		if(region.equals(StringParameter.US_EAST_VIRGINIA)) {
			amiID = EC2AMIParameter.US_EAST_VIRGINIA;
		} else if(region.equals(StringParameter.US_WEST_OREGON)) {
			amiID = EC2AMIParameter.US_WEST_OREGON;
		} else if(region.equals(StringParameter.US_WEST_CALIFORNIA)) {
			amiID = EC2AMIParameter.US_WEST_CALIFORNIA;
		} else if(region.equals(StringParameter.EU_WEST_IRELAND)) {
			amiID = EC2AMIParameter.EU_EAST_IRELAND;
		} else if(region.equals(StringParameter.ASIA_PACIFIC_SINGAPORE)) {
			amiID = EC2AMIParameter.ASIA_PACIFIC_SINGAPORE;
		} else if(region.equals(StringParameter.ASIA_PACIFIC_TOKYO)) {
			amiID = EC2AMIParameter.ASIA_PACIFIC_TOKYO;
		} else if(region.equals(StringParameter.SOUTH_AMERICA_SAO_PAULO)) {
			amiID = EC2AMIParameter.SOUTH_AMERICA_SAO_PAULO;
		} else {
			amiID = EC2AMIParameter.US_EAST_VIRGINIA;
		}
		
		return amiID;
	}

	/**
	 * if compare fails, default value is US EAST VIRGINIA
	 * @return Endpoint
	 */
	public static String getEndpoint(String region) {
		String endpoint = null;
		
		if(region.equals(StringParameter.US_EAST_VIRGINIA)) {
			endpoint = AwsEC2RegionParameter.US_EAST_VIRGINIA;
		} else if(region.equals(StringParameter.US_WEST_OREGON)) {
			endpoint = AwsEC2RegionParameter.US_WEST_OREGON;
		} else if(region.equals(StringParameter.US_WEST_CALIFORNIA)) {
			endpoint = AwsEC2RegionParameter.US_WEST_CALIFORNIA;
		} else if(region.equals(StringParameter.EU_WEST_IRELAND)) {
			endpoint = AwsEC2RegionParameter.EU_EAST_IRELAND;
		} else if(region.equals(StringParameter.ASIA_PACIFIC_SINGAPORE)) {
			endpoint = AwsEC2RegionParameter.ASIA_PACIFIC_SINGAPORE;
		} else if(region.equals(StringParameter.ASIA_PACIFIC_TOKYO)) {
			endpoint = AwsEC2RegionParameter.ASIA_PACIFIC_TOKYO;
		} else if(region.equals(StringParameter.SOUTH_AMERICA_SAO_PAULO)) {
			endpoint = AwsEC2RegionParameter.SOUTH_AMERICA_SAO_PAULO;
		} else {
			endpoint = AwsEC2RegionParameter.US_EAST_VIRGINIA;
		}
		
		return endpoint;
	}
	
	/**
	 * Sets the YAML parameters for a given CassandraCluster
	 * @param cluster
	 */
	private void setYamlConfigParameters(CassandraCluster cluster) {
		if(ex.getPartitioner().equals("Random Partitioner")) {
			cluster.setConfigParameter(YamlParameters.PARTITIONER, "org.apache.cassandra.dht.RandomPartitioner");
		} else if(ex.getPartitioner().equals("Byte Ordered Partitioner")) {
			cluster.setConfigParameter(YamlParameters.PARTITIONER, "org.apache.cassandra.dht.ByteOrderedPartitioner");
		}
		
		cluster.setConfigParameter(YamlParameters.HINTED_HANDOFF, String.valueOf(ex.isHintedHandoff()));
		cluster.setConfigParameter(YamlParameters.MAX_WINDOW_TIME, String.valueOf(ex.getMaxWindowTime()));
		cluster.setConfigParameter(YamlParameters.THROTTLE_DELAY, String.valueOf(ex.getThrottleDelay()));

		cluster.setConfigParameter(YamlParameters.SYNC_TYPE, ex.getSyncType());
		if(ex.getSyncType().equals("batch")) {
			cluster.setConfigParameter(YamlParameters.TIME_WINDOW_BATCH, String.valueOf(ex.getTimeWindow()));
		} else if(ex.getSyncType().equals("periodic")) {
			cluster.setConfigParameter(YamlParameters.TIME_WINDOW_PERIODIC, String.valueOf(ex.getTimeWindow()));
		}
		cluster.setConfigParameter(YamlParameters.COMMITLOG_TOTAL_SPACE, String.valueOf(ex.getCommitlogTotalSpace()));
		cluster.setConfigParameter(YamlParameters.REDUCE_CACHE_AT, String.valueOf(ex.getReduceCacheAt()));
		cluster.setConfigParameter(YamlParameters.REDUCE_CACHE_CAPACITY_TO, String.valueOf(ex.getReduceCacheCapacity()));
		cluster.setConfigParameter(YamlParameters.CONCURRENT_READS, String.valueOf(ex.getConcurrentReads()));
		cluster.setConfigParameter(YamlParameters.CONCURRENT_WRITES, String.valueOf(ex.getConcurrentWrites()));
		cluster.setConfigParameter(YamlParameters.MEMTABLE_WRITER_THREADS, String.valueOf(ex.getMemtableWriterThreads()));
		cluster.setConfigParameter(YamlParameters.MEMTABLE_TOTAL_SPACE, String.valueOf(ex.getMemtableTotalSpace()));
		cluster.setConfigParameter(YamlParameters.FLUSH_FRACTION, String.valueOf(ex.getFlushFraction()));
	}
	
	/**
	 * 
	 * @return
	 */
	public ProgessObserver getObserver() {
		return observer;
	}
}