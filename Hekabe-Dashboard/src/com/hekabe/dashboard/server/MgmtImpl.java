package com.hekabe.dashboard.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.db4o.Db4oEmbedded;
import com.db4o.ObjectContainer;
import com.hekabe.cassandra.cluster.CassandraCluster;
import com.hekabe.cassandra.cluster.CassandraMultiRegionCluster;
import com.hekabe.cassandra.cluster.EC2CassandraCluster;
import com.hekabe.cassandra.cluster.StaticCassandraCluster;
import com.hekabe.cassandra.instance.CassandraInstance;
import com.hekabe.cassandra.instance.EC2CassandraInstance;
import com.hekabe.cassandra.util.YCSBWorkload;
import com.hekabe.dashboard.shared.IpExchange;
import com.hekabe.dashboard.shared.NewClusterExchange;
import com.hekabe.dashboard.shared.User;
import com.hekabe.dashboard.shared.parameter.InstanceStatusParameter;
import com.hekabe.dashboard.shared.parameter.StringParameter;
import com.hekabe.dashboard.shared.parameter.YCSBParameter;
import com.hekabe.dashboard.shared.util.Util;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class MgmtImpl {

	private final String SIMPLEDB_DOMAIN = "hekabe-cluster";

	private static final ObjectContainer userdb = Db4oEmbedded.openFile(
			Db4oEmbedded.newConfiguration(), MgmtImpl.class.getResource("").getPath() + "db4o-userdb");
	{
		System.out.println("using db:" + MgmtImpl.class.getResource("").getPath() + "db4o-userdb");
	}
	

	/**
	 * Stores user, timestamp and status in SimpleDB and writes the XML file
	 * 
	 * @param ex
	 */
	public void saveCluster(NewClusterExchange ex) {
		// get username and timestamp
		String username = ex.getUsername();
		Long timestamp = ex.getTimestamp();
		String filename = username + "_" + timestamp + ".xml";

		// add simpledb entry with username and timestamp
		try {
			AmazonSimpleDB sdb = new AmazonSimpleDBClient(getCredentials(ex));

			List<ReplaceableAttribute> replaceableAttributes = new ArrayList<ReplaceableAttribute>();
			replaceableAttributes.add(new ReplaceableAttribute("user",
					username, true));
			replaceableAttributes.add(new ReplaceableAttribute("timestamp",
					String.valueOf(timestamp), true));
			replaceableAttributes.add(new ReplaceableAttribute("running",
					"true", true));
			PutAttributesRequest putAttributesRequest = new PutAttributesRequest(
					SIMPLEDB_DOMAIN, filename, replaceableAttributes);
			sdb.putAttributes(putAttributesRequest);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// write xml file
		exchangeObjectToXML(ex, filename);
	}

	/**
	 * Saves ExchangeObject as XML file
	 * 
	 * @param ex
	 *            NewClusterExchange object you want to save as XML
	 * @param filename
	 *            Filename of the XML file
	 */
	public void exchangeObjectToXML(NewClusterExchange ex, String filename) {
		XStream xstream = new XStream(new StaxDriver());
		String xml = xstream.toXML(ex);

		try {
			FileUtils.writeStringToFile(new File(filename), xml);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates NewClusterExchange object from given XML file
	 * 
	 * @param filename
	 *            Name of the XML file
	 * @return
	 */
	public static NewClusterExchange xmlToExchangeObject(String filename) {
		XStream xstream = new XStream(new StaxDriver());
		String xml = null;
		try {
			xml = FileUtils.readFileToString(new File(filename));
		} catch (IOException e) {
			e.printStackTrace();
		}
		NewClusterExchange newExObject = (NewClusterExchange) xstream
				.fromXML(xml);
		return newExObject;
	}

	/**
	 * Returns an ArrayList of NewClusterExchange objects given by a user
	 * 
	 * @param user
	 *            User you want to get the NewClusterExchange objects
	 * @return
	 */
	public ArrayList<NewClusterExchange> getClusterExchangeList(String user) {
		ArrayList<NewClusterExchange> exList = new ArrayList<NewClusterExchange>();

		// get users timestamp list from simpledb
		try {

			AmazonSimpleDB sdb = new AmazonSimpleDBClient(getUser(user)
					.getCredentials());

			String selectExpression = "select `timestamp` from `"
					+ SIMPLEDB_DOMAIN + "` where user = '" + user + "'";
			SelectRequest selectRequest = new SelectRequest(selectExpression);
			for (Item item : sdb.select(selectRequest).getItems()) {
				exList.add(xmlToExchangeObject(item.getName()));
				// for (Attribute attribute : item.getAttributes()) {
				// String timestamp = attribute.getValue();
				// String filename = user + "_" + timestamp + ".xml";
				// exList.add(xmlToExchangeObject(filename));
				// }
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return exList;
	}

	/**
	 * EC2: Terminating all instances of this cluster. 1&1: Stop's Cassandra
	 * instances and deletes Cassandra installation on the cluster.
	 * 
	 * @param user
	 * @param timestamp
	 * @return
	 */
	public String deleteCluster(String user, long timestamp) {
		String tag = createTag(user, timestamp);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (ex.getProvider().equals(StringParameter.AMAZON_EC2)
				|| ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
			doShutdownEC2(tag, ex);
		}
		if (ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			doActionOnStatic(ex, null, "removeCassandra");
		}

		if (ex.isMultiRegionCluster()) {
			if (ex.getMrProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
				doActionOnStatic(ex, null, "removeCassandra", true);
			}

		}

		FileUtils.deleteQuietly(new File(getFilename(user, timestamp)));
		FileUtils.deleteQuietly(new File(getFilename(user, timestamp).replace(
				".xml", ".pem")));

		try {
			InputStream credentialsAsStream = Thread.currentThread()
					.getContextClassLoader()
					.getResourceAsStream("AwsCredentials.properties");
			AmazonSimpleDB sdb = new AmazonSimpleDBClient(
					new PropertiesCredentials(credentialsAsStream));

			sdb.deleteAttributes(new DeleteAttributesRequest(SIMPLEDB_DOMAIN,
					getFilename(user, timestamp)));
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Starts cassandra on single instance
	 * 
	 * @param user
	 *            User who wants to start
	 * @param timestamp
	 *            Timestamp the cluster was started
	 * @param ip
	 *            IP of the instance you want to start Cassandra
	 * @param provider
	 *            The provider of the intance (EC2/1&1)
	 * @return
	 */
	public String startCassandra(String user, long timestamp, String ip,
			String provider) {
		String tag = createTag(user, timestamp);

		HashMap<String, String> tagKeyValue = new HashMap<String, String>();
		tagKeyValue.put("cassandraCluster", tag);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (provider.equals(StringParameter.AMAZON_EC2)) {
			doActionOnEC2(tag, tagKeyValue, ex, "startCassandra", ip);
		}
		if (provider.equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			doActionOnStatic(ex, ip, "startCassandra");
		}

		updateStatusInXmlFile(ip, user, timestamp,
				InstanceStatusParameter.RUNNING);
		return null;
	}

	/**
	 * Stops cassandra on single instance.
	 * 
	 * @param user
	 * @param timestamp
	 * @param ip
	 * @param provider
	 * @return
	 */
	public String stopCassandra(String user, long timestamp, String ip,
			String provider) {
		String tag = createTag(user, timestamp);

		HashMap<String, String> tagKeyValue = new HashMap<String, String>();
		tagKeyValue.put("cassandraCluster", tag);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (provider.equals(StringParameter.AMAZON_EC2)) {
			doActionOnEC2(tag, tagKeyValue, ex, "stopCassandra", ip);
		}
		if (provider.equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			doActionOnStatic(ex, ip, "stopCassandra");
		}

		updateStatusInXmlFile(ip, user, timestamp,
				InstanceStatusParameter.STOPPED);
		return null;
	}

	/**
	 * Decommissions node
	 * 
	 * @param user
	 * @param timestamp
	 * @param ip
	 * @param provider
	 * @return
	 */
	public String decommissionCassandra(String user, long timestamp, String ip,
			String provider) {
		String tag = createTag(user, timestamp);

		HashMap<String, String> tagKeyValue = new HashMap<String, String>();
		tagKeyValue.put("cassandraCluster", tag);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (provider.equals(StringParameter.AMAZON_EC2)) {
			doActionOnEC2(tag, tagKeyValue, ex, "decommissionNode", ip);
		}
		if (provider.equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			doActionOnStatic(ex, ip, "decommissionNode");
		}

		updateStatusInXmlFile(ip, user, timestamp,
				InstanceStatusParameter.DECOMMISSIONED);
		return null;
	}

	/**
	 * executes removeCassandra.sh script. There is NO DECOMMISSION executed
	 * first.
	 * 
	 * @param user
	 * @param timestamp
	 * @param ip
	 * @return
	 */
	public String terminateInstance(String user, long timestamp, String ip,
			String provider) {
		String tag = createTag(user, timestamp);

		HashMap<String, String> tagKeyValue = new HashMap<String, String>();
		tagKeyValue.put("cassandraCluster", tag);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (provider.equals(StringParameter.AMAZON_EC2)) {
			doActionOnEC2(tag, tagKeyValue, ex, "removeCassandra", ip);
		}
		if (provider.equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			doActionOnStatic(ex, ip, "removeCassandra");
		}

		updateStatusInXmlFile(ip, user, timestamp,
				InstanceStatusParameter.TERMINATED);
		return null;
	}

	/**
	 * Executes the benchmark.
	 * 
	 * @param user
	 *            String
	 * @param timestamp
	 *            long
	 * @param parameters
	 *            HashMap<String, String>
	 * @return
	 * @throws IOException
	 */
	public String startBenchmark(String user, long timestamp,
			HashMap<String, String> parameters) throws IOException {
		YCSBWorkload workload = new YCSBWorkload("217.160.94.220", "root",
				"ejuJT76W");
		String filename = parameters.get("filename");
		boolean load = false;
		if (parameters.get(YCSBParameter.LOAD).equals("true")) {
			load = true;
		} else {
			load = false;
		}
		parameters.remove("filename");
		parameters.remove(YCSBParameter.LOAD);

		for (Entry<String, String> entry : parameters.entrySet()) {
			workload.setParameter(entry.getKey(), entry.getValue());
		}

		List<CassandraCluster> clusterList = new ArrayList<CassandraCluster>();
		EC2CassandraCluster ec2Cluster = null;
		EC2CassandraCluster mrEc2Cluster = null;
		StaticCassandraCluster staticCluster = null;
		StaticCassandraCluster mrStaticCluster = null;

		String tag = createTag(user, timestamp);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
			AWSCredentials credentials;
			String endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
			credentials = getCredentials(ex);
			HashMap<String, String> tagKeyValue = new HashMap<String, String>();
			tagKeyValue.put("cassandraCluster", tag);
			AmazonEC2 ec2 = new AmazonEC2Client(credentials);
			ec2.setEndpoint(endpoint);

			ec2Cluster = EC2CassandraCluster.takeOverCassandraCluster(ec2,
					tagKeyValue, new File(tag + ".pem"));

			if (ex.isMultiRegionCluster()) {
				clusterList.add(ec2Cluster);
			} else {
				if (!ex.isPreparedForBenchmarking()) {
					ec2Cluster.setUpBenchmarkConfiguration();
					ex.setPreparedForBenchmarking(true);
				}

				workload.runBenchmark(ec2Cluster, load, filename, null);
			}

		} else if (ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
					ex.getIps(), InstanceStatusParameter.RUNNING);

			staticCluster = StaticCassandraCluster
					.takeOverCluster(ipPasswordMap);

			if (ex.isMultiRegionCluster()) {
				clusterList.add(staticCluster);
			} else {
				if (!ex.isPreparedForBenchmarking()) {
					staticCluster.setUpBenchmarkConfiguration();
					ex.setPreparedForBenchmarking(true);
				}

				workload.runBenchmark(staticCluster, load, filename, null);
			}
		}

		if (ex.isMultiRegionCluster()) {
			if (ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
				AWSCredentials credentials;
				String endpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
				credentials = getCredentials(ex);
				HashMap<String, String> tagKeyValue = new HashMap<String, String>();
				tagKeyValue.put("cassandraCluster", tag);
				AmazonEC2 ec2 = new AmazonEC2Client(credentials);
				ec2.setEndpoint(endpoint);

				mrEc2Cluster = EC2CassandraCluster.takeOverCassandraCluster(
						ec2, tagKeyValue, new File(tag + "_mr.pem"));
				clusterList.add(mrEc2Cluster);

			} else if (ex.getMrProvider().equals(
					StringParameter.ONE_AND_ONE_CLOUD)) {
				HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
						ex.getMrIps(), InstanceStatusParameter.RUNNING);

				mrStaticCluster = StaticCassandraCluster
						.takeOverCluster(ipPasswordMap);
				clusterList.add(mrStaticCluster);
			}

			CassandraMultiRegionCluster mrCluster = CassandraMultiRegionCluster
					.takeOverMultiRegionCluster(clusterList);
			mrCluster.setUpBenchmarkConfiguration();
			workload.runBenchmark(mrCluster, load, filename, null);
		}

		return null;
	}

	/**
	 * Sets new config values for the cluster
	 * 
	 * @param user
	 * @param timestamp
	 * @param parameters
	 * @return
	 */
	public String setConfigValues(String user, long timestamp,
			HashMap<String, String> parameters) {
		// List<CassandraCluster> clusterList = new
		// ArrayList<CassandraCluster>();
		EC2CassandraCluster ec2Cluster = null;
		EC2CassandraCluster mrEc2Cluster = null;
		StaticCassandraCluster staticCluster = null;
		StaticCassandraCluster mrStaticCluster = null;
		// CassandraMultiRegionCluster does not support setConfigValues yet.
		// CassandraMultiRegionCluster mrCluster = null;

		String tag = createTag(user, timestamp);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
			AWSCredentials credentials;
			String endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
			credentials = getCredentials(ex);
			HashMap<String, String> tagKeyValue = new HashMap<String, String>();
			tagKeyValue.put("cassandraCluster", tag);
			AmazonEC2 ec2 = new AmazonEC2Client(credentials);
			ec2.setEndpoint(endpoint);

			ec2Cluster = EC2CassandraCluster.takeOverCassandraCluster(ec2,
					tagKeyValue, new File(tag + ".pem"));

			// if mr cluster, added ec2cluster to cluster list, else set new
			// config values and update cassandra.
			// if(ex.isMultiRegionCluster()) {
			// clusterList.add(ec2Cluster);
			// } else {
			for (Entry<String, String> entry : parameters.entrySet()) {
				ec2Cluster.setConfigParameter(entry.getKey(), entry.getValue());
			}
			ec2Cluster.executeActionOnAllInstances("updateCassandra");
			// }

		} else if (ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
					ex.getIps(), null);

			staticCluster = StaticCassandraCluster
					.takeOverCluster(ipPasswordMap);

			// if mr cluster, added ec2cluster to cluster list, else set new
			// config values and update cassandra.
			// if(ex.isMultiRegionCluster()) {
			// clusterList.add(staticCluster);
			// } else {
			for (Entry<String, String> entry : parameters.entrySet()) {
				staticCluster.setConfigParameter(entry.getKey(),
						entry.getValue());
			}
			staticCluster.executeActionOnAllInstances("updateCassandra");
			// }
		}

		if (ex.isMultiRegionCluster()) {
			if (ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
				AWSCredentials credentials;
				String endpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
				credentials = getCredentials(ex);
				HashMap<String, String> tagKeyValue = new HashMap<String, String>();
				tagKeyValue.put("cassandraCluster", tag);
				AmazonEC2 ec2 = new AmazonEC2Client(credentials);
				ec2.setEndpoint(endpoint);

				mrEc2Cluster = EC2CassandraCluster.takeOverCassandraCluster(
						ec2, tagKeyValue, new File(tag + "_mr.pem"));
				// clusterList.add(mrEc2Cluster);

				for (Entry<String, String> entry : parameters.entrySet()) {
					mrEc2Cluster.setConfigParameter(entry.getKey(),
							entry.getValue());
				}

				mrEc2Cluster.executeActionOnAllInstances("updateCassandra");

			} else if (ex.getMrProvider().equals(
					StringParameter.ONE_AND_ONE_CLOUD)) {
				HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
						ex.getMrIps(), null);

				mrStaticCluster = StaticCassandraCluster
						.takeOverCluster(ipPasswordMap);
				// clusterList.add(mrStaticCluster);

				for (Entry<String, String> entry : parameters.entrySet()) {
					mrStaticCluster.setConfigParameter(entry.getKey(),
							entry.getValue());
				}

				mrStaticCluster.executeActionOnAllInstances("updateCassandra");
			}
		}

		return null;
	}

	/**
	 * Rebalance token
	 * 
	 * @param user
	 * @param timestamp
	 * @return
	 */
	public String rebalanceToken(String user, long timestamp) {
		CassandraMultiRegionCluster mrCluster = null;
		EC2CassandraCluster ec2Cluster = null;
		StaticCassandraCluster staticCluster = null;

		String tag = createTag(user, timestamp);

		HashMap<String, String> tagKeyValue = new HashMap<String, String>();
		tagKeyValue.put("cassandraCluster", tag);

		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		if (ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
			AWSCredentials credentials;
			String endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
			credentials = getCredentials(ex);

			AmazonEC2 ec2 = new AmazonEC2Client(credentials);
			ec2.setEndpoint(endpoint);

			ec2Cluster = EC2CassandraCluster.takeOverCassandraCluster(ec2,
					tagKeyValue, new File(tag + ".pem"));
			if (!ex.isMultiRegionCluster())
				ec2Cluster.rebalanceTokens();

		} else if (ex.getProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
					ex.getIps(), InstanceStatusParameter.RUNNING);

			if (ipPasswordMap.size() > 0) {
				staticCluster = StaticCassandraCluster
						.takeOverCluster(ipPasswordMap);
				if (!ex.isMultiRegionCluster())
					staticCluster.rebalanceTokens();
			}
		}

		if (ex.isMultiRegionCluster()) {
			List<CassandraCluster> clusters = new ArrayList<CassandraCluster>();
			EC2CassandraCluster mrEc2Cluster = null;
			StaticCassandraCluster mrStaticCluster = null;

			if (ec2Cluster != null)
				clusters.add(ec2Cluster);
			if (staticCluster != null)
				clusters.add(staticCluster);

			if (ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
				AWSCredentials credentials;
				String endpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
				credentials = getCredentials(ex);

				AmazonEC2 ec2 = new AmazonEC2Client(credentials);
				ec2.setEndpoint(endpoint);

				mrEc2Cluster = EC2CassandraCluster.takeOverCassandraCluster(
						ec2, tagKeyValue, new File(tag + "_mr.pem"));
				clusters.add(mrEc2Cluster);

			} else if (ex.getProvider().equals(
					StringParameter.ONE_AND_ONE_CLOUD)) {
				HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
						ex.getMrIps(), InstanceStatusParameter.RUNNING);

				if (ipPasswordMap.size() > 0) {
					mrStaticCluster = StaticCassandraCluster
							.takeOverCluster(ipPasswordMap);
				}

				clusters.add(mrStaticCluster);
			}

			mrCluster = CassandraMultiRegionCluster
					.takeOverMultiRegionCluster(clusters);
			mrCluster.rebalanceTokens();
		}

		return null;
	}

	/**
	 * Returns filename given by the username and the timestamp the cluster was
	 * started
	 * 
	 * @param user
	 * @param timestamp
	 * @return
	 */
	public static String getFilename(String user, long timestamp) {
		StringBuilder sb = new StringBuilder(user);
		sb.append("_").append(timestamp).append(".xml");
		return sb.toString();
	}

	/**
	 * Returns PropertiesCredentials of given NewClusterExchange object.
	 * 
	 * @param ex
	 * @return
	 */
	public AWSCredentials getCredentials(NewClusterExchange ex) {
		InputStream credentialsAsStream = null;
		if (ex.getAccessKey() == null && ex.getSecretAccessKey() == null) {
			return getUser(ex.getUsername()).getCredentials();
		} else {
			StringBuilder sb = new StringBuilder(String.valueOf(ex
					.getTimestamp()));
			sb.append(".properties");

			credentialsAsStream = Thread.currentThread()
					.getContextClassLoader()
					.getResourceAsStream(String.valueOf(sb.toString()));
			try {
				return new PropertiesCredentials(credentialsAsStream);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	/**
	 * Return NewClusterExchange object given by username and timestamp.
	 * 
	 * @param user
	 *            String
	 * @param timestamp
	 *            long
	 * @return
	 */
	public NewClusterExchange getConfigData(String user, long timestamp) {
		NewClusterExchange ex = xmlToExchangeObject(getFilename(user, timestamp));

		return ex;
	}

	/**
	 * Creates distinct tag 'user_timestamp' for identification.
	 * 
	 * @param user
	 * @param timestamp
	 * @return
	 */
	private String createTag(String user, long timestamp) {
		StringBuilder sb = new StringBuilder(user);
		sb.append("_").append(timestamp);
		String tag = sb.toString();
		return tag;
	}

	/**
	 * Shuts down every EC2 instance given by tag and NewClusterExchange object.
	 * 
	 * @param tag
	 * @param ex
	 */
	private void doShutdownEC2(String tag, NewClusterExchange ex) {
		AWSCredentials credentials = getCredentials(ex);
		String endpoint = null;
		String mrEndpoint = null;

		if (ex.getProvider().equals(StringParameter.AMAZON_EC2)) {
			endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
			shutdownEC2(tag, credentials, endpoint);
		}
		if (ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
			mrEndpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
			shutdownEC2(tag, credentials, mrEndpoint);
		}
	}

	/**
	 * Shuts down single EC2 instance
	 * 
	 * @param tag
	 *            String
	 * @param credentials
	 *            AWSCredentials
	 * @param endpoint
	 *            String
	 */
	private void shutdownEC2(String tag, AWSCredentials credentials,
			String endpoint) {
		AmazonEC2 ec2 = new AmazonEC2Client(credentials);
		ec2.setEndpoint(endpoint);

		DescribeInstancesRequest describeInstance = new DescribeInstancesRequest();
		describeInstance.withFilters(new Filter("tag:cassandraCluster")
				.withValues(tag));

		DescribeInstancesResult describeInstanceResult = ec2
				.describeInstances(describeInstance);
		List<Reservation> reservations = describeInstanceResult
				.getReservations();

		ArrayList<String> instanceIds = new ArrayList<String>();

		for (Reservation r : reservations) {
			List<Instance> instances = r.getInstances();
			for (Instance i : instances) {
				instanceIds.add(i.getInstanceId());
			}
		}

		ec2.terminateInstances(new TerminateInstancesRequest(instanceIds));
	}

	/**
	 * Executes an action like 'startCassandra', 'stopCassandra',
	 * 'updateCassandra', 'decommissionCassandra', 'removeCassanra' on EC2.
	 * 
	 * @param tag
	 *            String
	 * @param tagKeyValue
	 *            HashMap<String, String>
	 * @param ex
	 *            NewClusterExchange
	 * @param action
	 *            String
	 * @param ip
	 *            String
	 */
	private void doActionOnEC2(String tag, HashMap<String, String> tagKeyValue,
			NewClusterExchange ex, String action, String ip) {
		ArrayList<IpExchange> allIpList = new ArrayList<IpExchange>();
		AWSCredentials credentials;
		String endpoint = null;
		String filename = null;

		allIpList.addAll(ex.getIps());
		allIpList.addAll(ex.getMrIps());

		for (IpExchange ipEx : allIpList) {
			if (ex.getIps().contains(ipEx) && ipEx.getIp().equals(ip)) {
				endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
				filename = tag + ".pem";
				break;
			} else if (ex.getMrIps().contains(ipEx) && ipEx.getIp().equals(ip)) {
				endpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
				filename = tag + "_mr.pem";
				break;
			}
		}

		// if(ex.isMultiRegionCluster() &&
		// ex.getMrProvider().equals(StringParameter.AMAZON_EC2)) {
		// endpoint = NewClusterImpl.getEndpoint(ex.getMrRegion());
		// filename = tag+"_mr.pem";
		// } else {
		// endpoint = NewClusterImpl.getEndpoint(ex.getRegion());
		//
		// }

		credentials = getCredentials(ex);

		AmazonEC2 ec2 = new AmazonEC2Client(credentials);
		ec2.setEndpoint(endpoint);

		EC2CassandraCluster ec2Cluster = EC2CassandraCluster
				.takeOverCassandraCluster(ec2, tagKeyValue, new File(filename));
		if (ec2Cluster != null) {
			for (EC2CassandraInstance instance : ec2Cluster.getInstances()) {
				if (instance.getPublicIp().equals(ip)) {
					instance.executeAction(action);
				}
			}
		}
	}

	/**
	 * Executes an action like 'startCassandra', 'stopCassandra',
	 * 'updateCassandra', 'decommissionCassandra', 'removeCassanra' on 1&1.
	 * 
	 * @param ex
	 *            NewClusterExchange
	 * @param ip
	 *            String
	 * @param action
	 *            String
	 * @param isMultiregion
	 *            boolean
	 */
	private void doActionOnStatic(NewClusterExchange ex, String ip,
			String action, boolean isMultiregion) {
		StaticCassandraCluster staticCluster;
		HashMap<String, String> ipPasswordMap;
		if (isMultiregion) {
			ipPasswordMap = Util.getIpPasswordMap(ex.getMrIps(), null);
		} else {
			ipPasswordMap = Util.getIpPasswordMap(ex.getIps(), null);
		}

		if (ipPasswordMap.size() > 0) {
			staticCluster = StaticCassandraCluster
					.takeOverCluster(ipPasswordMap);

			if (ip == null) {
				staticCluster.executeActionOnAllInstances(action);
			} else {
				for (CassandraInstance instance : staticCluster.getInstances()) {
					if (instance.getPublicIp().equals(ip)) {
						instance.executeAction(action);
					}
				}
			}
		}
	}

	/**
	 * Executes action an a single static (1&1) instance.
	 * 
	 * @param ex
	 *            NewClusterExchange
	 * @param ip
	 *            String
	 * @param action
	 *            String
	 */
	private void doActionOnStatic(NewClusterExchange ex, String ip,
			String action) {
		StaticCassandraCluster staticCluster;
		HashMap<String, String> ipPasswordMap = Util.getIpPasswordMap(
				ex.getIps(), null);

		if (ex.getMrProvider().equals(StringParameter.ONE_AND_ONE_CLOUD)) {
			HashMap<String, String> mrIpPasswordMap = Util.getIpPasswordMap(
					ex.getMrIps(), null);
			ipPasswordMap.putAll(mrIpPasswordMap);
		}

		if (ipPasswordMap.size() > 0) {
			staticCluster = StaticCassandraCluster
					.takeOverCluster(ipPasswordMap);

			if (ip == null) {
				staticCluster.executeActionOnAllInstances(action);
			} else {
				for (CassandraInstance instance : staticCluster.getInstances()) {
					if (instance.getPublicIp().equals(ip)) {
						instance.executeAction(action);
					}
				}
			}
		}
	}

	/**
	 * Updates status in XML file.
	 * 
	 * @param ip
	 *            String
	 * @param user
	 *            String
	 * @param timestamp
	 *            long
	 * @param status
	 *            String
	 */
	private void updateStatusInXmlFile(String ip, String user, long timestamp,
			String status) {
		String filename = getFilename(user, timestamp);
		updateStatusInXmlFile(ip, filename, status);
	}

	/**
	 * Updates status in XML file.
	 * 
	 * @param ip
	 * @param filename
	 * @param status
	 */
	private void updateStatusInXmlFile(String ip, String filename, String status) {
		// read
		DocumentBuilderFactory domFactory = DocumentBuilderFactory
				.newInstance();
		domFactory.setNamespaceAware(true);
		DocumentBuilder builder = null;
		Document doc = null;
		try {
			builder = domFactory.newDocumentBuilder();
			doc = builder.parse(filename);
		} catch (ParserConfigurationException e2) {
			e2.printStackTrace();
		} catch (SAXException e2) {
			e2.printStackTrace();
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		XPathFactory factory = XPathFactory.newInstance();
		XPath xpath = factory.newXPath();

		// modify
		XPathExpression expr = null;
		Object result = null;
		try {
			expr = xpath
					.compile("//com.hekabe.dashboard.shared.IpExchange[ip/text() = '"
							+ ip + "']");
			result = expr.evaluate(doc, XPathConstants.NODESET);
		} catch (XPathExpressionException e2) {
			e2.printStackTrace();
		}
		NodeList nodes = (NodeList) result;
		for (int i = 0; i < nodes.getLength(); i++) {
			nodes.item(i).getLastChild().setTextContent(status);
		}

		// output
		Transformer transformer = null;
		DOMSource source = new DOMSource(doc);
		FileOutputStream os = null;
		StreamResult xmlResult;
		try {
			transformer = TransformerFactory.newInstance().newTransformer();
			os = new FileOutputStream(new File(filename));
			xmlResult = new StreamResult(os);
			transformer.transform(source, xmlResult);
		} catch (TransformerConfigurationException e1) {
			e1.printStackTrace();
		} catch (TransformerFactoryConfigurationError e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (TransformerException e1) {
			e1.printStackTrace();
		}
	}

	public boolean addUser(String username, String password,
			String ec2AccessKey, String ec2AccessKeySecret) {
		userdb.store(new User(username, password, ec2AccessKey,
				ec2AccessKeySecret));
		userdb.commit();
		return userdb.queryByExample(
				new User(username, password, ec2AccessKey, ec2AccessKeySecret))
				.size() > 0;
	}

	public boolean checkUser(String username, String password) {
		return userdb.queryByExample(new User(username, password, null, null))
				.size() > 0;
	}

	private User getUser(String username) {
		return checkUser(username, null) ? (User) userdb.queryByExample(
				new User(username, null, null, null)).get(0) : null;
	}

}
