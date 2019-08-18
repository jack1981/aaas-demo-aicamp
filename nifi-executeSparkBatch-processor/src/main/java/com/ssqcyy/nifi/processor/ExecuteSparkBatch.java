/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ssqcyy.nifi.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.gson.Gson;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "spark", "livy", "http", "execute" })
@CapabilityDescription("Submit a spark batch job using Livy.")
public class ExecuteSparkBatch extends AbstractProcessor {
	public final String APPLICATION_JSON = "application/json";

	public static final PropertyDescriptor LIVY_URL = new PropertyDescriptor.Builder().name("livy-url")
			.displayName("Livy URL").description("The hostname (or IP address) & port of the Livy server.")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder().name("Connection Timeout")
			.description("Max wait time for connection to remote service.").required(true).defaultValue("5 secs")
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();

	public static final PropertyDescriptor FILE_PATH = new PropertyDescriptor.Builder().name("file")
			.displayName("File Path").description("File containing the application to execute").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();
	public static final PropertyDescriptor CLASS_NAME = new PropertyDescriptor.Builder().name("className")
			.displayName("Class Name").description("Application Java/Spark main class").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor DRIVER_MEMORY = new PropertyDescriptor.Builder().name("driverMemory")
			.displayName("Driver Memory").description("Amount of memory to use for the driver process").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor DRIVER_CORES = new PropertyDescriptor.Builder().name("driverCores")
			.displayName("Driver Cores").description("Number of cores to use for the driver process").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor EXECUTOR_MEMORY = new PropertyDescriptor.Builder().name("executorMemory")
			.displayName("Executor Memory").description("Amount of memory to use per executor process").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor EXECUTOR_CORES = new PropertyDescriptor.Builder().name("executorCores")
			.displayName("Executor Cores").description("Number of cores to use for each executor").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor EXECUTOR_NUMBERS = new PropertyDescriptor.Builder().name("numExecutors")
			.displayName("Executor Count").description("Number of executors to launch for this session").required(false)
			.addValidator(StandardValidators.INTEGER_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor JARS = new PropertyDescriptor.Builder().name("jars")
			.displayName("Session JARs").description("JARs to be used in the Spark session.").required(false)
			.addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
			.expressionLanguageSupported(false).build();

	public static final PropertyDescriptor ARGS = new PropertyDescriptor.Builder().name("args").displayName("Job Args")
			.description(
					"Command line arguments for the application. Seperate each argument with the specified arg delimeter.")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(true)
			.build();
	public static final PropertyDescriptor ARGS_DELIM = new PropertyDescriptor.Builder().name("args_delim")
			.displayName("Args Delimeter").description("Delimeter to seperate job arguments").required(true)
			.expressionLanguageSupported(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue(",")
			.build();

	public static final PropertyDescriptor PROXY_USER = new PropertyDescriptor.Builder().name("proxyUser")
			.displayName("Proxy User").description("User to impersonate when running the job").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).expressionLanguageSupported(false).build();

	public static final PropertyDescriptor CONF = new PropertyDescriptor.Builder().name("conf")
			.displayName("Spark Conf").description("Spark configuration properties as Map of key=val").required(false)
			.expressionLanguageSupported(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor STATUS_CHECK_INTERVAL = new PropertyDescriptor.Builder()
			.name("exec-spark-iactive-status-check-interval").displayName("Status Check Interval")
			.description("The amount of time to wait between checking the status of an operation.").required(true)
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).expressionLanguageSupported(true)
			.defaultValue("5 sec").build();

	public static final PropertyDescriptor STATUS_CHECK_COUNT = new PropertyDescriptor.Builder()
			.name("exec-spark-iactive-status-check-count").displayName("Status check count while waiting")
			.description("How many times status should be checked.It could be used to add a timeout. "
					+ "e.g. if status check count is 10 and status check interval is 5 then "
					+ "this processor would wait for 50 minutes before routing to failure.")
			.required(true).addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
			.expressionLanguageSupported(false).defaultValue("100").build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("FlowFiles that are successfully processed are sent to this relationship").build();

	public static final Relationship REL_WAIT = new Relationship.Builder().name("wait")
			.description("FlowFiles that are waiting on an available Spark session will be sent to this relationship")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("FlowFiles are routed to this relationship when they cannot be parsed").build();

	private volatile List<PropertyDescriptor> properties;
	private volatile Set<Relationship> relationships;

	@Override
	public void init(final ProcessorInitializationContext context) {
		List<PropertyDescriptor> properties = new ArrayList<>();
		properties.add(LIVY_URL);
		properties.add(CONNECT_TIMEOUT);
		properties.add(FILE_PATH);
		properties.add(CLASS_NAME);
		properties.add(PROXY_USER);
		properties.add(JARS);
		properties.add(ARGS);
		properties.add(ARGS_DELIM);
		properties.add(DRIVER_CORES);
		properties.add(DRIVER_MEMORY);
		properties.add(EXECUTOR_CORES);
		properties.add(EXECUTOR_MEMORY);
		properties.add(EXECUTOR_NUMBERS);
		properties.add(STATUS_CHECK_INTERVAL);
		properties.add(STATUS_CHECK_COUNT);
		this.properties = Collections.unmodifiableList(properties);

		Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_WAIT);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return relationships;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return properties;
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
		return new PropertyDescriptor.Builder()
				.description("Specifies the value for '" + propertyDescriptorName + "' for spark job")
				.name(propertyDescriptorName).dynamic(true).addValidator(StandardValidators.NON_BLANK_VALIDATOR)
				.build();
	}

	private SparkBatchPojo createPojoFromContext(ProcessContext context, FlowFile flowfile) {
		SparkBatchPojo pojo = new SparkBatchPojo();
		String args = context.getProperty(ARGS).evaluateAttributeExpressions(flowfile).getValue();
		String delim = context.getProperty(ARGS_DELIM).getValue();
		if (args != null) {
			List<String> argsArray = Arrays.stream(args.split(delim)).filter(StringUtils::isNotBlank).map(String::trim)
					.collect(Collectors.toList());
			pojo.setArgs(argsArray);
		}

		String jars = context.getProperty(JARS).getValue();
		if (jars != null) {
			List<String> jarsArray = Arrays.stream(jars.split(",")).filter(StringUtils::isNotBlank).map(String::trim)
					.collect(Collectors.toList());
			pojo.setJars(jarsArray);
		}
		pojo.setFile(context.getProperty(FILE_PATH).evaluateAttributeExpressions(flowfile).getValue());
		pojo.setClassName(context.getProperty(CLASS_NAME).evaluateAttributeExpressions(flowfile).getValue());
		if (!StringUtils.isEmpty(context.getProperty(DRIVER_CORES).getValue())) {
			pojo.setDriverCores(Integer.valueOf(context.getProperty(DRIVER_CORES).getValue()));
		}
		pojo.setDriverMemory(context.getProperty(DRIVER_MEMORY).getValue());
		if (!StringUtils.isEmpty(context.getProperty(EXECUTOR_CORES).getValue())) {
			pojo.setExecutorCores(Integer.valueOf(context.getProperty(EXECUTOR_CORES).getValue()));
		}
		pojo.setExecutorMemory(context.getProperty(EXECUTOR_MEMORY).getValue());

		if (!StringUtils.isEmpty(context.getProperty(EXECUTOR_NUMBERS).getValue())) {
			pojo.setNumExecutors(Integer.valueOf(context.getProperty(EXECUTOR_NUMBERS).getValue()));
		}
		pojo.setProxyUser(context.getProperty(PROXY_USER).getValue());
		Map<String, String> confMap = new HashMap<String, String>();
		for (Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
			if (entry.getKey().isDynamic()) {
				confMap.put(entry.getKey().getName(), entry.getValue());
			}

		}
		pojo.setConf(confMap);
		return pojo;

	}

	@Override
	public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}

		final ComponentLog log = getLogger();
		int connectTimeout = Math.toIntExact(context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS));

		final long statusCheckInterval = context.getProperty(STATUS_CHECK_INTERVAL)
				.evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.MILLISECONDS);
		SparkBatchPojo pojo = createPojoFromContext(context, flowFile);
		Gson gson = new Gson();
		String payload = gson.toJson(pojo);
		getLogger().info(payload);
		String livyUrl = context.getProperty(LIVY_URL).evaluateAttributeExpressions(flowFile).getValue();
		try {
			final JSONObject result = submitAndHandleJob(livyUrl, payload, statusCheckInterval, context,
					connectTimeout);
			log.info("LivyBatchProcessor Result of Job Submit: " + result);
			if (result == null) {
				session.transfer(flowFile, REL_FAILURE);
			} else {
				try {
					final JSONObject output = result.getJSONObject("appInfo");
					flowFile = session.write(flowFile, out -> out.write(output.toString().getBytes()));
					flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
					session.transfer(flowFile, REL_SUCCESS);
				} catch (JSONException je) {
					// The result doesn't contain the data, just send the output object as the flow
					// file content to failure (after penalizing)
					log.error(
							"Spark Session returned an error, sending the output JSON object as the flow file content to failure (after penalizing)");
					flowFile = session.write(flowFile, out -> out.write(result.toString().getBytes()));
					flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), APPLICATION_JSON);
					flowFile = session.penalize(flowFile);
					session.transfer(flowFile, REL_FAILURE);
				}
			}
		} catch (IOException ioe) {
			log.error("Failure processing flowfile {} due to {}, penalizing and routing to failure",
					new Object[] { flowFile, ioe.getMessage() }, ioe);
			flowFile = session.penalize(flowFile);
			session.transfer(flowFile, REL_FAILURE);
		}
	}

	private JSONObject submitAndHandleJob(String livyUrl, String payload, long statusCheckInterval,
			ProcessContext context, int connectTimeout) throws IOException {
		ComponentLog log = getLogger();
		String batchUrl = livyUrl + "/batches";
		JSONObject output = null;
		Map<String, String> headers = new HashMap<>();
		headers.put("Content-Type", APPLICATION_JSON);
		// headers.put("X-Requested-By", LivySessionService.USER);
		headers.put("Accept", "application/json");

		log.debug("submitAndHandleJob() Submitting Job to Spark via: " + batchUrl);
		int statusCheckCount = Integer.valueOf(context.getProperty(STATUS_CHECK_COUNT).getValue());
		try {
			JSONObject jobInfo = readJSONObjectFromUrlPOST(batchUrl, headers, payload, connectTimeout);
			log.info("submitAndHandleJob() Job Info: " + jobInfo);
			String sessionId = String.valueOf(jobInfo.getInt("id"));
			batchUrl = batchUrl + "/" + sessionId;
			jobInfo = readJSONObjectFromUrl(batchUrl, headers, connectTimeout);
			String jobState = jobInfo.getString("state");

			log.debug("submitAndHandleJob() New Job Info: " + jobInfo);
			Thread.sleep(statusCheckInterval);

			if (jobState.equalsIgnoreCase("available")) {
				log.debug("submitAndHandleJob() Job status is: " + jobState + ". returning output...");
				// output = jobInfo.getJSONObject("output");
				output = jobInfo;
			} else if (jobState.equalsIgnoreCase("running") || jobState.equalsIgnoreCase("waiting")
					|| jobState.equalsIgnoreCase("starting")) {
				while (!(jobState.equalsIgnoreCase("available") || jobState.equalsIgnoreCase("success")
						|| jobState.equalsIgnoreCase("dead"))) {
					if (jobState.equalsIgnoreCase("dead")) {
						throw new IOException(jobState);
					}
					log.debug("submitAndHandleJob() Job status is: " + jobState + ". Waiting for job to complete...");
					Thread.sleep(statusCheckInterval);
					jobInfo = readJSONObjectFromUrl(batchUrl, headers, connectTimeout);
					jobState = jobInfo.getString("state");
					statusCheckCount -= 1;
					if (statusCheckCount <= 0) {
						log.info("Can't wait more for this job to finish");
						throw new IOException("Timed out waiting for the job to finish.");
					}
				}
				// output = jobInfo.getJSONObject("output");
				output = jobInfo;
			} else if (jobState.equalsIgnoreCase("error") || jobState.equalsIgnoreCase("cancelled")
					|| jobState.equalsIgnoreCase("cancelling")) {
				log.debug("Job status is: " + jobState
						+ ". Job did not complete due to error or has been cancelled. Check SparkUI for details.");
				throw new IOException(jobState);
			}
		} catch (JSONException | InterruptedException e) {
			throw new IOException(e);
		}
		return output;
	}

	private JSONObject readJSONObjectFromUrlPOST(String urlString, Map<String, String> headers, String payload,
			int connectTimeout) throws IOException, JSONException {

		HttpURLConnection connection = getConnection(urlString, connectTimeout);
		connection.setRequestMethod("POST");
		connection.setDoOutput(true);

		for (Map.Entry<String, String> entry : headers.entrySet()) {
			connection.setRequestProperty(entry.getKey(), entry.getValue());
		}

		OutputStream os = connection.getOutputStream();
		os.write(payload.getBytes());
		os.flush();

		if (connection.getResponseCode() != HttpURLConnection.HTTP_OK
				&& connection.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode() + " : "
					+ connection.getResponseMessage());
		}

		InputStream content = connection.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
		String jsonText = IOUtils.toString(rd);
		getLogger().info("Livy response for job submit: " + jsonText);
		return new JSONObject(jsonText);
	}

	private JSONObject readJSONObjectFromUrl(final String urlString, final Map<String, String> headers,
			int connectTimeout) throws IOException, JSONException {

		HttpURLConnection connection = getConnection(urlString, connectTimeout);
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			connection.setRequestProperty(entry.getKey(), entry.getValue());
		}
		connection.setRequestMethod("GET");
		connection.setDoOutput(true);
		InputStream content = connection.getInputStream();
		BufferedReader rd = new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8));
		String jsonText = IOUtils.toString(rd);
		getLogger().info("Livy response for status check: " + jsonText);
		return new JSONObject(jsonText);
	}

	public HttpURLConnection getConnection(String urlString, int connectTimeout) throws IOException {
		URL url = new URL(urlString);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setConnectTimeout(connectTimeout);
		return connection;
	}

}