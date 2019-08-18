package com.ssqcyy.nifi.processor;

import java.util.List;
import java.util.Map;

public class SparkBatchPojo {

	String file;
	String proxyUser;
	String className	;
	List<String>  args;
	List<String> jars;	

	String driverMemory;
	int driverCores;
	String executorMemory;
	int executorCores;
	int numExecutors;
	Map<String, String> conf;
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	public String getProxyUser() {
		return proxyUser;
	}
	public void setProxyUser(String proxyUser) {
		this.proxyUser = proxyUser;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public List<String> getArgs() {
		return args;
	}
	public void setArgs(List<String> args) {
		this.args = args;
	}
	public List<String> getJars() {
		return jars;
	}
	public void setJars(List<String> jars) {
		this.jars = jars;
	}
	public String getDriverMemory() {
		return driverMemory;
	}
	public void setDriverMemory(String driverMemory) {
		this.driverMemory = driverMemory;
	}
	public int getDriverCores() {
		return driverCores;
	}
	public void setDriverCores(int driverCores) {
		this.driverCores = driverCores;
	}
	public String getExecutorMemory() {
		return executorMemory;
	}
	public void setExecutorMemory(String executorMemory) {
		this.executorMemory = executorMemory;
	}
	public int getExecutorCores() {
		return executorCores;
	}
	public void setExecutorCores(int executorCores) {
		this.executorCores = executorCores;
	}
	public int getNumExecutors() {
		return numExecutors;
	}
	public void setNumExecutors(int numExecutors) {
		this.numExecutors = numExecutors;
	}
	public Map<String, String> getConf() {
		return conf;
	}
	public void setConf(Map<String, String> conf) {
		this.conf = conf;
	}
	
	
}