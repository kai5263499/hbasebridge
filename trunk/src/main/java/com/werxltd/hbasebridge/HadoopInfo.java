package com.werxltd.hbasebridge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobQueueInfo;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.json.JSONException;
import org.json.JSONObject;

@SuppressWarnings("deprecation")
public class HadoopInfo {
	private Configuration hconf;

	public HadoopInfo() {
		hconf = new Configuration();
	}

	public JSONObject clusterstatus() throws JSONException, IOException {
		JSONObject result = new JSONObject();

		JobConf jobconf = new JobConf(hconf);
		JobClient jc = new JobClient(jobconf);
		ClusterStatus cs = jc.getClusterStatus(true);

		result.put("activetrackernames", cs.getActiveTrackerNames());
		result.put("blacklistedtrackernames", cs.getBlacklistedTrackerNames());
		result.put("blacklistedtrackers", cs.getBlacklistedTrackers());
		result.put("maptasks", cs.getMapTasks());
		result.put("maxmemory", cs.getMaxMemory());
		result.put("maxmaptasks", cs.getMaxMapTasks());
		result.put("maxreducetasks", cs.getMaxReduceTasks());
		result.put("reducetasks", cs.getReduceTasks());
		result.put("tasktrackers", cs.getTaskTrackers());
		result.put("ttyexpiryinterval", cs.getTTExpiryInterval());
		result.put("usedmemory", cs.getUsedMemory());

		result.put("jobtrackerstate", cs.getJobTrackerState().name()
				.toLowerCase());

		result.put("jobqueues", jobstatus());

		// result.put("jobhistory", getJobHistoryStatus());

		return result;
	}

	public JSONObject jobstatus() throws IOException, JSONException {
		JSONObject result = new JSONObject();

		JobConf jobconf = new JobConf(hconf);
		JobClient jc = new JobClient(jobconf);

		JobQueueInfo[] queues = jc.getQueues();

		for (JobQueueInfo queue : queues) {
			String schedInfo = queue.getSchedulingInfo();
			if (schedInfo.trim().equals("")) {
				schedInfo = "N/A";
			}

			JSONObject queueObj = new JSONObject();
			String queueName = queue.getQueueName();

			queueObj.put("name", queueName);

			JobStatus[] jobs = new JobStatus[0];
			jobs = jc.getJobsFromQueue(queueName);

			for (JobStatus job : jobs) {
				RunningJob hjob = jc.getJob(job.getJobID());
				JSONObject jobObj = new JSONObject();
				jobObj.put("name", hjob.getJobName());
				jobObj.put("filename", hjob.getJobFile());
				jobObj.put("username", job.getUsername());
				jobObj.put("complete", job.isJobComplete());
				jobObj.put("starttime", job.getStartTime());
				jobObj.put("mapprogress", job.mapProgress());
				jobObj.put("reduceprogress", job.reduceProgress());
				jobObj.put("setupprogress", job.setupProgress());
				jobObj.put("schedulinginfo", job.getSchedulingInfo());

				TaskReport[] ctrs = jc.getCleanupTaskReports(job.getJobID());
				JSONObject ctrObj = new JSONObject();
				for (TaskReport ctr : ctrs) {
					ctrObj.put("state", ctr.getState());
				}
				jobObj.append("cleanuptasks", ctrObj);

				String stateStr = "";
				switch (job.getRunState()) {
				case JobStatus.FAILED:
					stateStr = "failed";
					break;
				case JobStatus.KILLED:
					stateStr = "killed";
					break;
				case JobStatus.PREP:
					stateStr = "prep";
					break;
				case JobStatus.RUNNING:
					stateStr = "running";
					break;
				case JobStatus.SUCCEEDED:
					stateStr = "succeeded";
					break;
				}
				jobObj.put("runstate", stateStr);

				org.apache.hadoop.mapred.JobPriority jobpri = job
						.getJobPriority();
				jobObj.put("jobpriority", jobpri.name().toLowerCase());

				queueObj.append("jobs", jobObj);
			}

			// LOG.error(queue.getQueueName());
			// System.out.printf("Queue Name : %s \n",
			// queue.getQueueName());
			// System.out.printf("Scheduling Info : %s \n",queue.getSchedulingInfo());

			result.append("queues", queueObj);
		}

		// LOG.error(jobs.length);
		// result.append("jobs", jobs);
		// This takes quite a while if your JobTracker is very active..
		/*
		 * for (JobStatus job : jobs) { LOG.error(job.toString());
		 * result.append("job", job); }
		 */

		return result;
	}
}
