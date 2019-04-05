package ambition.blink.job;

import org.apache.flink.api.common.Plan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.graph.StreamGraph;
import ambition.blink.common.job.JobParameter;

import java.util.Map;

/**
 * @Author: wpl
 */
public interface JobClient {

   Plan getJobPlan(JobParameter jobParameter, Map<String,String> extParams) throws Exception;

   JobGraph getJobGraph(JobParameter jobParameter, Map<String,String> extParams) throws Exception;

   StreamGraph getStreamGraph(JobParameter jobParameter, Map<String,String> extParams) throws Exception;
}
