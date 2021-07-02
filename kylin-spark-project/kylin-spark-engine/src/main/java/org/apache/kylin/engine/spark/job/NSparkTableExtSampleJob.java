package org.apache.kylin.engine.spark.job;

import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.base.Preconditions;

public class NSparkTableExtSampleJob extends DefaultChainedExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTableExtSampleJob.class);

    // for reflection only
    public NSparkTableExtSampleJob() {

    }

    public static NSparkTableExtSampleJob create(String project, String submitter, long maxSampleCount,
            String tableName) {
        return create(project, submitter, maxSampleCount, tableName, UUID.randomUUID().toString());
    }

    public static NSparkTableExtSampleJob create(String project, String submitter, long maxSampleCount,
            String tableName, String jobId) {
        Preconditions.checkArgument(!project.isEmpty());
        Preconditions.checkArgument(!submitter.isEmpty());
        Preconditions.checkArgument(!tableName.isEmpty());

        NSparkTableExtSampleJob job = new NSparkTableExtSampleJob();
        job.setId(jobId);
        job.setName(ExecutableConstants.STEP_NAME_SAMPLE_TABLE + "-" + tableName);
        job.setProjectName(project);
        job.setSubmitter(submitter);
        job.setParam(MetadataConstants.TABLE_NAME, tableName);
        job.setParam(MetadataConstants.P_JOB_ID, jobId);
        job.setParam(MetadataConstants.P_PROJECT_NAME, project);
        job.setParam(MetadataConstants.TABLE_SAMPLE_MAX_COUNT, String.valueOf(maxSampleCount));

        //set param for job metrics
        KylinConfig config = KylinConfig.getInstanceFromEnv().base();
        JobStepFactory.addStep(job, JobStepType.TABLE_SAMPLE_HIVE, config);

        return job;
    }
}
