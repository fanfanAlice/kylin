package org.apache.kylin.engine.spark.job;

import org.apache.kylin.job.constant.ExecutableConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NSparkSampleTableStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkSampleTableStep.class);

    // called by reflection
    public NSparkSampleTableStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_SAMPLE_TABLE);
    }

}
