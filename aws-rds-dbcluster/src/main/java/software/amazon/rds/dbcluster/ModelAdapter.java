package software.amazon.rds.dbcluster;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ModelAdapter {
    private static final boolean DEFAULT_PAUSE_DEFAULT = true;
    private static final int DEFAULT_BACKUP_RETENTION_PERIOD = 1;
    private static final int DEFAULT_MAX_CAPACITY = 16;
    private static final int DEFAULT_MIN_CAPACITY = 2;
    private static final int DEFAULT_SECONDS_UNTIL_AUTO_PAUSE = 300;
    private static final int DEFAULT_PORT = 3306;
    private static final String SERVERLESS_ENGINE_MODE = "serverless";

    private static final Map<String, Integer> DEFAULT_ENGINE_PORTS = ImmutableMap.of(
            "aurora-postgresql", 5432
    );

    public static ResourceModel setDefaults(final ResourceModel resourceModel) {

        final Integer port = resourceModel.getPort();
        final Integer backupRetentionPeriod = resourceModel.getBackupRetentionPeriod();
        final List<DBClusterRole> associatedRoles = resourceModel.getAssociatedRoles();
        final ScalingConfiguration scalingConfiguration = resourceModel.getScalingConfiguration();

        resourceModel.setBackupRetentionPeriod(backupRetentionPeriod == null ? DEFAULT_BACKUP_RETENTION_PERIOD : backupRetentionPeriod);
        resourceModel.setAssociatedRoles(associatedRoles == null ? Lists.newArrayList() : associatedRoles);

        if (SERVERLESS_ENGINE_MODE.equalsIgnoreCase(resourceModel.getEngineMode())) {
            ScalingConfiguration defaultScalingConfiguration = ScalingConfiguration.builder()
                    .secondsUntilAutoPause(DEFAULT_SECONDS_UNTIL_AUTO_PAUSE)
                    .autoPause(DEFAULT_PAUSE_DEFAULT)
                    .minCapacity(DEFAULT_MIN_CAPACITY)
                    .maxCapacity(DEFAULT_MAX_CAPACITY)
                    .build();
            resourceModel.setScalingConfiguration(scalingConfiguration == null ? defaultScalingConfiguration : scalingConfiguration);
        } else {
            resourceModel.setPort(port != null ? port : getDefaultPortForEngine(resourceModel.getEngine()));
        }

        return resourceModel;
    }

    private static int getDefaultPortForEngine(final String engine) {
        return DEFAULT_ENGINE_PORTS.getOrDefault(engine, DEFAULT_PORT);
    }
}
