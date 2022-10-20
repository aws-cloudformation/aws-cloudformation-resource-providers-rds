package software.amazon.rds.dbcluster;

import java.util.Collections;
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

    private static final String ENGINE_AURORA = "aurora";
    private static final String ENGINE_AURORA_MYSQL = "aurora-mysql";
    private static final String ENGINE_AURORA_POSTGRESQL = "aurora-postgresql";

    private static final Map<EngineMode, Map<String, Integer>> DEFAULT_ENGINE_PORTS = ImmutableMap.of(
            EngineMode.Provisioned, ImmutableMap.of(
                    ENGINE_AURORA, 3306,
                    ENGINE_AURORA_MYSQL, 3306,
                    ENGINE_AURORA_POSTGRESQL, 3306
            ),
            EngineMode.Serverless, ImmutableMap.of(
                    ENGINE_AURORA, 3306,
                    ENGINE_AURORA_MYSQL, 3306,
                    ENGINE_AURORA_POSTGRESQL, 5432
            )
    );

    public static ResourceModel setDefaults(final ResourceModel resourceModel) {

        final Integer port = resourceModel.getPort();
        final Integer backupRetentionPeriod = resourceModel.getBackupRetentionPeriod();
        final List<DBClusterRole> associatedRoles = resourceModel.getAssociatedRoles();
        final ScalingConfiguration scalingConfiguration = resourceModel.getScalingConfiguration();

        resourceModel.setBackupRetentionPeriod(backupRetentionPeriod == null ? DEFAULT_BACKUP_RETENTION_PERIOD : backupRetentionPeriod);
        resourceModel.setAssociatedRoles(associatedRoles == null ? Lists.newArrayList() : associatedRoles);

        if (EngineMode.Serverless.equalsString(resourceModel.getEngineMode())) {
            ScalingConfiguration defaultScalingConfiguration = ScalingConfiguration.builder()
                    .secondsUntilAutoPause(DEFAULT_SECONDS_UNTIL_AUTO_PAUSE)
                    .autoPause(DEFAULT_PAUSE_DEFAULT)
                    .minCapacity(DEFAULT_MIN_CAPACITY)
                    .maxCapacity(DEFAULT_MAX_CAPACITY)
                    .build();
            resourceModel.setScalingConfiguration(scalingConfiguration == null ? defaultScalingConfiguration : scalingConfiguration);
        }

        final EngineMode engineMode = EngineMode.fromString(resourceModel.getEngineMode());

        resourceModel.setPort(port != null ? port : getDefaultPortForEngine(resourceModel.getEngine(), engineMode));

        return resourceModel;
    }

    private static int getDefaultPortForEngine(final String engine, EngineMode engineMode) {
        if (engineMode == null) {
            engineMode = EngineMode.Provisioned;
        }
        return DEFAULT_ENGINE_PORTS
                .getOrDefault(engineMode, Collections.emptyMap())
                .getOrDefault(engine, DEFAULT_PORT);
    }
}
