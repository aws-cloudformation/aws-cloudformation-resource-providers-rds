package software.amazon.rds.dbcluster.util;

import com.amazonaws.util.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.rds.dbcluster.EngineMode;
import software.amazon.rds.dbcluster.ResourceModel;

import static software.amazon.rds.common.util.DifferenceUtils.diff;

public class ResourceModelHelper {

    public static boolean isRestoreToPointInTime(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBClusterIdentifier());
    }

    public static boolean isRestoreFromSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getSnapshotIdentifier());
    }

    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return isRestoreFromSnapshot(model) || isRestoreToPointInTime(model);
    }

    public static boolean shouldEnableHttpEndpointV2AfterCreate(final ResourceModel model) {
        return BooleanUtils.isTrue(model.getEnableHttpEndpoint()) && !EngineMode.Serverless.equals(EngineMode.fromString(model.getEngineMode()));
    }

    public static boolean hasServerlessV2ScalingConfigurationChanged(final ResourceModel previousModel, final ResourceModel desiredModel) {
        return diff(previousModel.getServerlessV2ScalingConfiguration(), desiredModel.getServerlessV2ScalingConfiguration()) != null;
    }
}
