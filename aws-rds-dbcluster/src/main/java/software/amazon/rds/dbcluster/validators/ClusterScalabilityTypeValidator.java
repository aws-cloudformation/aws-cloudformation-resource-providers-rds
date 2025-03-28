package software.amazon.rds.dbcluster.validators;

import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.dbcluster.ResourceModel;
import software.amazon.rds.dbcluster.util.ResourceModelHelper;

public class ClusterScalabilityTypeValidator {
    public static void validateRequest(final ResourceModel model) throws RequestValidationException {
        if (ResourceModelHelper.isRestoreToPointInTime(model) && model.getClusterScalabilityType() != null) {
            throw new RequestValidationException("The ClusterScalabilityType parameter is not allowed when creating a cluster from a point-in-time restore. This value is automatically inherited from the source DB cluster.");
        }

        if (ResourceModelHelper.isRestoreFromSnapshot(model) && model.getClusterScalabilityType() != null) {
            throw new RequestValidationException("The ClusterScalabilityType parameter is not allowed when creating a DB cluster from a snapshot restore. This value is automatically inherited from the snapshot.");
        }

    }
}
