package software.amazon.rds.dbinstance.validators;

import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

public class OracleCustomSystemId {
    public static void validateRequest(final ResourceModel model) throws RequestValidationException {
        if (ResourceModelHelper.isRestoreToPointInTime(model) && model.getDBSystemId() != null) {
            throw new RequestValidationException("The DBSystemId parameter cannot be specified when you create a DB instance from a point-in-time restore.");
        }

        if (ResourceModelHelper.isDBInstanceReadReplica(model) && model.getDBSystemId() != null) {
            throw new RequestValidationException("The DBSystemId parameter cannot be specified when you create a read replica.");
        }

        if (ResourceModelHelper.isRestoreFromSnapshot(model) && model.getDBSystemId() != null) {
            throw new RequestValidationException("The DBSystemId parameter cannot be specified when you create a DB instance from a snapshot restore.");
        }
    }
}
