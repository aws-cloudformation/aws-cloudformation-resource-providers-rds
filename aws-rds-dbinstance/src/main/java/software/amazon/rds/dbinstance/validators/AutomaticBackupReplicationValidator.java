package software.amazon.rds.dbinstance.validators;

import com.amazonaws.util.StringUtils;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.dbinstance.ResourceModel;
import software.amazon.rds.dbinstance.util.ResourceModelHelper;

public class AutomaticBackupReplicationValidator {
    public static void validateRequest(final ResourceModel model) throws RequestValidationException {
        if (StringUtils.isNullOrEmpty(ResourceModelHelper.getAutomaticBackupReplicationRegion(model))) {
            if (!StringUtils.isNullOrEmpty(ResourceModelHelper.getAutomaticBackupReplicationKmsKeyId(model) )) {
                throw new RequestValidationException("You must specify the AutomaticBackupReplicationRegion parameter when setting the AutomaticBackupReplicationKmsKeyId parameter.");
            }
            if (ResourceModelHelper.getAutomaticBackupReplicationRetentionPeriod(model) != null) {
                throw new RequestValidationException("You must specify the AutomaticBackupReplicationRegion parameter when setting the AutomaticBackupReplicationRetentionPeriod parameter.");
            }
        } else {
            if (ResourceModelHelper.getBackupRetentionPeriod(model) != null && ResourceModelHelper.getBackupRetentionPeriod(model) == 0) {
                throw new RequestValidationException("AutomaticBackupReplicationRegion cannot be specified when BackupRetentionPeriod is 0.");
            }
        }
    }
}
