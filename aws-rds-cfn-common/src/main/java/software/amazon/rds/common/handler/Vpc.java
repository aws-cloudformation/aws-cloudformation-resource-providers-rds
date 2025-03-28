package software.amazon.rds.common.handler;

import com.amazonaws.util.CollectionUtils;

import java.util.List;

public final class Vpc {

    public Vpc() {
    }

    /**
     * Is very important that we never reset to default VPC when both previous and desired security group is null,
     * or we would be potentially doing a modification that is not clearly intended from the model.
     */
    public static boolean shouldSetDefaultVpcId(
        final List<String> previousResourceStateVPCSecurityGroups,
        final List<String> desiredResourceStateVPCSecurityGroups
    ) {
        if (!CollectionUtils.isNullOrEmpty(previousResourceStateVPCSecurityGroups) && CollectionUtils.isNullOrEmpty(desiredResourceStateVPCSecurityGroups)) {
            // The only condition when we should update the default VPC is when the model is unsetting the securityGroup, and never in any other condition.
            // For example, when a customer import an existing resource (DBInstance or DBCluster), we should never change the VPC security groups regardless
            // of what the model says, because is not intuitive from the model definition that we are changing to a default VPC
            return true;
        }
        return false;
    }
}
