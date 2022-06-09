package software.amazon.rds.common.error;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.utils.StringUtils;

public enum ErrorCode {
    AccessDenied("AccessDenied"),
    AccessDeniedException("AccessDeniedException"),
    ClientUnavailable("ClientUnavailable"),
    DBClusterAlreadyExistsFault("DBClusterAlreadyExistsFault"),
    DBInstanceAlreadyExists("DBInstanceAlreadyExists"),
    DBInstanceNotFound("DBInstanceNotFound"),
    DBParameterGroupNotFound("DBParameterGroupNotFound"),
    DBSecurityGroupNotFound("DBSecurityGroupNotFound"),
    DBSnapshotAlreadyExists("DBSnapshotAlreadyExists"),
    DBSnapshotNotFound("DBSnapshotNotFound"),
    DBSubnetGroupNotFoundFault("DBSubnetGroupNotFoundFault"),
    InstanceQuotaExceeded("InstanceQuotaExceeded"),
    InsufficientDBInstanceCapacity("InsufficientDBInstanceCapacity"),
    InternalFailure("InternalFailure"),
    InvalidDBInstanceState("InvalidDBInstanceState"),
    InvalidDBSecurityGroupState("InvalidDBSecurityGroupState"),
    InvalidDBSnapshotState("InvalidDBSnapshotState"),
    InvalidOptionGroupStateFault("InvalidOptionGroupStateFault"),
    InvalidParameterCombination("InvalidParameterCombination"),
    InvalidParameterValue("InvalidParameterValue"),
    InvalidRestoreFault("InvalidRestoreFault"),
    InvalidVPCNetworkStateFault("InvalidVPCNetworkStateFault"),
    KMSKeyNotAccessibleFault("KMSKeyNotAccessibleFault"),
    MissingParameter("MissingParameter"),
    NotAuthorized("NotAuthorized"),
    ProvisionedIopsNotAvailableInAZFault("ProvisionedIopsNotAvailableInAZFault"),
    SnapshotQuotaExceeded("SnapshotQuotaExceeded"),
    StorageQuotaExceeded("StorageQuotaExceeded"),
    ThrottlingException("ThrottlingException"),
    Throttling("Throttling");

    private final String code;

    ErrorCode(final String code) {
        this.code = code;
    }

    public static ErrorCode fromString(final String errorStr) {
        if (StringUtils.isNotBlank(errorStr)) {
            for (final ErrorCode errorCode : ErrorCode.values()) {
                if (errorCode.equals(errorStr)) {
                    return errorCode;
                }
            }
        }
        return null;
    }

    public static ErrorCode fromException(final AwsServiceException exception) {
        final AwsErrorDetails errorDetails = exception.awsErrorDetails();
        if (errorDetails != null) {
            return ErrorCode.fromString(errorDetails.errorCode());
        }
        return null;
    }

    @Override
    public String toString() {
        return code;
    }

    public boolean equals(final String cmp) {
        return code.equals(cmp);
    }
}
