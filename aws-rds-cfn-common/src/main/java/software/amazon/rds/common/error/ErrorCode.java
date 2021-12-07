package software.amazon.rds.common.error;

import software.amazon.awssdk.utils.StringUtils;

public enum ErrorCode {
    AccessDeniedException("AccessDeniedException"),
    ClientUnavailable("ClientUnavailable"),
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
    ThrottlingException("ThrottlingException");

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

    @Override
    public String toString() {
        return code;
    }

    public boolean equals(final String cmp) {
        return code.equals(cmp);
    }
}
