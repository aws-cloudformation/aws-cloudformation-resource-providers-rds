package software.amazon.rds.dbinstance.common;

import software.amazon.awssdk.services.rds.model.AuthorizationNotFoundException;
import software.amazon.awssdk.services.rds.model.CertificateNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DbClusterSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceAutomatedBackupQuotaExceededException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbInstanceRoleNotFoundException;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupDoesNotCoverEnoughAZsException;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DbUpgradeDependencyFailureException;
import software.amazon.awssdk.services.rds.model.DomainNotFoundException;
import software.amazon.awssdk.services.rds.model.InstanceQuotaExceededException;
import software.amazon.awssdk.services.rds.model.InsufficientDbInstanceCapacityException;
import software.amazon.awssdk.services.rds.model.InvalidDbClusterStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceAutomatedBackupStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSecurityGroupStateException;
import software.amazon.awssdk.services.rds.model.InvalidDbSnapshotStateException;
import software.amazon.awssdk.services.rds.model.InvalidRestoreException;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.InvalidVpcNetworkStateException;
import software.amazon.awssdk.services.rds.model.KmsKeyNotAccessibleException;
import software.amazon.awssdk.services.rds.model.NetworkTypeNotSupportedException;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.ProvisionedIopsNotAvailableInAzException;
import software.amazon.awssdk.services.rds.model.SnapshotQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageQuotaExceededException;
import software.amazon.awssdk.services.rds.model.StorageTypeNotSupportedException;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.error.ErrorRuleSet;
import software.amazon.rds.common.error.ErrorStatus;
import software.amazon.rds.common.handler.Commons;

import java.util.function.Function;

public class ErrorRuleSets {

    public static final ErrorRuleSet DEFAULT_DB_INSTANCE = ErrorRuleSet
        .extend(Commons.DEFAULT_ERROR_RULE_SET)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
            ErrorCode.InstanceQuotaExceeded,
            ErrorCode.InsufficientDBInstanceCapacity,
            ErrorCode.SnapshotQuotaExceeded,
            ErrorCode.StorageQuotaExceeded)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.DBSubnetGroupNotAllowedFault,
            ErrorCode.InvalidParameterCombination,
            ErrorCode.InvalidParameterValue,
            ErrorCode.InvalidVPCNetworkStateFault,
            ErrorCode.KMSKeyNotAccessibleFault,
            ErrorCode.MissingParameter,
            ErrorCode.ProvisionedIopsNotAvailableInAZFault,
            ErrorCode.StorageTypeNotSupportedFault)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBClusterNotFoundFault,
            ErrorCode.DBParameterGroupNotFound,
            ErrorCode.DBSecurityGroupNotFound,
            ErrorCode.DBSnapshotNotFound,
            ErrorCode.DBSubnetGroupNotFoundFault)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            CertificateNotFoundException.class,
            DbClusterNotFoundException.class,
            DbInstanceNotFoundException.class,
            DbParameterGroupNotFoundException.class,
            DbSecurityGroupNotFoundException.class,
            DbSnapshotNotFoundException.class,
            DbClusterSnapshotNotFoundException.class,
            DbSubnetGroupNotFoundException.class,
            DomainNotFoundException.class,
            OptionGroupNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
            DbInstanceAutomatedBackupQuotaExceededException.class,
            InsufficientDbInstanceCapacityException.class,
            InstanceQuotaExceededException.class,
            SnapshotQuotaExceededException.class,
            StorageQuotaExceededException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbInstanceStateException.class,
            InvalidDbClusterStateException.class,
            DbUpgradeDependencyFailureException.class,
            InvalidDbSecurityGroupStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            AuthorizationNotFoundException.class,
            DbSubnetGroupDoesNotCoverEnoughAZsException.class,
            InvalidVpcNetworkStateException.class,
            KmsKeyNotAccessibleException.class,
            NetworkTypeNotSupportedException.class,
            ProvisionedIopsNotAvailableInAzException.class,
            StorageTypeNotSupportedException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.GeneralServiceException),
            InvalidSubnetException.class)
        .build();

    public static final ErrorRuleSet DB_INSTANCE_FETCH_ENGINE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            CfnInvalidRequestException.class)
        .build();

    public static final ErrorRuleSet RESTORE_DB_INSTANCE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            ErrorCode.DBInstanceAlreadyExists)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.InvalidDBSnapshotState,
            ErrorCode.InvalidRestoreFault)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            InvalidDbSnapshotStateException.class,
            InvalidRestoreException.class)
        .build();
    public static final ErrorRuleSet CREATE_DB_INSTANCE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            ErrorCode.DBInstanceAlreadyExists)
        .withErrorClasses(
            ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .build();

    public static final ErrorRuleSet CREATE_DB_INSTANCE_READ_REPLICA = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            ErrorCode.DBInstanceAlreadyExists)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbClusterNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbClusterStateException.class)
        .build();

    public static final ErrorRuleSet REBOOT_DB_INSTANCE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBInstanceNotFound)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            ErrorCode.InvalidDBInstanceState)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbInstanceNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbInstanceStateException.class)
        .build();

    public static final ErrorRuleSet MODIFY_DB_INSTANCE_AUTOMATIC_BACKUP_REPLICATION = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorClasses(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
            InvalidDbInstanceAutomatedBackupStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
            DbInstanceAutomatedBackupQuotaExceededException.class)
        .build();

    public static final ErrorRuleSet MODIFY_DB_INSTANCE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            ErrorCode.InvalidDBInstanceState)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBInstanceNotFound)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.InvalidDBSecurityGroupState,
            ErrorCode.InvalidParameterCombination)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbInstanceStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbInstanceNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            CfnInvalidRequestException.class,
            InvalidDbSecurityGroupStateException.class)
        .build();

    public static final ErrorRuleSet UPDATE_ASSOCIATED_ROLES = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorClasses(ErrorStatus.ignore(),
            DbInstanceRoleAlreadyExistsException.class,
            DbInstanceRoleNotFoundException.class)
        .build();

    private static final Function<Exception, ErrorStatus> ignoreDBInstanceBeingDeletedConditionalErrorStatus = exception -> {
        if (isDBInstanceBeingDeletedException(exception)) {
            return ErrorStatus.ignore(OperationStatus.IN_PROGRESS);
        }
        return ErrorStatus.failWith(HandlerErrorCode.ResourceConflict);
    };

    public static final ErrorRuleSet DELETE_DB_INSTANCE = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.InvalidParameterValue)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBInstanceNotFound)
        .withErrorCodes(ErrorStatus.conditional(ignoreDBInstanceBeingDeletedConditionalErrorStatus),
            ErrorCode.InvalidDBInstanceState)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.DBSnapshotAlreadyExists)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbInstanceNotFoundException.class)
        .withErrorClasses(ErrorStatus.conditional(ignoreDBInstanceBeingDeletedConditionalErrorStatus),
            InvalidDbInstanceStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            DbSnapshotAlreadyExistsException.class)
        .build();

    // Note: looking up this error message fragment is the only way to distinguish between an already deleting
    // instance and any other invalid states (e.g. a stopped instance). It relies on a specific error text returned by
    // AWS RDS API. The message text is by no means guarded by any public contract. This error message can change
    // in the future with no prior notice by AWS RDS. A change in this error message would cause a CFN stack failure
    // upon a stack deletion: if an instance is being deleted out-of-bounds. This is a pretty corner (still common) case
    // where the CFN handler is trying to help the customer. A regular stack deletion will not be impacted.
    // Considered bounded-safe.
    private static final String IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT = "is already being deleted";

    private static boolean isDBInstanceBeingDeletedException(final Exception e) {
        if (e instanceof InvalidDbInstanceStateException) {
            return looksLikeDBInstanceBeingDeletedMessage(e.getMessage());
        }
        return false;
    }

    private static boolean looksLikeDBInstanceBeingDeletedMessage(final String message) {
        if (StringUtils.isBlank(message)) {
            return false;
        }
        return message.contains(ErrorRuleSets.IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT);
    }
}
