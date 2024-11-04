package software.amazon.rds.dbinstance.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
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
import software.amazon.rds.dbinstance.BaseHandlerStd;

import java.util.function.Function;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Errors {

    public static final ErrorRuleSet DEFAULT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
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

    public static final ErrorRuleSet DELETE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.InvalidParameterValue)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBInstanceNotFound)
        .withErrorCodes(ErrorStatus.conditional(Errors::ignoreDBInstanceBeingDeletedConditionalErrorStatus),
            ErrorCode.InvalidDBInstanceState)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            ErrorCode.DBSnapshotAlreadyExists)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbInstanceNotFoundException.class)
        .withErrorClasses(ErrorStatus.conditional(Errors::ignoreDBInstanceBeingDeletedConditionalErrorStatus),
            InvalidDbInstanceStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            DbSnapshotAlreadyExistsException.class)
        .build();

    public static final ErrorRuleSet UPDATE_ASSOCIATED_ROLES_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorClasses(ErrorStatus.ignore(),
            DbInstanceRoleAlreadyExistsException.class,
            DbInstanceRoleNotFoundException.class)
        .build();

    public static final ErrorRuleSet MODIFY_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
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

    public static final ErrorRuleSet MODIFY_DB_INSTANCE_AUTOMATIC_BACKUP_REPLICATION_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorClasses(ErrorStatus.ignore(OperationStatus.IN_PROGRESS),
            InvalidDbInstanceAutomatedBackupStateException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ServiceLimitExceeded),
            DbInstanceAutomatedBackupQuotaExceededException.class)
        .build();

    public static final ErrorRuleSet REBOOT_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            ErrorCode.DBInstanceNotFound)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            ErrorCode.InvalidDBInstanceState)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbInstanceNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbInstanceStateException.class)
        .build();

    public static final ErrorRuleSet CREATE_DB_INSTANCE_READ_REPLICA_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            ErrorCode.DBInstanceAlreadyExists)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.NotFound),
            DbClusterNotFoundException.class)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.ResourceConflict),
            InvalidDbClusterStateException.class)
        .build();

    public static final ErrorRuleSet RESTORE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
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

    public static final ErrorRuleSet DB_INSTANCE_FETCH_ENGINE_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorClasses(ErrorStatus.failWith(HandlerErrorCode.InvalidRequest),
            CfnInvalidRequestException.class)
        .build();

    public static final ErrorRuleSet CREATE_DB_INSTANCE_ERROR_RULE_SET = ErrorRuleSet
        .extend(DEFAULT_DB_INSTANCE_ERROR_RULE_SET)
        .withErrorCodes(ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            ErrorCode.DBInstanceAlreadyExists)
        .withErrorClasses(
            ErrorStatus.failWith(HandlerErrorCode.AlreadyExists),
            DbInstanceAlreadyExistsException.class)
        .build();

    private static ErrorStatus ignoreDBInstanceBeingDeletedConditionalErrorStatus(final Exception exception) {
        if (isDBInstanceBeingDeletedException(exception)) {
            return ErrorStatus.ignore(OperationStatus.IN_PROGRESS);
        }
        return ErrorStatus.failWith(HandlerErrorCode.ResourceConflict);
    };

    private static boolean isDBInstanceBeingDeletedException(final Exception e) {
        if (e instanceof InvalidDbInstanceStateException) {
            return looksLikeDBInstanceBeingDeletedMessage(e.getMessage());
        }
        return false;
    }

    private static final String IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT = "is already being deleted";

    private static boolean looksLikeDBInstanceBeingDeletedMessage(final String message) {
        if (StringUtils.isBlank(message)) {
            return false;
        }
        return message.contains(IS_ALREADY_BEING_DELETED_ERROR_FRAGMENT);
    }
}
