package software.amazon.rds.dbinstance;

import com.amazonaws.arn.Arn;
import com.amazonaws.util.CollectionUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.BooleanUtils;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DomainMembership;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.awssdk.services.rds.model.PendingModifiedValues;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.dbinstance.status.DBInstanceStatus;
import software.amazon.rds.dbinstance.status.DBParameterGroupStatus;
import software.amazon.rds.dbinstance.status.DomainMembershipStatus;
import software.amazon.rds.dbinstance.status.OptionGroupStatus;
import software.amazon.rds.dbinstance.status.ReadReplicaStatus;
import software.amazon.rds.dbinstance.status.VPCSecurityGroupStatus;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DBInstancePredicates {

    private static final String SECRET_STATUS_ACTIVE = "active";
    private static final List<String> RDS_CUSTOM_ORACLE_ENGINES = ImmutableList.of(
            "custom-oracle-ee",
            "custom-oracle-ee-cdb"
    );
    private static final String READ_REPLICA_STATUS_TYPE = "read replication";

    public static void assertNoDBInstanceTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final DBInstanceStatus status = DBInstanceStatus.fromString(dbInstance.dbInstanceStatus());
        if (status != null && status.isTerminal()) {
            throw new CfnNotStabilizedException(new Exception("DB Instance is in state: " + status.toString()));
        }
    }

    public static void assertNoOptionGroupTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final List<OptionGroupMembership> termOptionGroups = Optional.ofNullable(dbInstance.optionGroupMemberships()).orElse(Collections.emptyList())
                .stream()
                .filter(optionGroup -> {
                    final OptionGroupStatus status = OptionGroupStatus.fromString(optionGroup.status());
                    return status != null && status.isTerminal();
                })
                .collect(Collectors.toList());

        if (!termOptionGroups.isEmpty()) {
            throw new CfnNotStabilizedException(new Exception(
                    String.format("OptionGroup %s is in a terminal state",
                            termOptionGroups.get(0).optionGroupName())));
        }
    }

    public static void assertNoDomainMembershipTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        final List<DomainMembership> terminalDomainMemberships = Optional.ofNullable(dbInstance.domainMemberships()).orElse(Collections.emptyList())
                .stream()
                .filter(domainMembership -> {
                    final DomainMembershipStatus status = DomainMembershipStatus.fromString(domainMembership.status());
                    return status != null && status.isTerminal();
                })
                .collect(Collectors.toList());

        if (!terminalDomainMemberships.isEmpty()) {
            throw new CfnNotStabilizedException(new Exception(String.format("Domain %s is in a terminal state",
                    terminalDomainMemberships.get(0).domain())));
        }
    }

    public static void assertNoTerminalStatus(final DBInstance dbInstance) throws CfnNotStabilizedException {
        assertNoDBInstanceTerminalStatus(dbInstance);
        assertNoOptionGroupTerminalStatus(dbInstance);
        assertNoDomainMembershipTerminalStatus(dbInstance);
    }

    public static boolean isInstanceStabilizedAfterReplicationStop(
        final DBInstance dbInstance,
        final ResourceModel model
    ) {
        assertNoTerminalStatus(dbInstance);
        return isDBInstanceAvailable(dbInstance)
                && !dbInstance.hasDbInstanceAutomatedBackupsReplications();
    }

    public static boolean isDBInstanceAvailable(final DBInstance dbInstance) {
        return DBInstanceStatus.Available.equalsString(dbInstance.dbInstanceStatus());
    }

    public static boolean isDomainMembershipsJoined(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.domainMemberships()).orElse(Collections.emptyList())
                .stream()
                .allMatch(membership -> DomainMembershipStatus.Joined.equalsString(membership.status()) ||
                        DomainMembershipStatus.KerberosEnabled.equalsString(membership.status()));
    }

    public static boolean isVpcSecurityGroupsActive(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.vpcSecurityGroups()).orElse(Collections.emptyList())
                .stream()
                .allMatch(group -> VPCSecurityGroupStatus.Active.equalsString(group.status()));
    }

    public static boolean isNoPendingChanges(final DBInstance dbInstance) {
        final PendingModifiedValues pending = dbInstance.pendingModifiedValues();
        return (pending == null) || (pending.dbInstanceClass() == null &&
                pending.allocatedStorage() == null &&
                pending.automationMode() == null &&
                pending.backupRetentionPeriod() == null &&
                pending.dbInstanceIdentifier() == null &&
                pending.dbSubnetGroupName() == null &&
                pending.engine() == null &&
                pending.engineVersion() == null &&
                pending.iamDatabaseAuthenticationEnabled() == null &&
                pending.iops() == null &&
                pending.licenseModel() == null &&
                pending.masterUserPassword() == null &&
                pending.multiAZ() == null &&
                pending.pendingCloudwatchLogsExports() == null &&
                pending.port() == null &&
                CollectionUtils.isNullOrEmpty(pending.processorFeatures()) &&
                pending.resumeFullAutomationModeTime() == null &&
                pending.storageThroughput() == null &&
                pending.storageType() == null
        );
    }

    public static boolean isCaCertificateChangesApplied(final DBInstance dbInstance, final ResourceModel model) {
        final PendingModifiedValues pending = dbInstance.pendingModifiedValues();
        return pending == null ||
                pending.caCertificateIdentifier() == null ||
                BooleanUtils.isNotTrue(model.getCertificateRotationRestart());
    }

    public static boolean isDBParameterGroupNotApplying(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList())
                .stream()
                .noneMatch(group -> DBParameterGroupStatus.Applying.equalsString(group.parameterApplyStatus()));
    }

    public static boolean isReplicationComplete(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.statusInfos()).orElse(Collections.emptyList())
                .stream()
                .filter(statusInfo -> READ_REPLICA_STATUS_TYPE.equals(statusInfo.statusType()))
                .allMatch(statusInfo -> ReadReplicaStatus.Replicating.equalsString(statusInfo.status()));
    }

    public static boolean isDBClusterParameterGroupInSync(final ResourceModel model, final DBCluster dbCluster) {
        return Optional.ofNullable(dbCluster.dbClusterMembers()).orElse(Collections.emptyList())
                .stream()
                .filter(member -> model.getDBInstanceIdentifier().equalsIgnoreCase(member.dbInstanceIdentifier()))
                .anyMatch(member -> DBParameterGroupStatus.InSync.equalsString(member.dbClusterParameterGroupStatus()));
    }

    public static boolean isDBClusterMember(final ResourceModel model) {
        return StringUtils.isNotBlank(model.getDBClusterIdentifier());
    }

    public static boolean isRdsCustomOracleInstance(final ResourceModel model) {
        return RDS_CUSTOM_ORACLE_ENGINES.contains(model.getEngine());
    }

    public static boolean isOptionGroupInSync(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.optionGroupMemberships()).orElse(Collections.emptyList())
                .stream()
                .allMatch(optionGroup -> OptionGroupStatus.InSync.equalsString(optionGroup.status()));
    }

    public static boolean isDBParameterGroupInSync(final DBInstance dbInstance) {
        return Optional.ofNullable(dbInstance.dbParameterGroups()).orElse(Collections.emptyList())
                .stream()
                .allMatch(parameterGroup -> DBParameterGroupStatus.InSync.equalsString(parameterGroup.parameterApplyStatus()));
    }

    public static boolean isMasterUserSecretStabilized(final DBInstance instance) {
        if (instance.masterUserSecret() == null) {
            return true;
        }
        return SECRET_STATUS_ACTIVE.equalsIgnoreCase(instance.masterUserSecret().secretStatus());
    }

    public static boolean isDBInstanceStabilizedAfterMutate(
        final DBInstance dbInstance,
        final ResourceModel model,
        final CallbackContext context,
        final RequestLogger requestLogger
    ) {
        assertNoTerminalStatus(dbInstance);

        final boolean isDBInstanceStabilizedAfterMutateResult = isDBInstanceAvailable(dbInstance) &&
                isReplicationComplete(dbInstance) &&
                isDBParameterGroupNotApplying(dbInstance) &&
                isNoPendingChanges(dbInstance) &&
                isCaCertificateChangesApplied(dbInstance, model) &&
                isVpcSecurityGroupsActive(dbInstance) &&
                isDomainMembershipsJoined(dbInstance) &&
                isMasterUserSecretStabilized(dbInstance);

        requestLogger.log(String.format("isDBInstanceStabilizedAfterMutate: %b", isDBInstanceStabilizedAfterMutateResult),
                ImmutableMap.of("isDBInstanceAvailable", isDBInstanceAvailable(dbInstance),
                        "isReplicationComplete", isReplicationComplete(dbInstance),
                        "isDBParameterGroupNotApplying", isDBParameterGroupNotApplying(dbInstance),
                        "isNoPendingChanges", isNoPendingChanges(dbInstance),
                        "isCaCertificateChangesApplied", isCaCertificateChangesApplied(dbInstance, model),
                        "isVpcSecurityGroupsActive", isVpcSecurityGroupsActive(dbInstance),
                        "isDomainMembershipsJoined", isDomainMembershipsJoined(dbInstance),
                        "isMasterUserSecretStabilized", isMasterUserSecretStabilized(dbInstance)),
                ImmutableMap.of("Description", "isDBInstanceStabilizedAfterMutate method will be repeatedly" +
                        " called with a backoff mechanism after the modify call until it returns true. This" +
                        " process will continue until all included flags are true."));

        return isDBInstanceStabilizedAfterMutateResult;
    }

    public static boolean isDBInstanceStabilizedAfterReboot(
            final DBInstance dbInstance,
            final RequestLogger requestLogger
    ) {
        assertNoTerminalStatus(dbInstance);

        final boolean isDBClusterParameterGroupStabilized = true;
        return isDBInstanceStabilizedAfterReboot(dbInstance, isDBClusterParameterGroupStabilized, requestLogger);
    }

    public static boolean isDBInstanceStabilizedAfterReboot(
        final DBInstance dbInstance,
        final DBCluster dbCluster,
        final ResourceModel model,
        final RequestLogger requestLogger
    ) {
        assertNoTerminalStatus(dbInstance);

        final boolean isDBClusterParameterGroupStabilized = isDBClusterParameterGroupInSync(model, dbCluster);
        return isDBInstanceStabilizedAfterReboot(dbInstance, isDBClusterParameterGroupStabilized, requestLogger);
    }

   private static boolean isDBInstanceStabilizedAfterReboot(
        final DBInstance dbInstance,
        final boolean isDBClusterParameterGroupStabilized,
        final RequestLogger requestLogger
    ) {
        final boolean isDBInstanceStabilizedAfterReboot = isDBInstanceAvailable(dbInstance) &&
                isDBParameterGroupInSync(dbInstance) &&
                isOptionGroupInSync(dbInstance) &&
                isDBClusterParameterGroupStabilized;

        requestLogger.log(String.format("isDBInstanceStabilizedAfterReboot: %b", isDBInstanceStabilizedAfterReboot),
                ImmutableMap.of("isDBInstanceAvailable", isDBInstanceAvailable(dbInstance),
                        "isDBParameterGroupInSync", isDBParameterGroupInSync(dbInstance),
                        "isOptionGroupInSync", isOptionGroupInSync(dbInstance),
                        "isDBClusterParameterGroupStabilized", isDBClusterParameterGroupStabilized),
                ImmutableMap.of("Description", "isDBInstanceStabilizedAfterReboot method will be repeatedly" +
                        " called with a backoff mechanism after the reboot call until it returns true. This" +
                        " process will continue until all included flags are true."));

        return isDBInstanceStabilizedAfterReboot;
    }

    public static boolean isInstanceStabilizedAfterReplicationStart(
        final DBInstance dbInstance,
        final ResourceModel model
    ) {
        assertNoTerminalStatus(dbInstance);
        return isDBInstanceAvailable(dbInstance)
                && dbInstance.hasDbInstanceAutomatedBackupsReplications() &&
                !dbInstance.dbInstanceAutomatedBackupsReplications().isEmpty() &&
                model.getAutomaticBackupReplicationRegion()
                        .equalsIgnoreCase(
                                Arn.fromString(dbInstance.dbInstanceAutomatedBackupsReplications().get(0).dbInstanceAutomatedBackupsArn()).getRegion());
    }
}
