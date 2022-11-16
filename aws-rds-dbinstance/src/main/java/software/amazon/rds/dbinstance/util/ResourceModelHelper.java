package software.amazon.rds.dbinstance.util;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.amazonaws.util.StringUtils;
import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.utils.CollectionUtils;
import software.amazon.rds.dbinstance.ResourceModel;

public final class ResourceModelHelper {
    private final static String STORAGE_TYPE_IO1 = "io1";

    public static final List<String> RDS_CUSTOM_ORACLE_ENGINES = ImmutableList.of(
            "custom-oracle-ee",
            "custom-oracle-ee-cdb"
    );

    public static final List<String> RDS_CUSTOM_SQL_SERVER_ENGINES = ImmutableList.of(
            "custom-sqlserver-ee",
            "custom-sqlserver-se",
            "custom-sqlserver-web"
    );

    public static final List<String> SQLSERVER_ENGINES_WITH_MIRRORING = Arrays.asList(
            "sqlserver-ee",
            "sqlserver-se"
    );

    public static boolean shouldUpdateAfterCreate(final ResourceModel model) {
        return (isReadReplica(model) || isRestoreFromSnapshot(model) || isCertificateAuthorityApplied(model)) &&
                (
                        !CollectionUtils.isNullOrEmpty(model.getDBSecurityGroups()) ||
                                StringUtils.hasValue(model.getAllocatedStorage()) ||
                                StringUtils.hasValue(model.getCACertificateIdentifier()) ||
                                StringUtils.hasValue(model.getDBParameterGroupName()) ||
                                StringUtils.hasValue(model.getEngineVersion()) ||
                                StringUtils.hasValue(model.getMasterUserPassword()) ||
                                StringUtils.hasValue(model.getPreferredBackupWindow()) ||
                                StringUtils.hasValue(model.getPreferredMaintenanceWindow()) ||
                                Optional.ofNullable(model.getBackupRetentionPeriod()).orElse(0) > 0 ||
                                Optional.ofNullable(model.getMaxAllocatedStorage()).orElse(0) > 0
                );
    }

    private static boolean isCertificateAuthorityApplied(final ResourceModel model) {
        return StringUtils.hasValue(model.getCACertificateIdentifier());
    }

    public static boolean isReadReplica(final ResourceModel model) {
        return StringUtils.hasValue(model.getSourceDBInstanceIdentifier());
    }

    public static boolean isRestoreFromSnapshot(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBSnapshotIdentifier());
    }

    public static boolean shouldSetStorageTypeOnRestoreFromSnapshot(final ResourceModel model) {
        return !Objects.equals(model.getStorageType(), STORAGE_TYPE_IO1) && shouldUpdateAfterCreate(model);
    }

    public static boolean isDBClusterMember(final ResourceModel model) {
        return StringUtils.hasValue(model.getDBClusterIdentifier());
    }

    public static boolean isRdsCustomFamily(final ResourceModel model) {
        return isRdsCustomOracleInstance(model) || isRdsCustomSQLServer(model);
    }

    public static boolean isRdsCustomSQLServer(final ResourceModel model) {
        return RDS_CUSTOM_SQL_SERVER_ENGINES.contains(model.getEngine());
    }

    public static boolean isRdsCustomOracleInstance(final ResourceModel model) {
        return RDS_CUSTOM_ORACLE_ENGINES.contains(model.getEngine());
    }
}
