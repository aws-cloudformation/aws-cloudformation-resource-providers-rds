package software.amazon.rds.dbinstance;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum LogParameters {
    AllocatedStorage("allocatedStorage"),
    AllowMajorVersionUpgrade("allowMajorVersionUpgrade"),
    AssociatedRoles("associatedRoles"),
    AutoMinorVersionUpgrade("autoMinorVersionUpgrade"),
    AvailabilityZone("availabilityZone"),
    BackupRetentionPeriod("backupRetentionPeriod"),
    CACertificateIdentifier("cACertificateIdentifier"),
    CharacterSetName("characterSetName"),
    CopyTagsToSnapshot("copyTagsToSnapshot"),
    DBClusterIdentifier("dBClusterIdentifier"),
    DBInstanceClass("dBInstanceClass"),
    DBInstanceIdentifier("dBInstanceIdentifier"),
    DBName("dBName"),
    DBParameterGroupName("dBParameterGroupName"),
    DBSecurityGroups("dBSecurityGroups"),
    DBSnapshotIdentifier("dBSnapshotIdentifier"),
    DBSubnetGroupName("dBSubnetGroupName"),
    DeleteAutomatedBackups("deleteAutomatedBackups"),
    DeletionProtection("deletionProtection"),
    Domain("domain"),
    DomainIAMRoleName("domainIAMRoleName"),
    EnableCloudwatchLogsExports("enableCloudwatchLogsExports"),
    EnableIAMDatabaseAuthentication("enableIAMDatabaseAuthentication"),
    EnablePerformanceInsights("enablePerformanceInsights"),
    Endpoint("endpoint"),
    Engine("engine"),
    EngineVersion("engineVersion"),
    Iops("iops"),
    KmsKeyId("kmsKeyId"),
    LicenseModel("licenseModel"),
    MasterUsername("masterUsername"),
    MasterUserPassword("masterUserPassword"),
    MaxAllocatedStorage("maxAllocatedStorage"),
    MonitoringInterval("monitoringInterval"),
    MonitoringRoleArn("monitoringRoleArn"),
    MultiAZ("multiAZ"),
    OptionGroupName("optionGroupName"),
    PerformanceInsightsKMSKeyId("performanceInsightsKMSKeyId"),
    PerformanceInsightsRetentionPeriod("performanceInsightsRetentionPeriod"),
    Port("port"),
    PreferredBackupWindow("preferredBackupWindow"),
    PreferredMaintenanceWindow("preferredMaintenanceWindow"),
    ProcessorFeatures("processorFeatures"),
    PromotionTier("promotionTier"),
    PubliclyAccessible("publiclyAccessible"),
    SourceDBInstanceIdentifier("sourceDBInstanceIdentifier"),
    SourceRegion("sourceRegion"),
    StorageEncrypted("storageEncrypted"),
    StorageType("storageType"),
    Tags("tags"),
    TdeCredentialArn("tdeCredentialArn"),
    TdeCredentialPassword("tdeCredentialPassword"),
    Timezone("timezone"),
    UseDefaultProcessorFeatures("useDefaultProcessorFeatures"),
    VPCSecurityGroups("vPCSecurityGroups");

    private String value;

    @Override
    public String toString() {
        return value;
    }

    public boolean equals(final String cmp) {
        return value.equals(cmp);
    }
}
