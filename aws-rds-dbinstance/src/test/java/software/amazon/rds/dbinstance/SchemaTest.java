package software.amazon.rds.dbinstance;

import org.json.JSONObject;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.rds.test.common.schema.ResourceDriftTestHelper;

public class SchemaTest {

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJsonObject();

    @Test
    public void testDrift_DBClusterIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterIdentifier("DBClusterIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterIdentifier("dbclusteridentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBClusterSnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterIdentifier("DBClusterSnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterIdentifier("dbclustersnapshotidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBInstanceIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBInstanceIdentifier("DBInstanceIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBInstanceIdentifier("dbinstanceidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBParameterGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBParameterGroupName("DBParameterGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBParameterGroupName("dbparametergroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBSubnetGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBSubnetGroupName("DBSubnetGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBSubnetGroupName("dbsubnetgroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBSnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBSnapshotIdentifier("DBSnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBSnapshotIdentifier("dbsnapshotidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_OptionGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .optionGroupName("OptionGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .optionGroupName("optiongroupname")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_SourceDBInstanceAutomatedBackupsArn_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .optionGroupName("SourceDBInstanceAutomatedBackupsArn")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .optionGroupName("sourcedbinstanceautomatedbackupsarn")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_SourceDBInstanceIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .sourceDBInstanceIdentifier("SourceDBInstanceIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .sourceDBInstanceIdentifier("sourcedbinstanceidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .kmsKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_PerformanceInsightsKMSKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .performanceInsightsKMSKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .performanceInsightsKMSKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_MasterUserSecret_KmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .masterUserSecret(MasterUserSecret.builder()
                        .kmsKeyId("test-kms-key-id")
                        .build())
                .build();
        final ResourceModel output = ResourceModel.builder()
                .masterUserSecret(MasterUserSecret.builder()
                        .kmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
                        .build())
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MajorVersionOnly() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MajorAndMinorVersion() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.6.40")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EngineVersion_MinorMismatch_ShouldDrift() {
        final ResourceModel input = ResourceModel.builder()
                .engineVersion("5.6")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engineVersion("5.7.40")
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void testDrift_Engine_CaseMismatch_ShouldNotDrift() {
        final ResourceModel input = ResourceModel.builder()
                .engine("Postgres")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engine("postgres")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBName("DBName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBName("dbname")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_DBName_Oracle_Uppercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBName("Oracle_DB")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBName("ORACLE_DB")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_PreferredMaintenanceWindow_Uppercase() {
        final ResourceModel input = ResourceModel.builder()
                .preferredMaintenanceWindow("Sun:19:00-Sun:23:00")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .preferredMaintenanceWindow("sun:19:00-sun:23:00")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_NetworkType_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .networkType("IO1")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .networkType("io1")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }
}
