package software.amazon.rds.dbcluster;

import org.assertj.core.api.Assertions;
import org.json.JSONObject;
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
    public void testDrift_DBClusterParameterGroupName_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .dBClusterParameterGroupName("DBClusterParameterGroupName")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .dBClusterParameterGroupName("dbclusterparametergroupname")
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
    public void testDrift_SnapshotIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .snapshotIdentifier("SnapshotIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .snapshotIdentifier("snapshotidentifier")
                .build();

        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_SourceDBClusterIdentifier_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .sourceDBClusterIdentifier("SourceDBClusterIdentifier")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .sourceDBClusterIdentifier("sourcedbclusteridentifier")
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
    public void testDrift_PerformanceInsightsKmsKeyId_ExpandArn() {
        final ResourceModel input = ResourceModel.builder()
                .performanceInsightsKmsKeyId("test-kms-key-id")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .performanceInsightsKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/test-kms-key-id")
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
    public void testDrift_PreferredMaintenanceWindow_Lowercase() {
        final ResourceModel input = ResourceModel.builder()
                .preferredMaintenanceWindow("Sat:06:01-Sat:08:01")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .preferredMaintenanceWindow("sat:06:01-sat:08:01")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EnableHttpEndpoint_Serverless_Drifted() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engineMode("serverless")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engineMode("serverless")
                .build();
        Assertions.assertThatThrownBy(() -> {
            ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
        }).isInstanceOf(AssertionError.class);
    }

    @Test
    public void testDrift_EnableHttpEndpoint_Provisioned() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .engineMode("provisioned")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
                .engineMode("provisioned")
                .build();
        ResourceDriftTestHelper.assertResourceNotDrifted(input, output, resourceSchema);
    }

    @Test
    public void testDrift_EnableHttpEndpoint_NoEngineMode() {
        final ResourceModel input = ResourceModel.builder()
                .enableHttpEndpoint(true)
                .build();
        final ResourceModel output = ResourceModel.builder()
                .enableHttpEndpoint(false)
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
                .engine("aurora-MySQL")
                .build();
        final ResourceModel output = ResourceModel.builder()
                .engine("aurora-mysql")
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
