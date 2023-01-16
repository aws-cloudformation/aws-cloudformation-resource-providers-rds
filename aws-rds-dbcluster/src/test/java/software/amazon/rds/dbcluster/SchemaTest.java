package software.amazon.rds.dbcluster;

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
}
