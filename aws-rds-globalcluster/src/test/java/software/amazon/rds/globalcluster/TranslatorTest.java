package software.amazon.rds.globalcluster;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import software.amazon.awssdk.services.rds.model.CreateGlobalClusterRequest;

public class TranslatorTest {

    @Test
    public void createGlobalClusterRequest_setsGlobalClusterIdentifier() {
        ResourceModel model = ResourceModel.builder().globalClusterIdentifier("foo").build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, null);

        assertThat(globalClusterRequest.globalClusterIdentifier()).isEqualTo("foo");
    }

    @Test
    public void createGlobalClusterRequest_setsEngine() {
        ResourceModel model = ResourceModel.builder().engine("aurora-mysql").build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, null);

        assertThat(globalClusterRequest.engine()).isEqualTo("aurora-mysql");
    }

    @Test
    public void createGlobalClusterRequest_setsEngineVersion() {
        ResourceModel model = ResourceModel.builder().engineVersion("5.7.mysql_aurora.2.07.2").build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, null);

        assertThat(globalClusterRequest.engineVersion()).isEqualTo("5.7.mysql_aurora.2.07.2");
    }

    @Test
    public void createGlobalClusterRequest_setsDeletionProtection() {
        ResourceModel model = ResourceModel.builder().deletionProtection(true).build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, null);

        assertThat(globalClusterRequest.deletionProtection()).isEqualTo(true);
    }

    @Test
    public void createGlobalClusterRequest_setsStorageEncryption() {
        ResourceModel model = ResourceModel.builder().storageEncrypted(true).build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, null);

        assertThat(globalClusterRequest.storageEncrypted()).isEqualTo(true);
    }

    @Test
    public void createGlobalClusterRequest_setsArn() {
        ResourceModel model = ResourceModel.builder().build();

        CreateGlobalClusterRequest globalClusterRequest = Translator.createGlobalClusterRequest(model, "arn:aws:rds::111111111111:global-cluster:cf-contract-test-global-cluster-0");

        assertThat(globalClusterRequest.sourceDBClusterIdentifier()).isEqualTo("arn:aws:rds::111111111111:global-cluster:cf-contract-test-global-cluster-0");
    }
}
