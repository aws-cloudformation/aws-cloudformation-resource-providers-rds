package software.amazon.rds.dbcluster;

import java.time.Instant;
import java.util.Collections;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.commons.collections.CollectionUtils;

import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.MasterUserSecret;
import software.amazon.awssdk.services.rds.model.WriteForwardingStatus;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.common.request.RequestValidationException;
import software.amazon.rds.common.request.ValidatedRequest;

class BaseHandlerStdTest {

    static class TestBaseHandlerStd extends BaseHandlerStd {

        public TestBaseHandlerStd(HandlerConfig config) {
            super(config);
        }

        @Override
        protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
                final AmazonWebServicesClientProxy proxy,
                final ValidatedRequest<ResourceModel> request,
                final CallbackContext callbackContext,
                final ProxyClient<RdsClient> rdsProxyClient,
                final ProxyClient<Ec2Client> ec2ProxyClient,
                final RequestLogger logger
        ) {
            return null;
        }
    }

    private TestBaseHandlerStd handler;

    @BeforeEach
    public void setUp() {
        handler = new TestBaseHandlerStd(null);
    }

    @Test
    void isMasterUserSecretStabilized_masterUserSecretIsNull() {
        Assertions.assertThat(BaseHandlerStd.isMasterUserSecretStabilized(
                DBCluster.builder()
                        .build()
        )).isTrue();
    }

    @Test
    void isMasterUserSecretStabilized_masterUserSecretStatusActive() {
        Assertions.assertThat(BaseHandlerStd.isMasterUserSecretStabilized(
                DBCluster.builder()
                        .masterUserSecret(MasterUserSecret.builder()
                                .secretStatus("Active")
                                .build())
                        .build()
        )).isTrue();
    }

    @Test
    void isMasterUserSecretStabilized_masterUserSecretStatusCreatingWithEmptyMembers() {
        DBCluster dbCluster = DBCluster.builder()
                .masterUserSecret(MasterUserSecret.builder()
                        .secretStatus("Creating")
                        .build())
                .build();
        Assertions.assertThat(CollectionUtils.isEmpty(dbCluster.dbClusterMembers()));
        Assertions.assertThat(BaseHandlerStd.isMasterUserSecretStabilized(dbCluster)).isTrue();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingNotRequested() {
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .build()
        )).isTrue();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingRequested() {
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .globalWriteForwardingRequested(true)
                        .build()
        )).isTrue();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingEnabled() {
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .globalWriteForwardingRequested(true)
                        .globalWriteForwardingStatus(WriteForwardingStatus.ENABLED)
                        .build()
        )).isTrue();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingEnabling() {
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .globalWriteForwardingRequested(true)
                        .globalWriteForwardingStatus(WriteForwardingStatus.ENABLING)
                        .build()
        )).isFalse();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingDisabled() {
        // GlobalWriteForwarding status will not enable until a replica is requested by customer
        // This prevents customers from creating a stack with only primary and setting the property
        // As WS does not validate this parameter the stack will wait on stabilization until timeout.
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .globalWriteForwardingRequested(true)
                        .globalWriteForwardingStatus(WriteForwardingStatus.DISABLED)
                        .build()
        )).isTrue();
    }

    @Test
    void isGlobalWriteForwardingStabilized_globalWriteForwardingDisabling() {
        Assertions.assertThat(BaseHandlerStd.isGlobalWriteForwardingStabilized(
                DBCluster.builder()
                        .globalWriteForwardingRequested(true)
                        .globalWriteForwardingStatus(WriteForwardingStatus.DISABLING)
                        .build()
        )).isFalse();
    }


    @Test
    void validateRequest_BlankRegionIsAccepted() {
        final ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
        request.setDesiredResourceState(ResourceModel.builder()
                .sourceRegion("")
                .build());
        Assertions.assertThatCode(() -> {
            handler.validateRequest(request);
        }).doesNotThrowAnyException();
    }

    @Test
    void validateRequest_UnknownRegionIsRejected() {
        final ResourceHandlerRequest<ResourceModel> request = new ResourceHandlerRequest<>();
        request.setDesiredResourceState(ResourceModel.builder()
                .sourceRegion("foo-bar-baz")
                .build());
        Assertions.assertThatExceptionOfType(RequestValidationException.class).isThrownBy(() -> {
            handler.validateRequest(request);
        });
    }
}
