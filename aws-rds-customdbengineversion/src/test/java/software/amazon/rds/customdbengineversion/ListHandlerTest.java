package software.amazon.rds.customdbengineversion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {
    public static final String ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION = "arn:123456789123:ue-east-1:rds:enginveversion:";
    final String DB_ENGINE_VERSION = "db-engine-version";
    final String ENGINE = "engine";

    final String DESCRIBE_DB_ENGINE_VERSIONS_MARKER = "test-describe-db-engine-versions-marker";
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;
    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;
    @Mock
    private RdsClient rdsClient;
    @Getter
    private ListHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.LIST;
    }

    @BeforeEach
    public void setup() {
        handler = new ListHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DescribeDbEngineVersionsResponse describeDBEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(Collections.singletonList(
                        DBEngineVersion.builder()
                                .engineVersion(DB_ENGINE_VERSION)
                                .engine(ENGINE)
                                .dbEngineVersionArn(ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION + "test-oracle-ee")
                                .tagList(Lists.newArrayList())
                                .build()
                ))
                .marker(DESCRIBE_DB_ENGINE_VERSIONS_MARKER)
                .build();
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)))
                .thenReturn(describeDBEngineVersionsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        final ResourceModel expectedModel = ResourceModel.builder()
                .engineVersion(DB_ENGINE_VERSION)
                .engine(ENGINE)
                .dBEngineVersionArn(ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION + "test-oracle-ee")
                .tags(Lists.newArrayList())
                .build();

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);

        verify(rdsProxy.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
    }

    @Test
    public void handleRequest_retrieveOnlyCustomEngineVersion() {
        final DescribeDbEngineVersionsResponse describeDBEngineVersionsResponse = DescribeDbEngineVersionsResponse.builder()
                .dbEngineVersions(ImmutableList.of(
                        DBEngineVersion.builder()
                                .engineVersion("92.1")
                                .engine("custom-oracle-ee")
                                .dbEngineVersionArn(ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION + "test-oracle-ee")
                                .tagList(Lists.newArrayList())
                                .build(),
                        DBEngineVersion.builder()
                                .engineVersion(DB_ENGINE_VERSION)
                                .engine("custom-oracle-ee-cdb")
                                .dbEngineVersionArn(ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION + "test-oracle-ee-cdb")
                                .tagList(Lists.newArrayList())
                                .build(),
                        DBEngineVersion.builder()
                                .engineVersion(DB_ENGINE_VERSION)
                                .engine("custom-sqlserver-se")
                                .dbEngineVersionArn(ARN_123456789123_UE_EAST_1_RDS_ENGINVEVERSION + "test-sqlserver-se")
                                .tagList(Lists.newArrayList())
                                .build(),
                        DBEngineVersion.builder()
                                .engineVersion(DB_ENGINE_VERSION)
                                .engine("custom-sqlserver-web")
                                .tagList(Lists.newArrayList())
                                .build()
                ))
                .marker(DESCRIBE_DB_ENGINE_VERSIONS_MARKER)
                .build();
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)))
                .thenReturn(describeDBEngineVersionsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels().size()).isEqualTo(3);

        verify(rdsProxy.client()).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
    }
}
