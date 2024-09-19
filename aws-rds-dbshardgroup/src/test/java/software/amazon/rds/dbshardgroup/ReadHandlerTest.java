package software.amazon.rds.dbshardgroup;

import java.time.Duration;

import lombok.Getter;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(TEST_HANDLER_CONFIG);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        expectServiceInvocation = false;

        ResourceHandlerRequest.ResourceHandlerRequestBuilder<ResourceModel> requestBuilder = ResourceHandlerRequest.builder();
        requestBuilder.region(REGION);
        requestBuilder.awsAccountId(ACCOUNT_ID);
        requestBuilder.awsPartition(PARTITION);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                requestBuilder,
                () -> DB_SHARD_GROUP_AVAILABLE,
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1))
                .describeDBShardGroups(
                        ArgumentMatchers.<DescribeDbShardGroupsRequest>argThat(
                                req -> DB_SHARD_GROUP_IDENTIFIER.equals(req.dbShardGroupIdentifier()))
                );
        verify(proxyClient.client(), times(1))
                .listTagsForResource(
                        ArgumentMatchers.<ListTagsForResourceRequest>argThat(
                                req -> DB_SHARD_GROUP_ARN.equalsIgnoreCase(req.resourceName()))
                );
    }
}
