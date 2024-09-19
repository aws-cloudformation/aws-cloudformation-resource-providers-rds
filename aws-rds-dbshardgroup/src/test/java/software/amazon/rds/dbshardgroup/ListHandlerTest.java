package software.amazon.rds.dbshardgroup;

import java.util.Collections;
import java.time.Duration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbShardGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ListHandler handler;

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new ListHandler(TEST_HANDLER_CONFIG);
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

        when(proxyClient.client().describeDBShardGroups(any(DescribeDbShardGroupsRequest.class)))
                .thenReturn(DescribeDbShardGroupsResponse.builder()
                        .dbShardGroups(DB_SHARD_GROUP_AVAILABLE)
                        .marker("marker2")
                        .build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                requestBuilder,
                null,
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels().size()).isEqualTo(1);
        assertThat(response.getResourceModels().stream().anyMatch(model ->
                Translator.translateDbShardGroupFromSdk(DB_SHARD_GROUP_AVAILABLE, Collections.emptySet()).equals(model))).isTrue();
        assertThat(response.getNextToken()).isEqualTo("marker2");



        verify(proxyClient.client(), times(1)).describeDBShardGroups(any(DescribeDbShardGroupsRequest.class));
        verify(proxyClient.client(), times(1))
                .listTagsForResource(
                        ArgumentMatchers.<ListTagsForResourceRequest>argThat(
                                req -> DB_SHARD_GROUP_ARN.equalsIgnoreCase(req.resourceName()))
                );
    }
}
