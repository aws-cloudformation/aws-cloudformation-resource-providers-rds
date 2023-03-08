package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_Success() {
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        final ResourceModel responseModel = response.getResourceModel();
        assertThat(responseModel.getDBClusterParameterGroupName()).isEqualTo(dbClusterParameterGroup.dbClusterParameterGroupName());
        assertThat(responseModel.getDescription()).isEqualTo(dbClusterParameterGroup.description());
        assertThat(responseModel.getFamily()).isEqualTo(dbClusterParameterGroup.dbParameterGroupFamily());
        assertThat(responseModel.getTags()).containsExactly(
                software.amazon.rds.dbclusterparametergroup.Tag.builder().key(TAG_KEY).value(TAG_VALUE).build()
        );

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_ReadParameters() {
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        when(rdsClient.describeEngineDefaultClusterParameters(any(DescribeEngineDefaultClusterParametersRequest.class)))
                .thenReturn(DescribeEngineDefaultClusterParametersResponse.builder()
                        .engineDefaults(
                                EngineDefaults.builder().parameters(
                                        Parameter.builder().parameterName(PARAM_1.parameterName()).parameterValue("default").build(),
                                        Parameter.builder().parameterName(PARAM_2.parameterName()).parameterValue("default").build()
                                ).build()
                        )
                        .build());

        when(rdsClient.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder()
                        .parameters(
                                Parameter.builder().parameterName(PARAM_1.parameterName()).parameterValue(PARAM_1.parameterValue()).build(),
                                Parameter.builder().parameterName(PARAM_2.parameterName()).parameterValue(PARAM_2.parameterValue()).build()
                        )
                        .build()
                );

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupName("group-name")
                        .dbClusterParameterGroupArn(ARN)
                        .build(),
                () -> RESOURCE_MODEL.toBuilder().parameters(null).build(),
                expectSuccess()
        );

        final ResourceModel model = response.getResourceModel();
        assertThat(model.getParameters()).isEqualTo(ImmutableMap.of(
                PARAM_1.parameterName(), PARAM_1.parameterValue(),
                PARAM_2.parameterName(), PARAM_2.parameterValue()
        ));

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEngineDefaultClusterParameters(any(DescribeEngineDefaultClusterParametersRequest.class));
        verify(rdsProxy.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_ReadParametersWithDefaultValues() {
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        when(rdsClient.describeEngineDefaultClusterParameters(any(DescribeEngineDefaultClusterParametersRequest.class)))
                .thenReturn(DescribeEngineDefaultClusterParametersResponse.builder()
                        .engineDefaults(
                                EngineDefaults.builder().parameters(
                                        Parameter.builder().parameterName(PARAM_1.parameterName()).parameterValue("default").build(),
                                        Parameter.builder().parameterName(PARAM_2.parameterName()).parameterValue("default").build()
                                ).build()
                        )
                        .build());

        when(rdsClient.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder()
                        .parameters(
                                Parameter.builder().parameterName(PARAM_1.parameterName()).parameterValue("default").build(),
                                Parameter.builder().parameterName(PARAM_2.parameterName()).parameterValue("default").build()
                        )
                        .build()
                );

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupName("group-name")
                        .dbClusterParameterGroupArn(ARN)
                        .build(),
                () -> RESOURCE_MODEL.toBuilder().parameters(null).build(),
                expectSuccess()
        );

        final ResourceModel model = response.getResourceModel();
        assertThat(model.getParameters()).isEqualTo(Collections.emptyMap());

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
        verify(rdsProxy.client(), times(1)).describeEngineDefaultClusterParameters(any(DescribeEngineDefaultClusterParametersRequest.class));
        verify(rdsProxy.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
