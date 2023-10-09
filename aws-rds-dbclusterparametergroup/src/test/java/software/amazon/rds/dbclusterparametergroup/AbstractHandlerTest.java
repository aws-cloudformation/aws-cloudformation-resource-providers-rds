package software.amazon.rds.dbclusterparametergroup;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.RequestLogger;
import software.amazon.rds.test.common.core.AbstractTestBase;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.MethodCallExpectation;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractHandlerTest extends AbstractTestBase<DBClusterParameterGroup, ResourceModel, CallbackContext> {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;
    protected static final RequestLogger EMPTY_REQUEST_LOGGER;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final DBClusterParameterGroup DB_PARAMETER_GROUP_ACTIVE;
    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "db-cluster-parameter-group";
    protected static final Map<String, Object> PARAMS;

    protected static final DBClusterParameterGroup DB_CLUSTER_PARAMETER_GROUP;
    protected static final List<Tag> TAG_SET;
    protected static final List<Tag> TAG_SET_ALTER;

    protected static final Parameter PARAM_1, PARAM_2;

    protected static final String ARN = "arn";
    protected static final String DESCRIPTION = "sample description";
    protected static final String FAMILY = "default.aurora.5";
    protected static final String TAG_KEY = "key";
    protected static final String TAG_VALUE = "value";
    protected static final String TAG_VALUE_ALTER = "value-alter";

    protected static final String UPDATED_DESCRIPTION = "updated description";

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();
        EMPTY_REQUEST_LOGGER = new RequestLogger(logger, ResourceHandlerRequest.builder().build(), null);

        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");

        PARAM_1 = Parameter.builder()
                .parameterName("param")
                .parameterValue("system_value")
                .isModifiable(true)
                .applyType("static")
                .build();

        PARAM_2 = Parameter.builder()
                .parameterName("param2")
                .parameterValue("system_value")
                .isModifiable(true)
                .applyType("dynamic")
                .build();

        RESOURCE_MODEL = ResourceModel.builder()
                .dBClusterParameterGroupName("testDBClusterParameterGroup")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(PARAMS)
                .build();

        DB_PARAMETER_GROUP_ACTIVE = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn("arn")
                .dbClusterParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .dbParameterGroupFamily("testFamily")
                .build();

        TAG_SET = Lists.newArrayList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build());
        TAG_SET_ALTER = Lists.newArrayList(Tag.builder().key(TAG_KEY).value(TAG_VALUE_ALTER).build());

        DB_CLUSTER_PARAMETER_GROUP = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName("name")
                .build();
    }

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJSONObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    static Map<String, String> translateTagsToMap(final Collection<Tag> tags) {
        return tags.stream().collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    static ProxyClient<RdsClient> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final RdsClient rdsClient
    ) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            ResponseT
            injectCredentialsAndInvokeV2(RequestT request,
                                         Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request,
                                              Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request,
                                                 Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT requestT,
                                                    Function<RequestT, ResponseInputStream<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT requestT,
                                              Function<RequestT, ResponseBytes<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    protected List<Parameter> translateParamMapToCollection(final Map<String, Object> params) {
        return params.entrySet().stream().map(entry -> Parameter.builder()
                        .parameterName(entry.getKey())
                        .parameterValue((String) entry.getValue())
                        .applyType(ParameterType.Dynamic.toString())
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    protected String getLogicalResourceIdentifier() {
        return LOGICAL_RESOURCE_IDENTIFIER;
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> invokeHandleRequest(
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext context
    ) {
        return getHandler().handleRequest(getProxy(), request, context, getRdsProxy(), EMPTY_REQUEST_LOGGER);
    }

    @Override
    protected void expectResourceSupply(final Supplier<DBClusterParameterGroup> supplier) {
        expectDescribeDBClusterParameterGroupCall().setup().then(res -> DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(supplier.get())
                .build());
    }

    protected MethodCallExpectation<DescribeDbClusterParameterGroupsRequest, DescribeDbClusterParameterGroupsResponse> expectDescribeDBClusterParameterGroupCall() {
        return new MethodCallExpectation<DescribeDbClusterParameterGroupsRequest, DescribeDbClusterParameterGroupsResponse>() {
            @Override
            public OngoingStubbing<DescribeDbClusterParameterGroupsResponse> setup() {
                return when(getRdsProxy().client().describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class)));
            }

            @Override
            public ArgumentCaptor<DescribeDbClusterParameterGroupsRequest> verify() {
                ArgumentCaptor<DescribeDbClusterParameterGroupsRequest> captor = ArgumentCaptor.forClass(DescribeDbClusterParameterGroupsRequest.class);
                Mockito.verify(getRdsProxy().client(), times(1)).describeDBClusterParameterGroups(captor.capture());
                return captor;
            }
        };
    }

    protected abstract BaseHandlerStd getHandler();

    protected abstract AmazonWebServicesClientProxy getProxy();

    protected abstract ProxyClient<RdsClient> getRdsProxy();

    public abstract HandlerName getHandlerName();
}
