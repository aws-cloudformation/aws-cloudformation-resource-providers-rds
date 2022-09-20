package software.amazon.rds.dbclusterparametergroup;


import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.ServiceProvider;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermission;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final DBClusterParameterGroup DB_PARAMETER_GROUP_ACTIVE;
    protected static final String LOGICAL_RESOURCE_IDENTIFIER;
    protected static final Map<String, Object> PARAMS;


    protected static final String DESCRIPTION;
    protected static final String UPDATED_DESCRIPTION;
    protected static final String FAMILY;
    protected static final List<Tag> TAG_SET;
    protected static final Parameter PARAM_1, PARAM_2;
    protected static final DBClusterParameterGroup DB_CLUSTER_PARAMETER_GROUP;

    protected static Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofMillis(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        LOGICAL_RESOURCE_IDENTIFIER = "db-cluster-parameter-group";

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


        DESCRIPTION = "sample description";
        UPDATED_DESCRIPTION = "updated description";
        FAMILY = "default.aurora.5";
        TAG_SET = Lists.newArrayList(Tag.builder().key("key").value("value").build());

        DB_CLUSTER_PARAMETER_GROUP = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn("arn")
                .dbClusterParameterGroupName("name")
                .build();
    }

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJSONObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .enablePermission(new AccessPermission(ServiceProvider.RDS, "DescribeDBClustersPaginator"))
                .verify(TestUtils.getVerificationData(mock));
    }

    static Map<String, String> translateTagsToMap(final Collection<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));

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
}
