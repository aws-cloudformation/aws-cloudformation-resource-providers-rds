package software.amazon.rds.dbparametergroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.mockito.internal.util.collections.Sets;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.RequestLogger;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final LoggerProxy logger;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESET_RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_WITH_TAGS;
    protected static final DBParameterGroup DB_PARAMETER_GROUP_ACTIVE;
    protected static final Set<Tag> TAG_SET;
    protected static final String LOGICAL_RESOURCE_IDENTIFIER;
    protected static final Map<String, Object> PARAMS;
    protected static final Map<String, Object> RESET_PARAMS;
    protected static final RequestLogger EMPTY_REQUEST_LOGGER;
    protected static final List<Parameter> MANY_CURRENT_PARAMETERS_SORTED;
    protected static final List<Parameter> MANY_DEFAULT_PARAMETERS_SORTED;

    private static List<Parameter> helpBuildManyParametersConstant(String parameterValue) {
        String[] sortedParameterNames = new String[] {
                "binlog_format", "character_set_server", "collation_server", "event_scheduler",
                "innodb_autoinc_lock_mode", "innodb_buffer_pool_instances", "innodb_lock_wait_timeout",
                "innodb_log_file_size", "innodb_print_all_deadlocks", "innodb_read_io_threads", "innodb_write_io_threads",
                "lock_wait_timeout", "log_bin_trust_function_creators", "lower_case_table_names" ,"max_allowed_packet",
                "net_write_timeout", "performance_schema", "query_cache_size", "query_cache_type", "read_only",
                "skip_name_resolve", "slave_parallel_type", "slow_query_log", "sql_mode", "time_zone", "tx_isolation"
        };

        List<Parameter> parameters =  new ArrayList<>();

        for (String parameterName : sortedParameterNames) {
            parameters.add(Parameter.builder()
                .parameterName(parameterName)
                .parameterValue(parameterValue)
                .applyType("dynamic")
                .isModifiable(true)
                .applyMethod("immediate")
                    .build());
        }
        return parameters;
    }



    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        logger = new LoggerProxy();
        EMPTY_REQUEST_LOGGER = new RequestLogger(logger, ResourceHandlerRequest.builder().build(), null);
        LOGICAL_RESOURCE_IDENTIFIER = "db-parameter-group";

        MANY_CURRENT_PARAMETERS_SORTED = helpBuildManyParametersConstant("current_value");
        MANY_DEFAULT_PARAMETERS_SORTED = helpBuildManyParametersConstant("default_value");

        PARAMS = new HashMap<>();
        PARAMS.put("param1", "value");
        PARAMS.put("param2", "value");

        RESET_PARAMS = new HashMap<>(PARAMS);
        RESET_PARAMS.remove("param1");

        RESOURCE_MODEL = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(PARAMS)
                .build();

        RESET_RESOURCE_MODEL = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(RESET_PARAMS)
                .build();

        RESOURCE_MODEL_WITH_TAGS = ResourceModel.builder()
                .dBParameterGroupName("testDBParameterGroup2")
                .description("test DB Parameter group description")
                .family("testFamily2")
                .tags(Collections.singletonList(Tag.builder().key("Key").value("Value").build()))
                .parameters(PARAMS)
                .build();

        DB_PARAMETER_GROUP_ACTIVE = DBParameterGroup.builder()
                .dbParameterGroupArn("arn")
                .dbParameterGroupName("testDBParameterGroup")
                .description("test DB Parameter group description")
                .dbParameterGroupFamily("testFamily")
                .build();

        TAG_SET = Sets.newSet(Tag.builder().key("key").value("value").build());
    }

    static Map<String, String> translateTagsToMap(final Set<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));

    }

    static String getClientRequestToken() {
        return UUID.randomUUID().toString();
    }

    static ProxyClient<RdsClient> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final RdsClient rdsClient) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    void mockDescribeDbParametersResponse(ProxyClient<RdsClient> proxyClient,
                                          String firstParamApplyType,
                                          String secondParamApplyType,
                                          boolean isModifiable,
                                          boolean mockDescribeParameters,
                                          boolean isPaginated) {
        Parameter param1 = Parameter.builder()
                .parameterName("param1")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(firstParamApplyType)
                .applyMethod("pending-reboot")
                .build();
        Parameter defaultParam1 = param1.toBuilder()
                .parameterValue("default_value")
                .applyMethod("")
                .build();
        Parameter param2 = Parameter.builder()
                .parameterName("param2")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();
        //Adding parameter to current parameters and not adding it to default. Expected behaviour is to ignore it
        Parameter param3 = Parameter.builder()
                .parameterName("param3")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();
        //Adding parameter to default parameters and not adding it to current. Expected behaviour is to ignore it
        Parameter param4 = Parameter.builder()
                .parameterName("param4")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();


        final DescribeEngineDefaultParametersResponse describeEngineDefaultParametersResponse = DescribeEngineDefaultParametersResponse.builder()
                .engineDefaults(EngineDefaults.builder()
                        .parameters(defaultParam1, param2, param4)
                        .build()
                ).build();

        if (!isPaginated) {
            when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class))).thenReturn(describeEngineDefaultParametersResponse);
        } else {

            final DescribeEngineDefaultParametersResponse firstPage = DescribeEngineDefaultParametersResponse.builder()
                    .engineDefaults(EngineDefaults.builder()
                            .parameters(defaultParam1, param2, param4)
                            .marker("marker")
                            .build()
                    ).build();

            when(proxyClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class)))
                    .thenReturn(firstPage)
                    .thenReturn(describeEngineDefaultParametersResponse);

        }
        if (!mockDescribeParameters)
            return;

        final DescribeDbParametersResponse describeDbParametersResponse = DescribeDbParametersResponse.builder().marker(null)
                .parameters(param1, param2, param3).build();
        if (!isPaginated) {
            when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class))).thenReturn(describeDbParametersResponse);
        } else {
            final DescribeDbParametersResponse firstPage = DescribeDbParametersResponse.builder().marker("marker")
                    .parameters(param1, param2, param3).build();

            when(proxyClient.client().describeDBParameters(any(DescribeDbParametersRequest.class)))
                    .thenReturn(firstPage)
                    .thenReturn(describeDbParametersResponse);
        }
    }
}
