package software.amazon.rds.customdbengineversion;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

public class ListHandler extends BaseHandlerStd {

    final static Collection<String> CUSTOM_ENGINES = ImmutableList.of(
            "custom-oracle-ee",
            "custom-oracle-ee-cdb",
            "custom-sqlserver-ee",
            "custom-sqlserver-se",
            "custom-sqlserver-web"
    );

    public ListHandler() {
        this(CUSTOM_ENGINE_VERSION_HANDLER_CONFIG_10H);
    }

    public ListHandler(HandlerConfig config) {
        super(config);
    }

    @Override
    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                          final ResourceHandlerRequest<ResourceModel> request,
                                                                          final CallbackContext callbackContext,
                                                                          final ProxyClient<RdsClient> proxyClient) {
        return proxy.initiate("rds::list-db-engine-versions", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(resourceModel -> Translator.describeDbEngineVersionsRequest(resourceModel, CUSTOM_ENGINES, request.getNextToken()))
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBEngineVersions
                )).done((describeRequest, describeResponse, proxyInvocation, resourceModel, context) -> {
                    final List<ResourceModel> resourceModels = Translator.translateFromSdk(describeResponse.dbEngineVersions().stream()
                            .filter(dbEngineVersion -> StringUtils.isNotBlank(dbEngineVersion.dbEngineVersionArn())));
                    return ProgressEvent.<ResourceModel, CallbackContext>builder()
                            .callbackContext(callbackContext)
                            .resourceModels(resourceModels)
                            .nextToken(describeResponse.marker())
                            .status(OperationStatus.SUCCESS)
                            .build();
                });
    }
}
