package software.amazon.rds.dbsecuritygroup;

import java.util.List;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandlerStd {

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        return proxy.initiate("rds::read-db-security-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeDBSecurityGroupsRequest)
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeDBSecurityGroups
                ))
                .handleError((describeRequest, exception, client, model, context) -> handleException(
                        ProgressEvent.progress(model, context),
                        exception
                ))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final DBSecurityGroup dbSecurityGroup = describeResponse.dbSecurityGroups().stream().findFirst().get();
                    final List<Tag> tags = listTags(proxyInvocation, dbSecurityGroup.dbSecurityGroupArn());
                    return ProgressEvent.success(
                            Translator.translateDBSecurityGroupFromSdk(
                                    dbSecurityGroup,
                                    tags
                            ),
                            context
                    );
                });
    }
}
