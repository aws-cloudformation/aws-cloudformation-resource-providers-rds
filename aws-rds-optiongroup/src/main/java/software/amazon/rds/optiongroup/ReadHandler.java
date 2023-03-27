package software.amazon.rds.optiongroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.Option;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.awssdk.services.rds.model.OptionSetting;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class ReadHandler extends BaseHandlerStd {

    public ReadHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public ReadHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger
    ) {
        return proxy.initiate("rds::read-option-group", proxyClient, request.getDesiredResourceState(), callbackContext)
                .translateToServiceRequest(Translator::describeOptionGroupsRequest)
                .backoffDelay(config.getBackoff())
                .makeServiceCall((describeRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                        describeRequest,
                        proxyInvocation.client()::describeOptionGroups
                ))
                .handleError((describeRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                        ProgressEvent.progress(resourceModel, ctx),
                        exception,
                        DEFAULT_OPTION_GROUP_ERROR_RULE_SET
                ))
                .done((describeRequest, describeResponse, proxyInvocation, model, context) -> {
                    final OptionGroup optionGroup = describeResponse.optionGroupsList().stream().findFirst().get();
                    final List<Option> overriddenConfigurations = getOverriddenOptionConfigurations(optionGroup);

                    final List<Tag> tags = listTags(proxyInvocation, optionGroup.optionGroupArn());
                    return ProgressEvent.success(
                            ResourceModel.builder()
                                    .optionGroupName(optionGroup.optionGroupName())
                                    .engineName(optionGroup.engineName())
                                    .majorEngineVersion(optionGroup.majorEngineVersion())
                                    .optionGroupDescription(optionGroup.optionGroupDescription())
                                    .optionConfigurations(Translator.translateOptionConfigurationsFromSdk(overriddenConfigurations))
                                    .tags(tags)
                                    .build(),
                            context
                    );
                });
    }

    private List<Option> getOverriddenOptionConfigurations(final OptionGroup optionGroup) {
        final List<Option> overriddenOptions = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(optionGroup.options())) {
            for (final Option option : optionGroup.options()) {
                final List<OptionSetting> optionSettings = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(option.optionSettings())) {
                    for (final OptionSetting optionSetting : option.optionSettings()) {
                        final String defaultValue = optionSetting.defaultValue();
                        if (!Objects.equals(defaultValue, optionSetting.value())) {
                            optionSettings.add(optionSetting);
                        }
                    }
                }
                overriddenOptions.add(option.toBuilder().optionSettings(optionSettings).build());
            }
        }
        return overriddenOptions;
    }
}
