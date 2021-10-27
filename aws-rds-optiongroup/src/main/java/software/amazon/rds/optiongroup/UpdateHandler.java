package software.amazon.rds.optiongroup;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.Commons;
import software.amazon.rds.common.handler.HandlerConfig;

public class UpdateHandler extends BaseHandlerStd {

    private static final String APEX_OPTION_NAME = "APEX";

    public UpdateHandler() {
        this(HandlerConfig.builder()
                .backoff(BACKOFF_DELAY)
                .build());
    }

    public UpdateHandler(final HandlerConfig config) {
        super(config);
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel previousModel = request.getPreviousResourceState();
        final ResourceModel desiredModel = request.getDesiredResourceState();

        final Collection<OptionConfiguration> previousOptions = Optional
                .ofNullable(previousModel.getOptionConfigurations())
                .orElse(Collections.emptyList());
        final Collection<OptionConfiguration> desiredOptions = Optional
                .ofNullable(desiredModel.getOptionConfigurations())
                .orElse(Collections.emptyList());

        final Collection<OptionConfiguration> optionsToInclude = getOptionsToInclude(previousOptions, desiredOptions);
        final Collection<OptionConfiguration> optionsToRemove = getOptionsToRemove(previousOptions, desiredOptions);

        final Map<String, String> previousTags = mergeMaps(
                request.getPreviousSystemTags(),
                request.getPreviousResourceTags()
        );
        final Map<String, String> desiredTags = mergeMaps(
                request.getSystemTags(),
                request.getDesiredResourceTags()
        );

        // Here we explicitly use some immutability properties of an OptionGroup resource.
        // In fact, ModifyOptionGroupRequest only passes optionConfigurations, the rest is immutable.
        // Therefore it is sufficient to check if there are any items in optionConfiguration include/remove lists.
        final boolean shouldUpdateCoreResource = !(optionsToInclude.isEmpty() && optionsToRemove.isEmpty());

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    // Skip the step if the core resource was not changed: tags-only change
                    if (!shouldUpdateCoreResource) {
                        return progress;
                    }
                    return proxy.initiate("rds::update-option-group", proxyClient, progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(model -> Translator.modifyOptionGroupRequest(model, optionsToInclude, optionsToRemove))
                            .backoffDelay(config.getBackoff())
                            .makeServiceCall((modifyRequest, proxyInvocation) -> proxyInvocation.injectCredentialsAndInvokeV2(
                                    modifyRequest,
                                    proxyInvocation.client()::modifyOptionGroup
                            ))
                            .handleError((modifyRequest, exception, client, resourceModel, ctx) -> Commons.handleException(
                                    ProgressEvent.progress(resourceModel, ctx),
                                    exception,
                                    DEFAULT_OPTION_GROUP_ERROR_RULE_SET
                            ))
                            .progress();
                })
                .then(progress -> updateTags(proxy, proxyClient, progress, previousTags, desiredTags))
                .then(progress -> new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger));
    }

    protected static boolean isOptionVersionDowngrade(
            final OptionConfiguration previousOption,
            final OptionConfiguration desiredOption
    ) {
        final OptionVersion previousVersion = new OptionVersion(previousOption.getOptionVersion());
        final OptionVersion desiredVersion = new OptionVersion(desiredOption.getOptionVersion());
        return previousVersion.compareTo(desiredVersion) > 0;
    }

    protected static boolean isApexOptionConfiguration(final OptionConfiguration optionConfiguration) {
        return Optional.ofNullable(optionConfiguration.getOptionName()).orElse("").equals(APEX_OPTION_NAME);
    }

    protected static Optional<OptionConfiguration> findApexOptionConfiguration(final Collection<OptionConfiguration> options) {
        return Optional.ofNullable(options).orElse(Collections.emptyList())
                .stream()
                .filter(UpdateHandler::isApexOptionConfiguration)
                .findFirst();
    }

    protected static Collection<OptionConfiguration> getOptionsToInclude(
            final Collection<OptionConfiguration> previousOptions,
            final Collection<OptionConfiguration> desiredOptions
    ) {
        final Optional<OptionConfiguration> previousApexOption = findApexOptionConfiguration(previousOptions);
        final Optional<OptionConfiguration> desiredApexOption = findApexOptionConfiguration(desiredOptions);

        Set<OptionConfiguration> optionsToInclude = new HashSet<>(desiredOptions);

        if (previousApexOption.isPresent() && desiredApexOption.isPresent()) {
            if (isOptionVersionDowngrade(previousApexOption.get(), desiredApexOption.get())) {
                optionsToInclude = optionsToInclude
                        .stream()
                        .filter(Objects::nonNull)
                        .map(option -> {
                            if (isApexOptionConfiguration(option)) {
                                return OptionConfiguration.builder()
                                        .dBSecurityGroupMemberships(option.getDBSecurityGroupMemberships())
                                        .optionName(option.getOptionName())
                                        .optionSettings(option.getOptionSettings())
                                        .optionVersion(previousApexOption.get().getOptionVersion())
                                        .port(option.getPort())
                                        .vpcSecurityGroupMemberships(option.getVpcSecurityGroupMemberships())
                                        .build();
                            }
                            return option;
                        })
                        .collect(Collectors.toSet());
            }
        }
        optionsToInclude.removeAll(previousOptions);

        return optionsToInclude;
    }

    protected static Collection<OptionConfiguration> getOptionsToRemove(
            final Collection<OptionConfiguration> previousOptions,
            final Collection<OptionConfiguration> desiredOptions
    ) {
        final Set<String> desiredOptionNames = desiredOptions.stream()
                .map(OptionConfiguration::getOptionName)
                .collect(Collectors.toSet());
        final Set<OptionConfiguration> optionsToRemove = previousOptions.stream()
                .filter(option -> !desiredOptionNames.contains(option.getOptionName()))
                .collect(Collectors.toSet());

        return optionsToRemove;
    }
}
