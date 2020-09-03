package software.amazon.rds.optiongroup;

import com.google.common.collect.ImmutableSet;
import lombok.NonNull;
import org.apache.commons.lang3.builder.EqualsBuilder;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class UpdateHandler extends BaseHandlerStd {

    private static final String APEX_OPTION_NAME = "APEX";

    protected ProgressEvent<ResourceModel, CallbackContext> handleRequest(
            final AmazonWebServicesClientProxy proxy,
            final ResourceHandlerRequest<ResourceModel> request,
            final CallbackContext callbackContext,
            final ProxyClient<RdsClient> proxyClient,
            final Logger logger) {

        final ResourceModel previous = request.getPreviousResourceState();
        final ResourceModel desired = request.getDesiredResourceState();

        final Collection<OptionConfiguration> previousOptions = Optional
                .ofNullable(previous.getOptionConfigurations())
                .orElse(Collections.emptySet());
        final Collection<OptionConfiguration> desiredOptions = Optional
                .ofNullable(desired.getOptionConfigurations())
                .orElse(Collections.emptySet());

        final Collection<OptionConfiguration> optionsToInclude = getOptionsToInclude(previousOptions, desiredOptions);
        final Collection<OptionConfiguration> optionsToRemove = getOptionsToRemove(previousOptions, desiredOptions);

        // Here we explicitly use some immutability properties of an OptionGroup resource.
        // In fact, ModifyOptionGroupRequest only passes optionConfigurations, the rest is immutable.
        // Therefore it is sufficient to check if there are any items in optionConfiguration include/remove lists.
        final boolean shouldUpdateCoreResource = !(optionsToInclude.isEmpty() && optionsToRemove.isEmpty());

        final Function<ResourceModel, ModifyOptionGroupRequest> decoratedModifyOptionGroupRequest = model -> Translator.modifyOptionGroupRequest(model, optionsToInclude, optionsToRemove);

        return ProgressEvent.progress(request.getDesiredResourceState(), callbackContext)
                .then(progress -> {
                    // Skip the step if the core resource was not changed: tags-only change
                    if (!shouldUpdateCoreResource) {
                        return progress;
                    }
                    return proxy.initiate("rds::update-option-group", proxyClient,progress.getResourceModel(), progress.getCallbackContext())
                            .translateToServiceRequest(decoratedModifyOptionGroupRequest)
                            .backoffDelay(CONSTANT)
                            .makeServiceCall((modifyOptionGroupRequest, proxyInvocation) -> {
                                return proxyInvocation.injectCredentialsAndInvokeV2(modifyOptionGroupRequest, proxyInvocation.client()::modifyOptionGroup);
                            })
                            .handleError((modifyOptionGroupRequest, exception, client, resourceModel, ctx) -> {
                                if (exception instanceof OptionGroupNotFoundException) {
                                    return ProgressEvent.defaultFailureHandler(exception, HandlerErrorCode.NotFound);
                                }
                                throw exception;
                            })
                            .progress();
                })
                .then(progress -> {
                    return tagResource(proxy, proxyClient, progress, request.getDesiredResourceTags());
                })
                .then(progress -> {
                    return new ReadHandler().handleRequest(proxy, request, callbackContext, proxyClient, logger);
                });
    }

    protected static boolean isOptionVersionDowngrade(
            @NonNull final OptionConfiguration previousOption,
            @NonNull final OptionConfiguration desiredOption
    ) {
        final OptionVersion previousVersion = new OptionVersion(previousOption.getOptionVersion());
        final OptionVersion desiredVersion = new OptionVersion(desiredOption.getOptionVersion());
        return previousVersion.compareTo(desiredVersion) > 0;
    }

    protected static boolean isApexOptionConfiguration(@NonNull final OptionConfiguration optionConfiguration) {
        return optionConfiguration.getOptionName().equals(APEX_OPTION_NAME);
    }

    protected static Optional<OptionConfiguration> findApexOptionConfiguration(@NonNull final Collection<OptionConfiguration> options) {
        return Optional.ofNullable(options).orElse(Collections.emptyList())
                .stream()
                .filter(UpdateHandler::isApexOptionConfiguration)
                .findFirst();
    }

    protected static Collection<OptionConfiguration> getOptionsToInclude(
            @NonNull final Collection<OptionConfiguration> previousOptions,
            @NonNull final Collection<OptionConfiguration> desiredOptions
    ) {
        final Optional<OptionConfiguration> previousApexOption = findApexOptionConfiguration(previousOptions);
        final Optional<OptionConfiguration> desiredApexOption = findApexOptionConfiguration(desiredOptions);

        Set<OptionConfiguration> optionsToInclude = new HashSet<>(desiredOptions);

        if (previousApexOption.isPresent() && desiredApexOption.isPresent()) {
            if (isOptionVersionDowngrade(previousApexOption.get(), desiredApexOption.get())) {
                optionsToInclude = optionsToInclude
                        .stream()
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
            @NonNull final Collection<OptionConfiguration> previousOptions,
            @NonNull final Collection<OptionConfiguration> desiredOptions
    ) {
        final Set<String> desiredOptionNames = desiredOptions.stream()
                .map(option -> option.getOptionName())
                .collect(Collectors.toSet());
        final Set<OptionConfiguration> optionsToRemove = previousOptions.stream()
                .filter(option -> !desiredOptionNames.contains(option.getOptionName()))
                .collect(Collectors.toSet());

        return optionsToRemove;
    }
}
