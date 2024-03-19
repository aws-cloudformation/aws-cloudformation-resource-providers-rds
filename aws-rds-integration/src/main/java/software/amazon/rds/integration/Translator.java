package software.amazon.rds.integration;

import com.amazonaws.util.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.rds.model.CreateIntegrationRequest;
import software.amazon.awssdk.services.rds.model.DeleteIntegrationRequest;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.ModifyIntegrationRequest;
import software.amazon.rds.common.handler.Tagging;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Translator {
    private static final TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");
    private static final DateFormat DATETIME_FORMATTER;
    static {
        // this is mimicking what the AWS CLI gives you.
        DATETIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX");
        DATETIME_FORMATTER.setTimeZone(TZ_UTC);
    }

    public static CreateIntegrationRequest createIntegrationRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateIntegrationRequest.builder()
                .kmsKeyId(model.getKMSKeyId())
                .integrationName(model.getIntegrationName())
                .sourceArn(model.getSourceArn())
                .targetArn(model.getTargetArn())
                .additionalEncryptionContext(model.getAdditionalEncryptionContext())
                .tags(Tagging.translateTagsToSdk(tags))
                .dataFilter(model.getDataFilter())
                .description(model.getDescription())
                .build();
            }

    /**
     * For Update, do not update a field, if the previous value is not null, but the new value is null.
     * This is consistent most other fields in other RDS resources.
     * In the future, if we have more fields, we may need different logic such as resetting the value when
     * a field changes to null.
     */
    public static boolean shouldModifyField(
            ResourceModel previousModel,
            ResourceModel desiredModel,
            Function<ResourceModel, String> attributeGetter
    ) {
        String previousValue = attributeGetter.apply(previousModel);
        String desiredValue = attributeGetter.apply(desiredModel);
        if (StringUtils.equals(previousValue, desiredValue)) {
            return false;
        }
        if (desiredValue == null) {
            return false;
        }
        // this may change once we allow empty DataFilter or Description on the service-side.
        if (desiredValue.isEmpty()) {
            return false;
        }
        return true;
    }

    /** Generates the ModifyIntegrationRequest based on the previous and the desired models.
     * Precondition: software.amazon.rds.integration.UpdateHandler#shouldModifyIntegration()
     * should have returned true for the pair of models.
     * */
    public static ModifyIntegrationRequest modifyIntegrationRequest(
            final ResourceModel previousModel,
            final ResourceModel desiredModel
    ) {
        ModifyIntegrationRequest.Builder builder = ModifyIntegrationRequest.builder()
                                .integrationIdentifier(desiredModel.getIntegrationArn());

        if (shouldModifyField(previousModel, desiredModel, ResourceModel::getIntegrationName)) {
            // integration name can not be empty here, because we will populate it at the model level.
            builder.integrationName(desiredModel.getIntegrationName());
        }

        if (shouldModifyField(previousModel, desiredModel, ResourceModel::getDescription)) {
            // currently, due to a quirk, we cannot unset the description.
            // so we will ignore the empty case
            builder.description(desiredModel.getDescription());
        }

        if (shouldModifyField(previousModel, desiredModel, ResourceModel::getDataFilter)) {
            // currently, due to a quirk, we cannot unset the description.
            // so we will ignore the empty case
            builder.dataFilter(desiredModel.getDataFilter());
        }

        return builder.build();
    }

    static DescribeIntegrationsRequest describeIntegrationsRequest(final ResourceModel model) {
        DescribeIntegrationsRequest.Builder describeRequestBuilder = DescribeIntegrationsRequest.builder();
        if (model.getIntegrationArn() != null) {
            describeRequestBuilder.integrationIdentifier(model.getIntegrationArn());
        } else if (model.getIntegrationName() != null){
            describeRequestBuilder.filters(Filter.builder().name(model.getIntegrationName()).build());
        } else {
            throw new RuntimeException("The integration model has neither the ARN nor the name: " + model);
        }
        return describeRequestBuilder.build();
    }

    static DescribeIntegrationsRequest describeIntegrationsRequest(final String nextToken) {
        return DescribeIntegrationsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static DeleteIntegrationRequest deleteIntegrationRequest(final ResourceModel model) {
        return DeleteIntegrationRequest.builder()
                .integrationIdentifier(model.getIntegrationArn())
                .build();
    }
    public static List<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    static Set<Tag> translateTags(final Collection<software.amazon.awssdk.services.rds.model.Tag> rdsTags) {
        return CollectionUtils.isNullOrEmpty(rdsTags) ? null
                : rdsTags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toSet());
    }

    public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    static ResourceModel translateToModel(
            final Integration integration
    ) {
        return ResourceModel.builder()
                .createTime(
                        Optional.ofNullable(integration.createTime())
                                .map(Date::from)
                                .map(DATETIME_FORMATTER::format)
                                .orElse(null))
                .sourceArn(integration.sourceArn())
                .integrationArn(integration.integrationArn())
                .integrationName(integration.integrationName())
                .targetArn(integration.targetArn())
                .kMSKeyId(integration.kmsKeyId())
                .tags(translateTags(integration.tags()))
                .dataFilter(integration.dataFilter())
                .description(integration.description())
                .additionalEncryptionContext(integration.additionalEncryptionContext())
                .build();
    }
}
