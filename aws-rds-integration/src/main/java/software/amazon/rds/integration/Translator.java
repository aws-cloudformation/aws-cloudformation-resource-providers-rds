package software.amazon.rds.integration;

import com.amazonaws.util.CollectionUtils;
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
            static ModifyIntegrationRequest modifyIntegrationRequest(
          final ResourceModel model
    ) {
                return ModifyIntegrationRequest.builder()
                                .integrationName(model.getIntegrationName())
                                .description(model.getDescription())
                                .dataFilter(model.getDataFilter())
                .build();
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
