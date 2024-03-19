package software.amazon.rds.integration;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Filter;
import software.amazon.awssdk.services.rds.model.ModifyIntegrationRequest;

import java.util.List;

@ExtendWith(MockitoExtension.class)
public class TranslatorTest {

    @Test
    public void translateTags_withNullTags() {
        Assertions.assertNull(Translator.translateTags(null));
    }

    @Test
    public void translateTags_withNonNullTags() {
        Assertions.assertEquals(
                ImmutableSet.of(
                        Tag.builder().key("k1").value("v1").build(),
                        Tag.builder().key("k2").value("v2").build()
                ),
                Translator.translateTags(
                    ImmutableSet.of(
                            software.amazon.awssdk.services.rds.model.Tag.builder().key("k1").value("v1").build(),
                            software.amazon.awssdk.services.rds.model.Tag.builder().key("k2").value("v2").build()
                    )
            )
        );
    }

    @Test
    public void translateDescribeWithoutArn_shouldUseName() {
        DescribeIntegrationsRequest request = Translator.describeIntegrationsRequest(
                ResourceModel.builder()
                        .integrationName("integname12345").build()
        );
        Assertions.assertNull(request.integrationIdentifier());
        List<Filter> filters = request.filters();
        Assertions.assertEquals(filters.size(), 1);
        Assertions.assertEquals("integname12345", filters.get(0).name());
    }

    @Test
    public void translateDescribeWithoutArnOrName_shouldThrow() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            Translator.describeIntegrationsRequest(
                    ResourceModel.builder().build()
            );
        });
    }

    @Test
    public void translateModify_differentName_shouldOnlyModifyName() {
        ModifyIntegrationRequest request = Translator.modifyIntegrationRequest(
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc")
                        .dataFilter("include: d1.t1")
                        .build(),
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name2")
                        .description("desc")
                        .dataFilter("include: d1.t1")
                        .build()
        );
        Assertions.assertEquals(request.integrationIdentifier(), "arn");
        Assertions.assertEquals(request.integrationName(), "name2");
        Assertions.assertNull(request.description());
        Assertions.assertNull(request.dataFilter());
    }

    @Test
    public void translateModify_differentDescription_shouldOnlyModifyDescription() {
        ModifyIntegrationRequest request = Translator.modifyIntegrationRequest(
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc")
                        .dataFilter("include: d1.t1")
                        .build(),
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc2")
                        .dataFilter("include: d1.t1")
                        .build()
        );
        Assertions.assertEquals(request.integrationIdentifier(), "arn");
        Assertions.assertEquals(request.description(), "desc2");
        Assertions.assertNull(request.integrationName());
        Assertions.assertNull(request.dataFilter());
    }

    @Test
    public void translateModify_differentDataFilter_shouldOnlyModifyDataFilter() {
        ModifyIntegrationRequest request = Translator.modifyIntegrationRequest(
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc")
                        .dataFilter("include: d1.t1")
                        .build(),
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc")
                        .dataFilter("include: d1.t2")
                        .build()
        );
        Assertions.assertEquals(request.integrationIdentifier(), "arn");
        Assertions.assertEquals(request.dataFilter(), "include: d1.t2");
        Assertions.assertNull(request.integrationName());
        Assertions.assertNull(request.description());
    }

    @Test
    public void translateModify_differentFields_shouldModifyAll() {
        ModifyIntegrationRequest request = Translator.modifyIntegrationRequest(
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name1")
                        .description("desc1")
                        .dataFilter("include: d1.t1")
                        .build(),
                ResourceModel.builder()
                        .integrationArn("arn")
                        .integrationName("name2")
                        .description("desc2")
                        .dataFilter("include: d2.t2")
                        .build()
        );
        Assertions.assertEquals(request.integrationIdentifier(), "arn");
        Assertions.assertEquals(request.integrationName(), "name2");
        Assertions.assertEquals(request.description(), "desc2");
        Assertions.assertEquals(request.dataFilter(), "include: d2.t2");
    }
}
