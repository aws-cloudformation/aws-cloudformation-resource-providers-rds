package software.amazon.rds.common.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

class ValidationsTest {

    @Test
    void testValidateEmptySourceRegion() {
        String nullRegion = null;
        try {
            Validations.validateSourceRegion(nullRegion);
        } catch (RequestValidationException e) {
            fail("Null region is valid");
        }
    }

    @Test
    void testValidateSourceRegion() {
        String validRegion = "us-east-1";
        try {
            Validations.validateSourceRegion(validRegion);
        } catch (RequestValidationException e) {
            fail("Region is valid");
        }
    }

    @Test
    void testValidateSourceRegionInvalid() {
        String invalidRegion = "not-a-region";
        try {
            Validations.validateSourceRegion(invalidRegion);
            fail("Region is invalid");
        } catch (RequestValidationException e) {
            return;
        }
    }

    @Test
    void testValidateNullTimestampRegion() {
        String nullTimestamp = null;
        try {
            Validations.validateSourceRegion(nullTimestamp);
        } catch (RequestValidationException e) {
            fail("Null timestamp is valid");
        }
    }

    @Test
    void testValidateTimestampRegion() {
        String validTimestamp = "2015-03-07T23:45:00Z";
        try {
            Validations.validateTimestamp(validTimestamp);
        } catch (RequestValidationException e) {
            fail("Timestamp is valid");
        }
    }

    @Test
    void testValidateTimestampInvalid() {
        String invalidTimestamp = "66465521456";
        try {
            Validations.validateTimestamp(invalidTimestamp);
            fail("Timestamp is invalid");
        } catch (RequestValidationException e) {
            return;
        }
    }

    @Test
    void testValidatedRequest() {
        ResourceHandlerRequest<Object> request = ResourceHandlerRequest.builder()
                .awsAccountId("accountId")
                .awsPartition("aws")
                .clientRequestToken("clientToken")
                .driftable(true)
                .nextToken("nextToken")
                .region("region")
                .rollback(false)
                .snapshotRequested(false)
                .stackId("stackId")
                .build();

        ValidatedRequest<Object> validatedRequest = new ValidatedRequest<>(request);

        assertThat(validatedRequest)
                .usingRecursiveComparison()
                .isEqualTo(request);
    }

}
