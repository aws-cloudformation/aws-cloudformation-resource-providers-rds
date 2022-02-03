package software.amazon.rds.common.printer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.amazonaws.AmazonServiceException;
import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

class FilteredJsonPrinterTest {

    @Test
    void testPrint() throws JsonProcessingException {
        ResourceHandlerRequest<Void> request = new ResourceHandlerRequest<>();
        request.setAwsAccountId("AWS_ACCOUNT_ID");
        request.setClientRequestToken("TOKEN");
        request.setStackId("STACK_ID");
        FilteredJsonPrinter filteredJsonPrinter = new FilteredJsonPrinter("awsAccountId");
        Assertions.assertFalse(filteredJsonPrinter.print(request).contains("awsAccountId"));
        Assertions.assertTrue(filteredJsonPrinter.print(request).contains("STACK_ID"));
    }

    @Test
    void testPrintNull() {
        try {
            FilteredJsonPrinter filteredJsonPrinter = new FilteredJsonPrinter("awsAccountId");
            filteredJsonPrinter.print(null);
        } catch (Exception exception) {
            Assertions.fail("Should fail silently");
        }
    }

    @Test
    void testPrintException() {
        try {
            throw new AmazonServiceException("RDS");
        } catch (Exception e) {
            FilteredJsonPrinter filteredJsonPrinter = new FilteredJsonPrinter("awsAccountId");
            Assertions.assertTrue(filteredJsonPrinter.print(e).contains("RDS"));
        }
    }

}
