package software.amazon.rds.common.printer;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface JsonPrinter {

    String print(final Object obj) throws JsonProcessingException;

    String print(final Throwable throwable);
}
