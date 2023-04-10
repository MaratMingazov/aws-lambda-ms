package maratmingazov;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompressionType;
import software.amazon.awssdk.services.s3.model.ExpressionType;
import software.amazon.awssdk.services.s3.model.InputSerialization;
import software.amazon.awssdk.services.s3.model.JSONInput;
import software.amazon.awssdk.services.s3.model.JSONOutput;
import software.amazon.awssdk.services.s3.model.OutputSerialization;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponse;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.selectobjectcontenteventstream.DefaultRecords;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Hello world!
 *
 */
public class ValidateDragonApp implements RequestHandler<Map<String, String>, String> {


    private final SsmClient ssmClient = SsmClient.builder().build();
    private final S3AsyncClient s3 = S3AsyncClient.builder().build();

    @Override
    public String handleRequest(Map<String, String> stringStringMap, Context context) {
        readDragonData(stringStringMap);
        System.err.println("Dragon was successfully validated.");
        return "Dragon validated";
    }

    protected  void readDragonData(Map<String, String> event) {
        String bucketName = getBucketName();
        String key = getKey();
        String query = getQuery(event);

        TestHandler testHandler = new TestHandler();

        // See reference on AWS SDK for Java 2.x async programming
        // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/asynchronous.html
        CompletableFuture<Void> selected = queryS3(s3, bucketName, key, query, testHandler);
        selected.join();

        for (SelectObjectContentEventStream events : testHandler.receivedEvents) {
            if (events instanceof DefaultRecords) {
                DefaultRecords defaultRecords = (DefaultRecords) events;
                String payload = defaultRecords.payload().asString(StandardCharsets.UTF_8);

                if (payload != null && !(payload.equals(""))) {
                    throw new DragonValidationException("Duplicate dragon reported", new RuntimeException());
                }
            }
        }
    }

    private String getBucketName() {
        GetParameterRequest getParameterRequest = GetParameterRequest
                .builder()
                .name("dragon_data_bucket_name")
                .withDecryption(false).build();
        GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
        return getParameterResponse.parameter().value();
    }

    private String getKey() {
        GetParameterRequest getParameterRequest = GetParameterRequest
                .builder()
                .name("dragon_data_file_name")
                .withDecryption(false).build();
        GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
        return getParameterResponse.parameter().value();
    }

    private static String getQuery(Map<String, String> event) {
        return "select * from S3Object[*][*] s where s.dragon_name_str =  '"
                + event.get("dragon_name_str") + "'";
    }

    private static CompletableFuture<Void> queryS3(S3AsyncClient s3,
                                                   String bucketName,
                                                   String key,
                                                   String query,
                                                   SelectObjectContentResponseHandler handler) {
        InputSerialization inputSerialization = InputSerialization.builder()
                                                                  .json(JSONInput.builder().type("Document").build())
                                                                  .compressionType(CompressionType.NONE)
                                                                  .build();

        OutputSerialization outputSerialization = OutputSerialization.builder()
                                                                     .json(JSONOutput.builder().build())
                                                                     .build();

        SelectObjectContentRequest select = SelectObjectContentRequest.builder()
                                                                      .bucket(bucketName)
                                                                      .key(key)
                                                                      .expression(query)
                                                                      .expressionType(ExpressionType.SQL)
                                                                      .inputSerialization(inputSerialization)
                                                                      .outputSerialization(outputSerialization)
                                                                      .build();

        return s3.selectObjectContent(select, handler);
    }

    private class TestHandler implements SelectObjectContentResponseHandler {
        private List<SelectObjectContentEventStream> receivedEvents = new ArrayList<>();

        @Override
        public void responseReceived(SelectObjectContentResponse response) {
        }

        @Override
        public void onEventStream(SdkPublisher<SelectObjectContentEventStream> publisher) {
            publisher.subscribe(receivedEvents::add);
        }

        @Override
        public void exceptionOccurred(Throwable throwable) {
        }

        @Override
        public void complete() {
        }
    }
}
