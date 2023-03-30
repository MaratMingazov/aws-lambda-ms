package maratmingazov;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;
import software.amazon.awssdk.services.ssm.model.GetParameterResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class ListDragonsApp implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private final SsmClient ssmClient = SsmClient.builder().build();
    private final S3AsyncClient s3 = S3AsyncClient.builder().build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
        String dragonsData = readDragonData(event);
        return generateResponse(dragonsData);
    }

    private String readDragonData(APIGatewayProxyRequestEvent event) {
        Map<String, String> queryParams = event.getQueryStringParameters();
        String bucketName = getBucketName();
        return bucketName;
    }

    private String getBucketName() {
        GetParameterRequest getParameterRequest = GetParameterRequest.builder()
                                                                     .name("dragon_data_bucket_name")
                                                                     .withDecryption(false)
                                                                     .build();
        GetParameterResponse getParameterResponse = ssmClient.getParameter(getParameterRequest);
        return getParameterResponse.parameter().value();
    }

    private  APIGatewayProxyResponseEvent generateResponse(String dragons) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        Map<String, String> headers = new HashMap<>();
        headers.put("access-control-allow-origin", "*");
        response.setStatusCode(200);
        response.setBody(dragons);
        response.setHeaders(headers);
        return response;
    }
}
