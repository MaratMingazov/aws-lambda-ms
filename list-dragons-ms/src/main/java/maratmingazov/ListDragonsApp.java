package maratmingazov;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class ListDragonsApp implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {


    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        return generateResponse("HelloWorld");
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
