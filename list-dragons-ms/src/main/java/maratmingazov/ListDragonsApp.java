package maratmingazov;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.Map;

/**
 * Hello world!
 *
 */
public class ListDragonsApp implements RequestHandler<Map<String, String>, String> {


    @Override
    public String handleRequest(Map<String, String> stringStringMap,
                                Context context) {
        return "helloWorld";
    }
}
