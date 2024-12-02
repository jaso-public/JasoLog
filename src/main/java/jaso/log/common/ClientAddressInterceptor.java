package jaso.log.common;

import java.net.SocketAddress;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;


public class ClientAddressInterceptor implements ServerInterceptor {
    // Define a context key to store the client address
    public static final Context.Key<SocketAddress> CLIENT_ADDRESS_KEY = Context.key("clientAddress");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        // Get the client address from the call attributes
        SocketAddress clientAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);

        // Attach the client address to the context
        Context context = Context.current().withValue(CLIENT_ADDRESS_KEY, clientAddress);

        // Proceed with the call in the modified context
        return Contexts.interceptCall(context, call, headers, next);
    }
}
