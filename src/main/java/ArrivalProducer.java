import java.util.List;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ArrivalProducer {

    private static final Logger log = LogManager.getLogger(ArrivalProducer.class);


    public static void callForArrivals() {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("assignmentservice", 5002)
                .usePlaintext()
                .build();

        ArrivalServiceGrpc.ArrivalServiceBlockingStub  arrivalServiceBlockingStub =
                ArrivalServiceGrpc.newBlockingStub(managedChannel);
        ArrivalRequest request = ArrivalRequest.newBuilder().setArrivalrequest("Give me the Assignment plz").build();
        ArrivalResponse reply = arrivalServiceBlockingStub.consumptionRate(request);
        log.info("Arrival from the producer is {}", reply);
        managedChannel.shutdown();
    }




}
