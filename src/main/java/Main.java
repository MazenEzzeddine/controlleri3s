import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;

public class Main {

    private static final Logger log = LogManager.getLogger(Main.class);
    static BinPack2 bp;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        initialize();
    }



    private static void initialize() throws InterruptedException, ExecutionException {
         bp = new BinPack2();
        log.info("Warming 30  seconds.");
        Thread.sleep(30 * 1000);



        while (true) {
            log.info("Querying Prometheus");
            ArrivalRates.arrivalRateTopicGeneral();
            ArrivalRates.LagTopicGeneral();
            scaleLogic();
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(5000);
        }
    }

    private static void scaleLogic() {
     bp.scaleAsPerBinPack();

    }
}
