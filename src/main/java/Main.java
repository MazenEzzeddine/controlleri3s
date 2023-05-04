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


    static double  currentReplicas= 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        System.out.println("Hello");
        initialize();

    }



    private static void initialize() throws InterruptedException, ExecutionException {


        log.info("Warming 30  seconds.");
        Thread.sleep(30 * 1000);

        while (true) {
            log.info("Querying Prometheus");
           double ar =   ArrivalRates.arrivalRateTopicGeneral();
            log.info("Sleeping for 5 seconds");
            log.info("******************************************");
            log.info("******************************************");

            scaleLogic(ar);
            Thread.sleep(5000);
        }
    }

    private static void scaleLogic(double ar) {

       double  neededReplicas = Math.ceil(ar/(90));

       if(neededReplicas != currentReplicas) {
           try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
               k8s.apps().deployments().inNamespace("default").withName("consumer").scale((int)neededReplicas);
               log.info("I have Upscaled group {} you should have {}", "testgroup1", currentReplicas);
               currentReplicas = neededReplicas;
           }
       }



    }
}
