
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BinPack2 {

    //TODO give fup and fdown as paramters to the functions.
    private static final Logger log = LogManager.getLogger(BinPack2.class);
    private  int size =1;
   private   Instant LastUpScaleDecision = Instant.now();
   private final double wsla = 0.5;
   private final double dynamicAverageMaxConsumptionRate = 200.0;




    public  void scaleAsPerBinPack() {
        log.info("Currently we have this number of consumers group {} {}","testgroup1", size );
        int neededsize = binPackAndScale();
        log.info("We currently need the following consumers for group1 (as per the bin pack) {}", neededsize);
        int replicasForscale = neededsize - size;
        if (replicasForscale > 0) {
            //TODO IF and Else IF can be in the same logic
            log.info("We have to upscale  group1 by {}", replicasForscale);
           size = neededsize;
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsize);
                log.info("I have Upscaled group {} you should have {}", "testgroup1", neededsize);
                 LastUpScaleDecision= Instant.now();
            }
        } else {
            int neededsized = binPackAndScaled();
            int replicasForscaled = size - neededsized;
            if (replicasForscaled > 0) {
                log.info("We have to downscale  group by {} {}", "testgroup1", replicasForscaled);
                size = neededsized;
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    k8s.apps().deployments().inNamespace("default").withName("latency").scale(neededsized);
                    log.info("I have downscaled group {} you should have {}", "testgroup1", neededsized);
                }
                LastUpScaleDecision = Instant.now();
            }
        }
        log.info("===================================");
    }


    private  int binPackAndScale() {
        log.info(" shall we upscale group {}", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);





        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > dynamicAverageMaxConsumptionRate*wsla) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f",dynamicAverageMaxConsumptionRate*wsla));
                partition.setArrivalRate(dynamicAverageMaxConsumptionRate*wsla);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());

        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(t)), 0L,
                        dynamicAverageMaxConsumptionRate*wsla));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers);
                for (i = 0; i < consumerCount; i++) {

                    if (/*consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&*/
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }
        log.info(" The BP up scaler recommended for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


    private  int binPackAndScaled() {
        log.info(" shall we down scale group {} ", "testgroup1");
        List<Consumer> consumers = new ArrayList<>();
        int consumerCount = 1;
        List<Partition> parts = new ArrayList<>(ArrivalRates.topicpartitions);
        double fractiondynamicAverageMaxConsumptionRate = dynamicAverageMaxConsumptionRate * 0.7*wsla;




        //if a certain partition has an arrival rate  higher than R  set its arrival rate  to R
        //that should not happen in a well partionned topic
        for (Partition partition : parts) {
            if (partition.getArrivalRate() > fractiondynamicAverageMaxConsumptionRate) {
                log.info("Since partition {} has arrival rate {} higher than consumer service rate {}" +
                                " we are truncating its arrival rate", partition.getId(),
                        String.format("%.2f", partition.getArrivalRate()),
                        String.format("%.2f", fractiondynamicAverageMaxConsumptionRate));
                partition.setArrivalRate(fractiondynamicAverageMaxConsumptionRate);
            }
        }
        //start the bin pack FFD with sort
        Collections.sort(parts, Collections.reverseOrder());
        while (true) {
            int j;
            consumers.clear();
            for (int t = 0; t < consumerCount; t++) {
                consumers.add(new Consumer((String.valueOf(consumerCount)), 0L,
                        fractiondynamicAverageMaxConsumptionRate));
            }

            for (j = 0; j < parts.size(); j++) {
                int i;
                Collections.sort(consumers);
                for (i = 0; i < consumerCount; i++) {

                    if (/*consumers.get(i).getRemainingLagCapacity() >= parts.get(j).getLag() &&*/
                            consumers.get(i).getRemainingArrivalCapacity() >= parts.get(j).getArrivalRate()) {
                        consumers.get(i).assignPartition(parts.get(j));
                        break;
                    }
                }
                if (i == consumerCount) {
                    consumerCount++;
                    break;
                }
            }
            if (j == parts.size())
                break;
        }

        log.info(" The BP down scaler recommended  for group {} {}", "testgroup1", consumers.size());
        return consumers.size();
    }


}
