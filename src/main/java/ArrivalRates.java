import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ArrivalRates {
    private static final Logger log = LogManager.getLogger(ArrivalRates.class);


    static ArrayList<Partition> topicpartitions;

    static {


        topicpartitions = new ArrayList<>();
        for (int i = 0; i <= 4; i++) {
            topicpartitions.add(new Partition(i, 0, 0));
        }

    }

    static double arrivalRateTopicGeneral() {

        List<String> arrivalqueries = Constants.getQueriesArrival("testtopic1");

        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(arrivalqueries.get(1)),
                    new URI(arrivalqueries.get(2)),
                    new URI(arrivalqueries.get(3)),
                    new URI(arrivalqueries.get(4)),
                    new URI(arrivalqueries.get(5))
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions.get(partition2)
                    .setArrivalRate(partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        //topicpartitions.setTotalArrivalRate(totalarrivalstopic2);
        log.info("totalArrivalRate for  topic  {} {}",
                "testtopic1", totalarrivalstopic2);



        return totalarrivalstopic2;


    }



    static double LagTopicGeneral() {

        //List<String> arrivalqueries = Constants.getQueriesLag("testtopic1", "testgroup1");//getQueriesLagAvg("testtopic1", "testgroup1");
        List<String> arrivalqueries = Constants.getQueriesLagAvg("testtopic1", "testgroup1");


        HttpClient client = HttpClient.newHttpClient();
        List<URI> partitions2 = new ArrayList<>();
        try {
            partitions2 = Arrays.asList(
                    new URI(arrivalqueries.get(1)),
                    new URI(arrivalqueries.get(2)),
                    new URI(arrivalqueries.get(3)),
                    new URI(arrivalqueries.get(4)),
                    new URI(arrivalqueries.get(5))
            );
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


        List<CompletableFuture<String>> partitionsfutures2 = partitions2.stream()
                .map(target -> client
                        .sendAsync(
                                HttpRequest.newBuilder(target).GET().build(),
                                HttpResponse.BodyHandlers.ofString())
                        .thenApply(HttpResponse::body))
                .collect(Collectors.toList());


        int partition2 = 0;
        double totalarrivalstopic2 = 0.0;
        double partitionArrivalRate2 = 0.0;
        for (CompletableFuture<String> cf : partitionsfutures2) {
            try {
                partitionArrivalRate2 = Util.parseJsonArrivalRate(cf.get(), partition2);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            topicpartitions.get(partition2)
                    .setLag((long)partitionArrivalRate2);

            totalarrivalstopic2 += partitionArrivalRate2;
            partition2++;
        }
        //topicpartitions.setTotalArrivalRate(totalarrivalstopic2);
        log.info("rotal average lag  for  topic  {} {}",
                "testtopic1", totalarrivalstopic2);



        return totalarrivalstopic2;


    }





}