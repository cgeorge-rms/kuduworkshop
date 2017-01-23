package com.rms.kuduworkshop;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/*
In this example I use the token based scanner to run parallel reads accross multiple tablets
This is what the spark datasource is doing behind the scenes
This depends on the address table being populated from the parquet file in SparkExample.scala
 */
public class ComplexParallelReadExample {

    public static void main(String[] args) {

        KuduClient client = new KuduClient.KuduClientBuilder("127.0.0.1:64046").build();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(10);

        try {
            //This will get information about the hashes and the table partitions
            KuduTable kuduAddressTable = client.openTable("address");
            // The number of tokens created is the number of partitions that will be read
            List<KuduScanToken> tokens = client.newScanTokenBuilder(kuduAddressTable)
                    .addPredicate(KuduPredicate.newComparisonPredicate(kuduAddressTable.getSchema().getColumn("countryCode"), KuduPredicate.ComparisonOp.EQUAL, "GB"))
                    .build();
//                    .setProjectedColumnNames() I can set my projections with this but I'll leave it blank to get the entire rows

            //blocking queue to "merge" the results...
            final BlockingQueue<String> queue = new LinkedBlockingDeque<>(1000);
            //Future list to know when we are done
            List<Future> futures = new ArrayList<>(10);

            for (final KuduScanToken token : tokens) {
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // This creates a scanner based on the tokens
                            KuduScanner kuduScanner = KuduScanToken.deserializeIntoScanner(token.serialize(), client);
                            while (kuduScanner.hasMoreRows()) {
                                RowResultIterator rowResults = kuduScanner.nextRows();
                                while(rowResults.hasNext()) {
                                    RowResult next = rowResults.next(); // fyi this does an in memory lookup for a backing memory array so you need to deep copy the data
                                    try {
                                        queue.put(next.rowToString());//I'm being lazy here as you would want to put into actual structure of some type
                                    } catch (InterruptedException e) {}
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                };
                futures.add(fixedThreadPool.submit(runnable));

            }

            int count = 0;
            while (true) {
                try {
                    String item = queue.poll(1, TimeUnit.SECONDS);
                    if (item==null) {
                        for (Future future : futures) {
                            if (!future.isDone()) {
                                return; // we are not done yet as there are still futures left
                            }
                        }
                        break;
                    }
                    count++;
                } catch (InterruptedException e) {}
            }
            System.out.println("Number of rows retrieved = " + count);

            fixedThreadPool.shutdown();
        } catch (KuduException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fixedThreadPool.shutdown();
            try {
                client.shutdown();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }


    }
}
