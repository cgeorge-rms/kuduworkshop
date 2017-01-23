package com.rms.kuduworkshop;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Another example from kudu examples... note that it deletes the table at the end of the run
 * The master has been hard coded for your convenience.
 */
public class SampleCreateAndRead {

    private static final String KUDU_MASTER = "127.0.0.1:64046";

    public static void main(String[] args) {
        System.out.println("-----------------------------------------------");
        System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
        System.out.println("-----------------------------------------------");
        String tableName = "java_sample-" + System.currentTimeMillis();
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

        try {
            List<ColumnSchema> columns = new ArrayList(2);
            columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                    .key(true)
                    .build());
            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");

            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys));

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            for (int i = 0; i < 3; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "value " + i);
                session.apply(insert);
            }

            List<String> projectColumns = new ArrayList<>(1);
            projectColumns.add("value");
            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    System.out.println(result.getString(0));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}