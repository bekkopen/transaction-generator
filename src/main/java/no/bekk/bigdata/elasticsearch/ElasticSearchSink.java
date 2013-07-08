package no.bekk.bigdata.elasticsearch;

import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 6/20/13
 * Time: 4:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchSink implements TransactionSink {
    private final int limit = 1000;
    private Parameters parameters;
    private TransportClient client;
    private BulkRequestBuilder bulk;
    private Calendar calendar = Calendar.getInstance();

    @Override
    public void insert(Transaction trans) {
        calendar.setTime(trans.date);
        SimpleDateFormat indexFormat = new SimpleDateFormat("yyyy-MM");
        String index = indexFormat.format(calendar.getTimeInMillis());
        bulk.add(
                client.prepareIndex(index, "trans", "" + trans.id)
                .setSource(transactionToJSON(trans)).setRouting(trans.getAccountNumber())
        );

        if (bulk.numberOfActions() >= limit) {
            flush();
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", "TransactionCluster")
                .build();
        client = new TransportClient(settings);
        System.out.println("Connecting to: " + parameters.zkHost);
        String[] tokens = parameters.zkHost.split(":");
        client.addTransportAddress(new InetSocketTransportAddress(tokens[0], Integer.parseInt(tokens[1])));
        bulk = client.prepareBulk();
    }

    @Override
    public void close() {
        flush();
        client.close();
    }

    private String transactionToJSON(Transaction transaction) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(transaction);
        } catch(Exception e) {
            return "IO Error: " + e.getMessage();
        }
    }

    private boolean flush() {
        BulkResponse resp = bulk.execute().actionGet();
        if (resp.hasFailures()) {
            throw new RuntimeException(resp.buildFailureMessage());
        }
        bulk = client.prepareBulk();
        return true;
    }
}
