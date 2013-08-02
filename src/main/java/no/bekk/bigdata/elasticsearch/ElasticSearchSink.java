package no.bekk.bigdata.elasticsearch;

import no.bekk.bigdata.Main;
import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;
import no.bekk.bigdata.csv.CSVSink;
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
    private final boolean DEBUG;
    private final int LIMIT;
    private Parameters parameters;
    private TransportClient client;
    private BulkRequestBuilder bulk;
    private Calendar calendar;
    private static final int PORT = 9300;
    
    public ElasticSearchSink() {
        DEBUG = Main.debug();
        LIMIT = 1000;
        calendar = Calendar.getInstance();
    }

    @Override
    public void insert(Transaction trans) {
        calendar.setTime(trans.date);
        SimpleDateFormat indexFormat = new SimpleDateFormat(parameters.esindexformat);
        String index = indexFormat.format(calendar.getTimeInMillis());
        String body = trans.toElasticJSON();

        if (Main.debug()) {
            System.out.println(body);
        } else {
            bulk.add(
                    client.prepareIndex(index, "trans", "" + trans.id)
                            .setSource(body).setRouting(trans.getAccountNumber())
            );

            if (bulk.numberOfActions() >= LIMIT) {
                flush();
            }
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
        if (DEBUG) return;

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", parameters.clusterName)
                .build();
        client = new TransportClient(settings);
        System.out.println("Connecting to: " + parameters.host);
        String[] hosts = parameters.host.split(",");

        for (String s : hosts)
                client.addTransportAddress(new InetSocketTransportAddress(s, PORT));

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
