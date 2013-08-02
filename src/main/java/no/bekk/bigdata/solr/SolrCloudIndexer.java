package no.bekk.bigdata.solr;


import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;

import java.io.IOException;

public class SolrCloudIndexer implements TransactionSink {

    private Parameters parameters;
    private CloudSolrServer server;
    private int inserts = 0;
    @Override
    public void insert(Transaction trans) {
        try {
            if (server == null) {
                this.server = new CloudSolrServer(parameters.host);
                this.server.setDefaultCollection("collection1");
            }
            this.server.addBean(SolrBankTransaction.fromTransaction(trans));
            if(inserts++ % 1_000_000 == 0){
                this.server.commit();
            }
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.server.commit();
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
    }
}
