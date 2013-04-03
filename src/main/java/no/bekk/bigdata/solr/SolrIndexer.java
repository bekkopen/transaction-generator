package no.bekk.bigdata.solr;

import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;

import java.io.IOException;

public class SolrIndexer implements TransactionSink {

    private ConcurrentUpdateSolrServer solr;
    private Parameters parameters;
    private String overrideSolrUrl;

    @Override
    public void insert(Transaction trans) {
        if(this.solr == null){
            this.solr = new ConcurrentUpdateSolrServer(overrideSolrUrl != null ? overrideSolrUrl : parameters.solrUrl[0], 1000, 3);
        }
        try {
            this.solr.addBean(SolrBankTransaction.fromTransaction(trans));
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            this.solr.optimize();
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
    }

    public void overrideSolrUrl(String solrUrl) {
        this.overrideSolrUrl = solrUrl;
    }
}
