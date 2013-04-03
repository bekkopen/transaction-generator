package no.bekk.bigdata;

import org.apache.solr.client.solrj.SolrServerException;

import java.io.IOException;

public interface TransactionSink {
    void insert(Transaction trans);
    void close();
    void setParameters(Parameters parameters);
}
