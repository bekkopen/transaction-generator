package no.bekk.bigdata.solr;


import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class MultiSolrIndexer implements TransactionSink{
    private List<SolrIndexer> indexers = new ArrayList<>();
    long currentIndexer = 0;

    @Override
    public void insert(Transaction trans) {
        indexers.get((int)(currentIndexer++ % indexers.size())).insert(trans);
    }

    @Override
    public void close() {
        for(SolrIndexer indexer : indexers){
            indexer.close();
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        for(int i = 0; i < parameters.solrUrl.length; i++){
            SolrIndexer indexer = new SolrIndexer();
            indexer.setParameters(parameters);
            indexer.overrideSolrUrl(parameters.solrUrl[i]);
            indexers.add(indexer);
        }
    }
}
