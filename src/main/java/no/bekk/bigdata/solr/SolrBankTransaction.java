package no.bekk.bigdata.solr;

import no.bekk.bigdata.Transaction;
import org.apache.solr.client.solrj.beans.Field;

import java.math.BigDecimal;
import java.util.Date;

public class SolrBankTransaction {

    @Field
    public final long id;

    @Field
    public final Date date;

    @Field
    public final double amount;

    @Field
    public final String description;

    @Field
    public final String accountNumber;

    @Field
    public final String remoteAccountNumber;


    public SolrBankTransaction(long id, Date date, BigDecimal amount, String description, String accountNumber, String remoteAccountNumber) {
        this.id = id;
        this.date = date;
        this.amount = amount.doubleValue();
        this.description = description;
        this.accountNumber = accountNumber;
        this.remoteAccountNumber = remoteAccountNumber;
    }

    public static SolrBankTransaction fromTransaction(Transaction trans) {
        return new SolrBankTransaction(trans.id, trans.date, trans.amount, trans.description, trans.getAccountNumber(), trans.remoteAccountNumber);
    }
}
