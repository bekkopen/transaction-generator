package no.bekk.bigdata.elasticsearch;

import com.sun.xml.bind.v2.TODO;
import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;

import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: andre b. amundsen
 * Date: 6/20/13
 * Time: 4:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchSink implements TransactionSink {

    private Parameters parameters;
    private ArrayList<Transaction> trans = new ArrayList<>();
    private int limit = 100;

    @Override
    public void insert(Transaction trans) {
        this.trans.add(trans);
        if (this.trans.size() >= limit){
            close();
            this.trans.clear();
        }
    }

    @Override
    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
        //build html string
    }

    @Override
    public void close() {
        //To change body of implemented methods use File | Settings | File Templates.
        //On close send transaction
    }

    private String transactionToJSON(Transaction transaction) {
        String JSON = ""; //get an addon (jackson objectmapper)
        /*JSON = String.format("accountNumber: \"%i\"", transaction.accountNumber);
        JSON.concat(String.format("amount: \"%i\"", transaction.amount));
        JSON.concat(String.format("archiveReference: \"%i\"", transaction.archiveReference));
        JSON.concat(String.format("batchNumber: \"%i\"", transaction.batchNumber));
        JSON.concat(String.format("bokforingDate: \"%i\"", transaction.bokforingDate));
        JSON.concat(String.format("currencyAmount: \"%i\"", transaction.currencyAmount));
        JSON.concat(String.format("currencyCode: \"%i\"", transaction.currencyCode));
        JSON.concat(String.format("date: \"%i\"", transaction.date));
        JSON.concat(String.format("description: \"%i\"", transaction.description));
        JSON.concat(String.format("fullDescription: \"%i\"", transaction.fullDescription));
        JSON.concat(String.format("id: \"%i\"", transaction.id));
        JSON.concat(String.format("isConfidential: \"%i\"", transaction.isConfidential));
        JSON.concat(String.format("numbericalReference: \"%s\"", transaction.numbericalReference));*/
        return JSON;
    }
}
