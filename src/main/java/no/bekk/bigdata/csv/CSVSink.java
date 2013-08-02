package no.bekk.bigdata.csv;

import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: torkil aamodt
 * Date: 7/17/13
 * Time: 11:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class CSVSink implements TransactionSink {
    private Parameters params;
    private FileWriter out;

    @Override
    public void insert(Transaction trans) {
        try {
            out.write(trans.toCSVJSON()+'\n');
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setParameters(Parameters params) {
        this.params = params;

        try {
            out = new FileWriter(params.host);
            out.write(Transaction.toCSVHeaderJSON()+'\n');
        } catch (IOException exception) {
            System.out.print(exception.getMessage());
            throw new RuntimeException("Could not open file for writing!");
        }
    }
}
