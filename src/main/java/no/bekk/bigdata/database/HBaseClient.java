package no.bekk.bigdata.database;

import no.bekk.bigdata.Transaction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseClient implements DatabaseClient {
    private HTable table;

    public HBaseClient() throws IOException {

        Configuration config = HBaseConfiguration.create();
        try {
            table = new HTable(config, "transactionTable");
        } catch (TableNotFoundException e) {
            HBaseAdmin hbase = new HBaseAdmin(config);
            HTableDescriptor desc = new HTableDescriptor("transactionTable");
            HColumnDescriptor meta[] = new HColumnDescriptor[]{
                    new HColumnDescriptor(Bytes.toBytes("date")),
                    new HColumnDescriptor(Bytes.toBytes("amount")),
                    new HColumnDescriptor(Bytes.toBytes("description")),
                    new HColumnDescriptor(Bytes.toBytes("accountNumber")),
                    new HColumnDescriptor(Bytes.toBytes("transactionCode")),
                    new HColumnDescriptor(Bytes.toBytes("valuteringDate")),
                    new HColumnDescriptor(Bytes.toBytes("posteringDate")),
                    new HColumnDescriptor(Bytes.toBytes("bokforingDate")),
                    new HColumnDescriptor(Bytes.toBytes("details"))
            };
            for (HColumnDescriptor column : meta) {
                desc.addFamily(column);
            }
            hbase.createTable(desc);
            table = new HTable(config, "transactionTable");
        }
    }

    @Override
    public void insert(Transaction trans) throws IOException {

        Put p = new Put(Bytes.toBytes(trans.id));

        p.add(Bytes.toBytes("date"), Bytes.toBytes(""), Bytes.toBytes(trans.date.getTime()));
        p.add(Bytes.toBytes("amount"), Bytes.toBytes(""), Bytes.toBytes(trans.amount));
        p.add(Bytes.toBytes("description"), Bytes.toBytes(""), Bytes.toBytes(trans.description));
        p.add(Bytes.toBytes("accountNumber"), Bytes.toBytes(""), Bytes.toBytes(trans.accountNumber.toString()));
        p.add(Bytes.toBytes("transactionCode"), Bytes.toBytes(""), Bytes.toBytes(trans.transactionCode));
        p.add(Bytes.toBytes("valuteringDate"), Bytes.toBytes(""), Bytes.toBytes(trans.valuteringDate.getTime()));
        p.add(Bytes.toBytes("posteringDate"), Bytes.toBytes(""), Bytes.toBytes(trans.posteringDate.getTime()));
        p.add(Bytes.toBytes("bokforingDate"), Bytes.toBytes(""), Bytes.toBytes(trans.bokforingDate.getTime()));

        //details
        p.add(Bytes.toBytes("details"), Bytes.toBytes("fullDescription"), Bytes.toBytes(trans.fullDescription));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("remoteAccountNumber"), Bytes.toBytes(trans.remoteAccountNumber));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("currencyAmount"), Bytes.toBytes(trans.currencyAmount));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("currencyCode"), Bytes.toBytes(trans.currencyCode));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("isConfidential"), Bytes.toBytes(trans.isConfidential));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("transactionCodeText"), Bytes.toBytes(trans.transactionCodeText));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("batchNumber"), Bytes.toBytes(trans.batchNumber));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("archiveReference"), Bytes.toBytes(trans.archiveReference));
        p.add(Bytes.toBytes("details"), Bytes.toBytes("numbericalReference"), Bytes.toBytes(trans.numbericalReference));
        table.put(p);
    }

    @Override
    public String retrieve(String key, String family, String qualifier) throws IOException {
        Get g = new Get(Bytes.toBytes(key));
        Result r = table.get(g);
        byte[] value = r.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        return Bytes.toString(value);
    }
}