package no.bekk.bigdata.elasticsearch;

import com.sun.xml.bind.v2.TODO;
import no.bekk.bigdata.Parameters;
import no.bekk.bigdata.Transaction;
import no.bekk.bigdata.TransactionSink;
import org.codehaus.jackson.map.ObjectMapper;

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
            flush();
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
        /*
        int port = 9200;
        String host = "bigdata01.dev.bekk.no";
        String msg = "";

        URL url = new URL("http://"+host+":"+port);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(conn.getOutPutStream());
        out.write(msg);
        out.close();
         */
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
        String buf = "{";
        for (Transaction t : trans) {
            buf += "" + t.id + ":" + transactionToJSON(t);
        }
        buf += "}";
        System.out.print(buf);
        return true;
    }
}
