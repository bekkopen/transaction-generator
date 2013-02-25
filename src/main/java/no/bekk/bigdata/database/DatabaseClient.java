package no.bekk.bigdata.database;

import no.bekk.bigdata.Transaction;

import java.io.IOException;

public interface DatabaseClient {

    void insert(Transaction trans) throws IOException;

    String retrieve(String key, String family, String qualifier) throws IOException;
}
