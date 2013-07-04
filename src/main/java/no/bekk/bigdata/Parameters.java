package no.bekk.bigdata;

import no.bekk.bigdata.solr.MultiSolrIndexer;
import no.bekk.bigdata.solr.SolrCloudIndexer;
import no.bekk.bigdata.solr.SolrIndexer;

public class Parameters {
    public static final int DEFAULT_START_YEAR = 2008;
    public static final int DEAFULT_NUMBER_OF_YEARS = 5;
    public static final int DEFAULT_USERS_TO_CREATE = 150; //750 000 i reel base, hvorav 500 000 er aktive.
    public static final int DEFAULT_MAX_ACCOUNTS_PER_USER_BM = 2000;
    public static final int DEFAULT_MAX_ACCOUNTS_PER_USER_PM = 20;
    public static boolean DEFAULT_LOGGING = true;
    public static boolean DEFAULT_GENERATE_TRANSACTIONS = true;
    public static boolean DEAFULT_DRY_RUN = false; // set this to true to generate data without inserting into database
    public static boolean DEFAULT_RESUME = false;
    public static long DEFAULT_TRANSACTIONS_TO_GENERATE = 60000000; //60 millions = 12 millions pr year over 5 years
    public static Class<? extends TransactionSink> DEFAULT_SINK = SolrIndexer.class;
    public static String[] DEFAULT_SOLR_URL = new String[]{"http://localhost:8000/solr"};
    public static String DEFAULT_ZK_HOST = "localhost:9983";


    public boolean logging = DEFAULT_LOGGING;
    public boolean generateTransactions = DEFAULT_GENERATE_TRANSACTIONS;
    public boolean dryrun = DEAFULT_DRY_RUN;
    public boolean resume = DEFAULT_RESUME;
    public long transactionsToGenerate = DEFAULT_TRANSACTIONS_TO_GENERATE;
    public int startYear = DEFAULT_START_YEAR;
    public int numberOfYears = DEAFULT_NUMBER_OF_YEARS;
    public int usersToCreate = DEFAULT_USERS_TO_CREATE;
    public int maxAccountsPerUserPm = DEFAULT_MAX_ACCOUNTS_PER_USER_PM;
    public int maxAccountsPerUserBm = DEFAULT_MAX_ACCOUNTS_PER_USER_BM;
    public Class<? extends TransactionSink> sink = DEFAULT_SINK;
    public String[] solrUrl =  DEFAULT_SOLR_URL;
    public String zkHost = DEFAULT_ZK_HOST;
}
