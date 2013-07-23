package no.bekk.bigdata;

import no.bekk.bigdata.csv.CSVSink;
import no.bekk.bigdata.elasticsearch.ElasticSearchSink;
import no.bekk.bigdata.solr.MultiSolrIndexer;
import no.bekk.bigdata.solr.SolrCloudIndexer;
import no.bekk.bigdata.solr.SolrIndexer;

import java.io.IOException;

import static no.bekk.bigdata.Parameters.*;

public class Main {
    private static boolean debug = false;

    public static boolean debug() {
        return debug;
    }


    private static void printHelp() {
        System.out.println("Possible parameters:");
        System.out.println(" --logging=off        --- turn off logging to console");
        System.out.println(" --generate=off       --- disable transaction generation");
        System.out.println(" --dryrun=on          --- dryrun, creates data but doesn't store to database");
        System.out.println(" --transcount=XXXXXX  --- number of transactions (in total) to generate, defaults to "
                                   + DEFAULT_TRANSACTIONS_TO_GENERATE);
        System.out.println(" --startyear=XXXX     --- start year, defaults to " + DEFAULT_START_YEAR);
        System.out.println(" --yearcount=X        --- number of years to generate data for, defaults to "
                                   + DEAFULT_NUMBER_OF_YEARS);
        System.out.println(" --usercount=XXXXXXX  --- number of users to create, defaults to " + DEFAULT_USERS_TO_CREATE);
        System.out.println(" --maxaccbm=XXXXXXX   --- max number of accounts per bm user, defaults to "
                                   + DEFAULT_MAX_ACCOUNTS_PER_USER_BM);
        System.out.println(" --maxaccpm=XXXXXXX   --- max number of accounts per pm user, defaults to "
                                   + DEFAULT_MAX_ACCOUNTS_PER_USER_PM);
        System.out.println(" --resume=on  --- Resumes previous session by retrieving random generator from disk. Defaults to off.");

        System.out.println(" --sink=hbase|solr|solrcloud|multisolr|elasticsearch  ---- where to output transactions, defaults to hbase");
        System.out.println(" --solrUrl=URL        ---- URL to solr when solr is set as sink. comma-seperated for sink=multisolr Defaults to " + DEFAULT_SOLR_URL);
        System.out.println(" --host=URL         ---- URL to zookeeper when solrcloud/elasticsearch is set as sink. Defaults to " + DEFAULT_SOLR_URL);
    }

    public static void main(String args[]) throws IOException, IllegalAccessException, InstantiationException {
        System.out.println("Usage: start with --help to get parameter list");
        Parameters parameters = new Parameters();
        for (String param : args) {
            String value = param.contains("=") ? param.split("=")[1] : "";

            if (param.startsWith("--help")) {
                printHelp();
                return;
            }

            if (param.startsWith("--logging")) {
                if ("off".equals(value)) {
                    parameters.logging = false;
                }
            }

            if (param.startsWith("--generate")) {
                if ("off".equals(value)) {
                    parameters.generateTransactions = false;
                }
            }

            if (param.startsWith("--dryrun")) {
                if ("on".equals(value)) {
                    parameters.dryrun = true;
                }
            }

            if (param.startsWith("--transcount")) {
                parameters.transactionsToGenerate = Integer.parseInt(value);
            }

            if (param.startsWith("--startyear")) {
                parameters.startYear = Integer.parseInt(value);
            }

            if (param.startsWith("--yearcount")) {
                parameters.numberOfYears = Integer.parseInt(value);
            }

            if (param.startsWith("--usercount")) {
                parameters.usersToCreate = Integer.parseInt(value);
            }

            if (param.startsWith("--maxaccbm")) {
                parameters.maxAccountsPerUserBm = Integer.parseInt(value);
            }

            if (param.startsWith("--maxaccpm")) {
                parameters.maxAccountsPerUserPm = Integer.parseInt(value);
            }

            if(param.startsWith("--sink")){
                switch(value){
                    case "solr":
                        parameters.sink = SolrIndexer.class;
                        break;
                    case "solrcloud":
                        parameters.sink = SolrCloudIndexer.class;
                        break;
                    case "multisolr":
                        parameters.sink = MultiSolrIndexer.class;
                        break;
                    case "elasticsearch":
                        parameters.sink = ElasticSearchSink.class;
                        break;
                    case "csv":
                        parameters.sink = CSVSink.class;
                        break;
                }

            }

            if (param.equals("--debug")) {
                debug = true;
            }

            if (param.startsWith("--resume")) {
                if ("on".equals(value))
                    parameters.resume = true;
            }

            if (param.startsWith("--host")) {
                parameters.host = value.trim();
            }

            if (param.startsWith("--solrUrl")) {
                parameters.solrUrl = value.trim().split(",");
            }

            if (param.startsWith("--esindexformat")) {
                parameters.esindexformat = value.trim();
            }
            
            if (param.startsWith("--clustername")) {
                parameters.clusterName = value.trim();
            }

        }
        TransactionSink sink = parameters.sink.newInstance();
        sink.setParameters(parameters);
        Generator generator = new Generator(parameters, sink);
        generator.goCrazy();
    }
}