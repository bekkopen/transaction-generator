package no.bekk.bigdata;

import no.bekk.bigdata.database.HBaseClient;

import java.io.IOException;

public class Main {
    private static boolean logging = true;
    private static boolean generateTransactions = true;
    private static boolean dryrun = false; // set this to true to generate data without inserting into database
    private static long transactionsToGenerate = 60000000; //60 millions = 12 millions pr year over 5 years
    private static int startYear = 2008;
    private static int numberOfYears = 5;
    private static int usersToCreate = 150; //750 000 i reel base, hvorav 500 000 er aktive.
    private static int maxAccountsPerUserPm = 20;
    private static int maxAccountsPerUserBm = 2000;

    private static void printHelp() {
        System.out.println("Possible parameters:");
        System.out.println(" --logging=off        --- turn off logging to console");
        System.out.println(" --generate=off       --- disable transaction generation");
        System.out.println(" --dryrun=on          --- dryrun, creates data but doesn't store to database");
        System.out.println(" --transcount=XXXXXX  --- number of transactions (in total) to generate, defaults to "
                                   + transactionsToGenerate);
        System.out.println(" --startyear=XXXX     --- start year, defaults to " + startYear);
        System.out.println(" --yearcount=X        --- number of years to generate data for, defaults to "
                                   + numberOfYears);
        System.out.println(" --usercount=XXXXXXX  --- number of users to create, defaults to " + usersToCreate);
        System.out.println(" --maxaccbm=XXXXXXX   --- max number of accounts per bm user, defaults to "
                                   + maxAccountsPerUserBm);
        System.out.println(" --maxaccpm=XXXXXXX   --- max number of accounts per pm user, defaults to "
                                   + maxAccountsPerUserPm);
    }

    public static void main(String args[]) throws IOException {
        System.out.println("Usage: start with --help to get parameter list");

        for (String param : args) {
            String value = param.contains("=") ? param.split("=")[1] : "";

            if (param.startsWith("--help")) {
                printHelp();
                return;
            }

            if (param.startsWith("--logging")) {
                if ("off".equals(value)) {
                    logging = false;
                }
            }

            if (param.startsWith("--generate")) {
                if ("off".equals(value)) {
                    generateTransactions = false;
                }
            }

            if (param.startsWith("--dryrun")) {
                if ("on".equals(value)) {
                    dryrun = true;
                }
            }

            if (param.startsWith("--transcount")) {
                transactionsToGenerate = Integer.parseInt(value);
            }

            if (param.startsWith("--startyear")) {
                startYear = Integer.parseInt(value);
            }

            if (param.startsWith("--yearcount")) {
                numberOfYears = Integer.parseInt(value);
            }

            if (param.startsWith("--usercount")) {
                usersToCreate = Integer.parseInt(value);
            }

            if (param.startsWith("--maxaccbm")) {
                maxAccountsPerUserBm = Integer.parseInt(value);
            }

            if (param.startsWith("--maxaccpm")) {
                maxAccountsPerUserPm = Integer.parseInt(value);
            }
        }

        TransactionGenerator generator = new TransactionGenerator(logging, generateTransactions, dryrun,
                                                                  transactionsToGenerate, startYear, numberOfYears,
                                                                  usersToCreate, maxAccountsPerUserBm,
                                                                  maxAccountsPerUserPm, new HBaseClient());
        generator.goCrazy();
    }
}