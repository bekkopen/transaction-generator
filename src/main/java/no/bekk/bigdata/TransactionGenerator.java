package no.bekk.bigdata;

import no.bekk.bigdata.database.DatabaseClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static no.bekk.bigdata.Utils.random;

public class TransactionGenerator {

    private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    
    private int startYear;
    private int numberOfYears;
    private long transactionsToGenerate;
    private long transactionsPerDay;
    private long transactionsCreated;
    private boolean logging =true;
    private DatabaseClient client;
    private boolean dryrun;

    public TransactionGenerator(int startYear, int numberOfYears, long transactionsToGenerate, long transactionsPerDay,
                                long transactionsCreated, DatabaseClient client, boolean dryrun) {
        this.startYear = startYear;
        this.numberOfYears = numberOfYears;
        this.transactionsToGenerate = transactionsToGenerate;
        this.transactionsPerDay = transactionsPerDay;
        this.transactionsCreated = transactionsCreated;
        this.client = client;
        this.dryrun = dryrun;
    }

    void generateTransactions() {

        // calculate the number of days to create transactions for
        Calendar calendar = Calendar.getInstance();
        calendar.set(startYear, Calendar.JANUARY, 1, 0, 0, 0);

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(startYear + numberOfYears, Calendar.JANUARY, 1, 0, 0, 0);

        int days = (int) ((endCalendar.getTimeInMillis() - calendar.getTimeInMillis()) / (1000.0 * 60 * 60 * 24));
        if (logging) {
            System.out.println("Total number of days to create transactions for: " + days);
        }

        transactionsPerDay = (long) Math.floor(transactionsToGenerate / days);

        if (logging) {
            System.out.printf("Transactions to create per month: %d\n", transactionsPerDay);
        }

        long transactions = 0;


        for (int day = 0; day < days; day++) {
            transactions = createTransactionsForOneDay(calendar, transactions);
        }

        if (logging) {
            System.out.println("Finished transaction creation, " + transactionsCreated + " created");
        }
    }

    private long createTransactionsForOneDay(Calendar calendar, long transactions) {
        // log the time it takes to generate one day of data
        long dayStartTime = System.currentTimeMillis();

        // Just for fun
        if (logging && calendar.get(Calendar.DAY_OF_MONTH) == 29 && calendar.get(Calendar.MONTH) == Calendar.FEBRUARY) {
            System.out.println(calendar.get(Calendar.YEAR) + " was a leap year!");
        }

        Date date = calendar.getTime();

        for (int transactionNum = 0; transactionNum < transactionsPerDay; transactionNum++) {
            Transaction transaction = createTransaction(transactions, date,
                                                        Utils.descriptions.get(random.nextInt(
                                                                Generator.DESCRIPTIONS_TO_CREATE)),
                                                        Utils.getRandomAccountNumber(),
                                                        Utils.getNextAccount(),
                                                        Utils.getTransCode());

            if (!dryrun) {
                storeTransaction(transaction);
            }
            transactions++;
        }
        transactionsCreated += transactionsPerDay;

        calendar.add(Calendar.DAY_OF_YEAR, 1);

        long milliseconds = (System.currentTimeMillis() - dayStartTime);
        if (logging) {
            System.out.printf(
                    "%s: Created %d transactions in %d ms (%d transactions per second), total is %d. \n",
                    dateFormatter.format(date),
                    transactionsPerDay,
                    milliseconds,
                    1000 * transactionsPerDay / milliseconds,
                    transactionsCreated);
        }
        return transactions;
    }

    private Transaction createTransaction(long id, Date date, String description, String accountNumber, Account account,
                                          String[] codeAndText) {
        Transaction transaction = new Transaction();
        transaction.id = id;
        transaction.date = date;
        transaction.bokforingDate = date;
        transaction.valuteringDate = date;
        transaction.posteringDate = date;
        transaction.isConfidential = false;

        // Creates amounts where (if MAX_AMOUNT = 2000000):
        // - 10% are small transactions of less than 100 kr (some are 0).
        // - 30% are less than 5000 kr
        // - 50% are less than 55000 kr
        // - about 10% are more than 1M kr.
        BigDecimal amount = new BigDecimal(Generator.MAX_AMOUNT * (Math.pow(0.1 + 0.9 * random.nextDouble(), 6.0)));
        transaction.amount = amount.setScale(2, RoundingMode.HALF_UP);
        transaction.currencyAmount = transaction.amount;
        transaction.currencyCode = "NOK";

        // get a random description. in reality, descriptions will not be randomly spread out as people tend to shop at
        // the same stores etc, but by using a reasonably small number of possible descriptions, we'll still get multiple
        // transactions with the same text for a user.
        transaction.description = description;

        // get a random account number.
        transaction.remoteAccountNumber = accountNumber;

        // add a slightly longer full description as some parts are always removed.
        transaction.fullDescription = Utils.getRandomWord() + " " + transaction.description;

        transaction.transactionCode = codeAndText[0];
        transaction.transactionCodeText = codeAndText[1];

        transaction.batchNumber = "" + (100000000 + random.nextInt(900000000));
        transaction.archiveReference = "" + (100000000 + random.nextInt(900000000));
        transaction.numbericalReference = "" + (100000000 + random.nextInt(900000000));

        //get a non-evenly distributed account number index, some accounts will get a much higher share of the
        //transactions.
        transaction.accountNumber = account.accountNumber;
        account.numberOfTransactions++; // just for statistics, keep track of the number of transactions per account.

        return transaction;
    }


    /**
     * Implement this to add data to database
     * @param transaction
     * @param transactionGenerator
     */
    private void storeTransaction(Transaction transaction) {
        try {
            client.insert(transaction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
