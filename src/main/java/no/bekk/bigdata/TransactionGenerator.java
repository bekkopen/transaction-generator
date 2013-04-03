package no.bekk.bigdata;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static no.bekk.bigdata.Utils.random;

public class TransactionGenerator {

    private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    
    private long transactionsPerDay;
    private long transactionsCreated;
    private final Parameters parameters;
    private final TransactionSink sink;

    public TransactionGenerator(Parameters parameters, TransactionSink sink, long transactionsPerDay, long transactionsCreated) {
        this.parameters = parameters;
        this.sink = sink;
        this.transactionsPerDay = transactionsPerDay;
        this.transactionsCreated = transactionsCreated;
    }

    void generateTransactions() {

        // calculate the number of days to create transactions for
        Calendar calendar = Calendar.getInstance();
        calendar.set(parameters.startYear, Calendar.JANUARY, 1, 0, 0, 0);

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.set(parameters.startYear + parameters.numberOfYears, Calendar.JANUARY, 1, 0, 0, 0);

        int days = (int) ((endCalendar.getTimeInMillis() - calendar.getTimeInMillis()) / (1000.0 * 60 * 60 * 24));
        if (parameters.logging) {
            System.out.println("Total number of days to create transactions for: " + days);
        }

        transactionsPerDay = (long) Math.floor(parameters.transactionsToGenerate / days);

        if (parameters.logging) {
            System.out.printf("Transactions to create per month: %d\n", transactionsPerDay);
        }

        long transactions = 0;

        long startTime = System.currentTimeMillis();
        for (int day = 0; day < days; day++) {
            transactions = createTransactionsForOneDay(calendar, transactions);
        }
        long runtTime = System.currentTimeMillis() - startTime;

        if (parameters.logging) {
            System.out.println(String.format("Finished transaction creation, %d created in %d s. Closing sink...", transactionsCreated, runtTime));
        }
        sink.close();
        if (parameters.logging) {
            System.out.println("Sink closed");
        }
    }

    private long createTransactionsForOneDay(Calendar calendar, long transactions) {
        // log the time it takes to generate one day of data
        long dayStartTime = System.currentTimeMillis();

        // Just for fun
        if (parameters.logging && calendar.get(Calendar.DAY_OF_MONTH) == 29 && calendar.get(Calendar.MONTH) == Calendar.FEBRUARY) {
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

            if (!parameters.dryrun) {
                storeTransaction(transaction);
            }
            transactions++;
        }
        transactionsCreated += transactionsPerDay;

        calendar.add(Calendar.DAY_OF_YEAR, 1);

        long milliseconds = (System.currentTimeMillis() - dayStartTime);
        if(milliseconds == 0){
            milliseconds++;
        }
        if (parameters.logging) {
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
     */
    private void storeTransaction(Transaction transaction) {
            sink.insert(transaction);
    }
}
