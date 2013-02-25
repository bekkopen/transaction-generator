package no.bekk.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class TransactionGenerator {
    private final static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private final HBaseClient client;

    // User controllable parameters:
    private final boolean logging;
    private final boolean generateTransactions;
    private final boolean dryrun;

    private final long transactionsToGenerate;
    private final int startYear;
    private final int numberOfYears;

    private final int usersToCreate;
    private final int maxAccountsPerUserPm;
    private final int maxAccountsPerUserBm;

    private static final int MAX_ACCOUNT_PREFIXES_PER_USER_PM = 3;
    private static final int MAX_ACCOUNT_PREFIXES_PER_USER_BM = 10;
    private static final int PERCENTAGE_PM_USERS = 85;


    // Parameters for transaction generation
    private final static int MAX_AMOUNT = 2000000;

    // word list, used when creating descriptions etc.
    private static List<String> words = new ArrayList<String>();
    private static int wordCount;

    //number of unique descriptions to create
    private final static int DESCRIPTIONS_TO_CREATE = 4000;
    private static List<String> descriptions = new ArrayList<String>(DESCRIPTIONS_TO_CREATE);

    // account number generation
    private final static int ACCOUNT_PREFIXES_TO_CREATE = 50;

    //number of unique account prefixes (account number series)
    private static List<String> accountPrefixes = new ArrayList<String>();

    // transaction codes list, used when creating transaction codes and code descriptions.
    private static List<String> transCodes = new ArrayList<String>();
    private static int transCodeCount;

    // Create a random generator that starts from the same position every time the program is run,
    // which means that all random data will be the same between two runs of the program.
    private Random random = new Random(1234567890l);


    // Generated data
    private final List<Account> accounts = new ArrayList<Account>();
    private final List<User> users;

    // statistics
    private static long transactionsCreated = 0;
    private static long transactionsPerDay = 0;


    public TransactionGenerator(boolean logging, boolean generateTransactions, boolean dryrun,
                                long transactionsToGenerate, int startYear, int numberOfYears,
                                int usersToCreate, int maxAccountsPerUserBm, int maxAccountsPerUserPm) throws
                                                                                                       IOException {
        this.logging = logging;
        this.generateTransactions = generateTransactions;
        this.dryrun = dryrun;
        this.transactionsToGenerate = transactionsToGenerate;
        this.startYear = startYear;
        this.numberOfYears = numberOfYears;
        this.usersToCreate = usersToCreate;
        this.maxAccountsPerUserBm = maxAccountsPerUserBm;
        this.maxAccountsPerUserPm = maxAccountsPerUserPm;
        this.users = new ArrayList<User>(usersToCreate);
        client = new HBaseClient();
    }

    /**
     * Yeah baby, let's overload the system!
     */
    public void goCrazy() throws IOException {
        long startTime = System.currentTimeMillis();

        // Precalculate metadata
        loadWordlist();
        generateDescriptions();
        generateAccountPrefixes();
        loadTransactionCodes();

        // Generate users with accounts (users are only needed to know what accounts to ask for when doing research later
        generateUsersAndAccounts();

        if (generateTransactions) {
            generateTransactions();
        }

        if (!dryrun) {
            storeUsersAndAccounts();
        }

        calculateStatistics();

        long seconds = (System.currentTimeMillis() - startTime) / 1000;
        if (logging) {
            System.out.println("Program completed in " + seconds + " seconds");
        }
    }


    private void generateTransactions() {

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
            // log the time it takes to generate one day of data
            long dayStartTime = System.currentTimeMillis();

            // Just for fun
            if (logging && calendar.get(Calendar.DAY_OF_MONTH) == 29 &&
                    calendar.get(Calendar.MONTH) == Calendar.FEBRUARY) {
                System.out.println(calendar.get(Calendar.YEAR) + " was a leap year!");
            }

            Date date = calendar.getTime();

            for (int transactionNum = 0; transactionNum < transactionsPerDay; transactionNum++) {
                Transaction transaction = createTransaction(transactions, date);
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
        }

        if (logging) {
            System.out.println("Finished transaction creation, " + transactionsCreated + " created");
        }
    }

    private Transaction createTransaction(long id, Date date) {
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
        BigDecimal amount = new BigDecimal(MAX_AMOUNT * (Math.pow(0.1 + 0.9 * random.nextDouble(), 6.0)));
        transaction.amount = amount.setScale(2, RoundingMode.HALF_UP);
        transaction.currencyAmount = transaction.amount;
        transaction.currencyCode = "NOK";

        // get a random description. in reality, descriptions will not be randomly spread out as people tend to shop at
        // the same stores etc, but by using a reasonably small number of possible descriptions, we'll still get multiple
        // transactions with the same text for a user.
        transaction.description = descriptions.get(random.nextInt(DESCRIPTIONS_TO_CREATE));

        // get a random account number.
        transaction.remoteAccountNumber = getRandomAccountNumber();

        // add a slightly longer full description as some parts are always removed.
        transaction.fullDescription = getRandomWord() + " " + transaction.description;

        String[] codeAndText = transCodes.get(random.nextInt(transCodeCount)).split(",");
        transaction.transactionCode = codeAndText[0];
        transaction.transactionCodeText = codeAndText[1];

        transaction.batchNumber = "" + (100000000 + random.nextInt(900000000));
        transaction.archiveReference = "" + (100000000 + random.nextInt(900000000));
        transaction.numbericalReference = "" + (100000000 + random.nextInt(900000000));

        //get a non-evenly distributed account number index, some accounts will get a much higher share of the
        //transactions.
        int accountIndex = getNextAccountIndex(accounts.size());
        Account account = accounts.get(accountIndex);
        transaction.accountNumber = account.accountNumber;
        account.numberOfTransactions++; // just for statistics, keep track of the number of transactions per account.

        return transaction;
    }

    /**
     * @param numberOfWords Number of words in text string
     * @return a randomly generated text string, containing from 1 to numberOfWords words
     */
    private String getRandomSentence(int numberOfWords) {
        if (numberOfWords > 1) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numberOfWords; i++) {
                builder.append(getRandomWord());
                if (i < numberOfWords - 1) {
                    builder.append(' ');
                }
            }
            return builder.toString();
        } else {
            return getRandomWord();
        }
    }

    private String getRandomWord() {
        return words.get(random.nextInt(wordCount));
    }

    private String getRandomAccountNumber() {
        return accountPrefixes.get(random.nextInt(ACCOUNT_PREFIXES_TO_CREATE)) + (1000000 + random.nextInt(9000000));
    }

    private void loadWordlist() throws IOException {
        System.out.println("Loading dictionary");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(TransactionGenerator.class.getResourceAsStream("/words.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            words.add(line);
        }
        reader.close();
        wordCount = words.size();
        if (logging) {
            System.out.printf("%d words loaded\n", wordCount);
        }
    }


    private void loadTransactionCodes() throws IOException {
        System.out.println("Loading transaction codes");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(TransactionGenerator.class.getResourceAsStream("/transkoder.csv")));
        String line;
        while ((line = reader.readLine()) != null) {
            transCodes.add(line);
        }
        reader.close();
        transCodeCount = transCodes.size();

        if (logging) {
            System.out.printf("%d transaction codes loaded\n", transCodeCount);
        }
    }

    /**
     * Create a list of unique transaction texts/descriptions, each containing from 1 to 3 words.
     */
    private void generateDescriptions() {
        System.out.println("Creating descriptions");
        for (int i = 0; i < DESCRIPTIONS_TO_CREATE; i++) {
            descriptions.add(getRandomSentence(1 + random.nextInt(3)));
        }
        if (logging) {
            System.out.printf("%d descriptions created\n", DESCRIPTIONS_TO_CREATE);
        }
    }

    /**
     * Create a list of unique transaction texts/descriptions, each containing from 1 to 3 words.
     */
    private void generateAccountPrefixes() {
        if (logging) {
            System.out.println("Creating account prefixes");
        }
        for (int i = 0; i < ACCOUNT_PREFIXES_TO_CREATE; i++) {
            accountPrefixes.add("" + (1000 + random.nextInt(9000)));
        }
        if (logging) {
            System.out.printf("%d account prefixes created\n", ACCOUNT_PREFIXES_TO_CREATE);
        }
    }

    /**
     * @param prefix first four digits of an account number
     * @return a random account number that starts with prefix.
     */
    private CompactCharSequence generateAccountNumber(String prefix) {
        //TODO: Consider re-adding this check to prevent number collitions.
        // create an unique account number;
        CompactCharSequence accountNumber;
//        do
//        {
        accountNumber = new CompactCharSequence(prefix + String.valueOf(1000000 + random.nextInt(9000000)));
//        } while (accounts.contains(accountNumber));

        return accountNumber;
    }

    private void generateUsersAndAccounts() {
        if (logging) {
            System.out.println("Going to create " + usersToCreate + " users");
        }

        for (int i = 0; i < usersToCreate; i++) {
            boolean isPm = random.nextInt(100) < PERCENTAGE_PM_USERS;

            // for the time being the number of accounts for a user is linearly distributed
            int numberOfAccounts = random.nextInt(isPm ? maxAccountsPerUserPm : maxAccountsPerUserBm);
            int numberOfAccountPrefixes =
                    (1 + random.nextInt(isPm ? MAX_ACCOUNT_PREFIXES_PER_USER_PM : MAX_ACCOUNT_PREFIXES_PER_USER_BM));
            int accountsPerPrefix = (int) Math.floor(numberOfAccounts / numberOfAccountPrefixes);

            User user = new User();
            user.id = i;
            user.isPm = isPm;
            user.accounts = new ArrayList<Account>(numberOfAccounts);
            users.add(user);

            // if (logging) System.out.println("Created " + (isPm ? "PM" : "BM") + " user with id " + user.id);

            // select a number of account prefixes - this is done in order to get 'similar' account numbers for a customer, as a customer
            // seldom has accounts spread between very many banks.
            int accountsLeftToCreate = numberOfAccounts;
            for (int j = 0; j < numberOfAccountPrefixes; j++) {
                String accountPrefix = accountPrefixes.get(random.nextInt(ACCOUNT_PREFIXES_TO_CREATE));
                int accountsToCreate =
                        (accountsLeftToCreate - accountsPerPrefix > 0 ? accountsPerPrefix : accountsLeftToCreate);
                for (int accIndex = 0; accIndex < accountsToCreate; accIndex++) {
                    Account account = new Account();
                    account.isPm = isPm;
                    account.accountNumber = generateAccountNumber(accountPrefix);
                    accounts.add(account);
                    user.accounts.add(account);
                    //if (logging) System.out.println("Created account " + account.accountNumber);
                }
                accountsLeftToCreate -= accountsToCreate;
            }
            // if (logging) System.out.println("Created " + user.accounts.size() + " accounts for user " + user.id);
            if (i % 1000 == 0) {
                if (logging) {
                    System.out.println(users.size() + " users and " + accounts.size() + " accounts created");
                }
            }
        }
        if (logging) {
            System.out.println("Created " + users.size() + " users and " + accounts.size() + " accounts");
        }
    }

    /**
     * Tries to distribute transactions across all accounts in a non-uniform way. Returns the index in the arrays list that the
     * transaction should be added to.
     */
    public int getNextAccountIndex(int numberOfAccounts) {
//        double scaleFactor = 1.02041;
        double scaleFactor = 1;
        double rand = random.nextDouble();

        int index = (int) Math.floor(numberOfAccounts * scaleFactor * (1 - 1 / (50 * (0.02 + rand))));
        if (index >= numberOfAccounts) {
            return numberOfAccounts - 1;
        } else {
            return index;
        }
    }

    private void calculateStatistics() {

        int groupsToCreate = 50;
        int[] transactionDistribution = new int[groupsToCreate];
        for (int i = 0; i < groupsToCreate; i++) {
            transactionDistribution[i] = 0;
        }

        long highestCount = 0;
        for (Account account : accounts) {
            if (account.numberOfTransactions > highestCount) {
                highestCount = account.numberOfTransactions;
            }
        }
        int transactionsPerGroup = (int) Math.ceil(highestCount / (0.0 + groupsToCreate));

        System.out.println();
        System.out.println("Transactions per group: " + transactionsPerGroup);
        System.out.println("Accounts with transactions: ");
        System.out.println("----------------------------");
        for (Account account : accounts) {
            int groupIndex = (int) Math.floor((account.numberOfTransactions) / transactionsPerGroup);
            transactionDistribution[groupIndex]++;
        }

        for (int i = 0; i < groupsToCreate; i++) {
            System.out.printf("Accounts with %d to %d transactions: %d\n",
                              i * transactionsPerGroup,
                              (i + 1) * transactionsPerGroup,
                              transactionDistribution[i]
            );
            transactionDistribution[i] = 0;
        }

        System.out.println();
        System.out.printf("Number of users created: %d\n", users.size());
        System.out.printf("Number of accounts created: %d\n", accounts.size());
        System.out.printf("Number of transactions created: %d\n", transactionsCreated);
        System.out.printf("Number of transactions per day: %d\n", transactionsPerDay);
        System.out.println("Max transactions in an account: " + highestCount);
    }


    /**
     * Implement this to add data to database
     */
    private void storeTransaction(Transaction transaction) {
        try {
            client.insert(transaction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implement this to add users and accounts to database (necessary for statistics etc. later)
     */
    private void storeUsersAndAccounts() {
        //TODO: Add DB code here
    }

}
