package no.bekk.bigdata;

import no.bekk.bigdata.database.DatabaseClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Generator {
    private final TransactionGenerator generator;

    // User controllable parameters:
    private final boolean logging;
    private final boolean generateTransactions;
    private final boolean dryrun;

    private final int usersToCreate;
    private final int maxAccountsPerUserPm;
    private final int maxAccountsPerUserBm;

    private static final int MAX_ACCOUNT_PREFIXES_PER_USER_PM = 3;
    private static final int MAX_ACCOUNT_PREFIXES_PER_USER_BM = 10;
    private static final int PERCENTAGE_PM_USERS = 85;

    // Parameters for transaction generation
    public final static int MAX_AMOUNT = 2000000;

    private static int wordCount;

    //number of unique descriptions to create
    public final static int DESCRIPTIONS_TO_CREATE = 4000;

    private static int transCodeCount;

    // statistics
    private static long transactionsCreated = 0;
    private static long transactionsPerDay = 0;


    public Generator(boolean logging, boolean generateTransactions, boolean dryrun,
                     long transactionsToGenerate, int startYear, int numberOfYears,
                     int usersToCreate, int maxAccountsPerUserBm, int maxAccountsPerUserPm,
                     DatabaseClient client) throws
                                                                                                       IOException {
        this.logging = logging;
        this.generateTransactions = generateTransactions;
        this.dryrun = dryrun;
        this.usersToCreate = usersToCreate;
        this.maxAccountsPerUserBm = maxAccountsPerUserBm;
        this.maxAccountsPerUserPm = maxAccountsPerUserPm;

        this.generator = new TransactionGenerator(startYear, numberOfYears, transactionsToGenerate, transactionsPerDay, transactionsCreated,
                                            client, dryrun);
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
            generator.generateTransactions();
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


    /**
     * @param numberOfWords Number of words in text string
     * @return a randomly generated text string, containing from 1 to numberOfWords words
     */
    private static String getRandomSentence(int numberOfWords) {
        if (numberOfWords > 1) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numberOfWords; i++) {
                builder.append(Utils.getRandomWord());
                if (i < numberOfWords - 1) {
                    builder.append(' ');
                }
            }
            return builder.toString();
        } else {
            return Utils.getRandomWord();
        }
    }

    private void loadWordlist() throws IOException {
        System.out.println("Loading dictionary");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(Generator.class.getResourceAsStream("/words.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            Utils.words.add(line);
        }
        reader.close();
        wordCount = Utils.words.size();
        if (logging) {
            System.out.printf("%d words loaded\n", wordCount);
        }
    }


    private void loadTransactionCodes() throws IOException {
        System.out.println("Loading transaction codes");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(Generator.class.getResourceAsStream("/transkoder.csv")));
        String line;
        while ((line = reader.readLine()) != null) {
            Utils.transCodes.add(line);
        }
        reader.close();
        transCodeCount = Utils.transCodes.size();

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
            Utils.descriptions.add(getRandomSentence(1 + Utils.random.nextInt(3)));
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
        for (int i = 0; i < Utils.ACCOUNT_PREFIXES_TO_CREATE; i++) {
            Utils.accountPrefixes.add("" + (1000 + Utils.random.nextInt(9000)));
        }
        if (logging) {
            System.out.printf("%d account prefixes created\n", Utils.ACCOUNT_PREFIXES_TO_CREATE);
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
        accountNumber = new CompactCharSequence(prefix + String.valueOf(1000000 + Utils.random.nextInt(9000000)));
//        } while (accounts.contains(accountNumber));

        return accountNumber;
    }

    private void generateUsersAndAccounts() {
        if (logging) {
            System.out.println("Going to create " + usersToCreate + " users");
        }

        for (int i = 0; i < usersToCreate; i++) {
            boolean isPm = Utils.random.nextInt(100) < PERCENTAGE_PM_USERS;

            // for the time being the number of accounts for a user is linearly distributed
            int numberOfAccounts = Utils.random.nextInt(isPm ? maxAccountsPerUserPm : maxAccountsPerUserBm);
            int numberOfAccountPrefixes =
                    (1 + Utils.random.nextInt(isPm ? MAX_ACCOUNT_PREFIXES_PER_USER_PM : MAX_ACCOUNT_PREFIXES_PER_USER_BM));
            int accountsPerPrefix = (int) Math.floor(numberOfAccounts / numberOfAccountPrefixes);

            User user = new User();
            user.id = i;
            user.isPm = isPm;
            user.accounts = new ArrayList<Account>(numberOfAccounts);
            Utils.users.add(user);

            // if (logging) System.out.println("Created " + (isPm ? "PM" : "BM") + " user with id " + user.id);

            // select a number of account prefixes - this is done in order to get 'similar' account numbers for a customer, as a customer
            // seldom has accounts spread between very many banks.
            int accountsLeftToCreate = numberOfAccounts;
            for (int j = 0; j < numberOfAccountPrefixes; j++) {
                String accountPrefix = Utils.accountPrefixes.get(Utils.random.nextInt(Utils.ACCOUNT_PREFIXES_TO_CREATE));
                int accountsToCreate =
                        (accountsLeftToCreate - accountsPerPrefix > 0 ? accountsPerPrefix : accountsLeftToCreate);
                for (int accIndex = 0; accIndex < accountsToCreate; accIndex++) {
                    Account account = new Account();
                    account.isPm = isPm;
                    account.accountNumber = generateAccountNumber(accountPrefix);
                    Utils.accounts.add(account);
                    user.accounts.add(account);
                    //if (logging) System.out.println("Created account " + account.accountNumber);
                }
                accountsLeftToCreate -= accountsToCreate;
            }
            // if (logging) System.out.println("Created " + user.accounts.size() + " accounts for user " + user.id);
            if (i % 1000 == 0) {
                if (logging) {
                    System.out.println(Utils.users.size() + " users and " + Utils.accounts.size() + " accounts created");
                }
            }
        }
        if (logging) {
            System.out.println("Created " + Utils.users.size() + " users and " + Utils.accounts.size() + " accounts");
        }
    }


    private void calculateStatistics() {

        int groupsToCreate = 50;
        int[] transactionDistribution = new int[groupsToCreate];
        for (int i = 0; i < groupsToCreate; i++) {
            transactionDistribution[i] = 0;
        }

        long highestCount = 0;
        for (Account account : Utils.accounts) {
            if (account.numberOfTransactions > highestCount) {
                highestCount = account.numberOfTransactions;
            }
        }
        int transactionsPerGroup = (int) Math.ceil(highestCount / (0.0 + groupsToCreate));

        System.out.println();
        System.out.println("Transactions per group: " + transactionsPerGroup);
        System.out.println("Accounts with transactions: ");
        System.out.println("----------------------------");
        for (Account account : Utils.accounts) {
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
        System.out.printf("Number of users created: %d\n", Utils.users.size());
        System.out.printf("Number of accounts created: %d\n", Utils.accounts.size());
        System.out.printf("Number of transactions created: %d\n", transactionsCreated);
        System.out.printf("Number of transactions per day: %d\n", transactionsPerDay);
        System.out.println("Max transactions in an account: " + highestCount);
    }


    /**
     * Implement this to add users and accounts to database (necessary for statistics etc. later)
     */
    private void storeUsersAndAccounts() {
        //TODO: Add DB code here
    }
}