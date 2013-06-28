package no.bekk.bigdata;

import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

public class Utils

{
    // Create a random generator that starts from the same position every time the program is run,
    // which means that all random data will be the same between two runs of the program.
    static Random random = new Random(1234567890l); //may be restored from disk
    static Calendar restoredCalendar = null; //calendar read from disk
    static long restoredCounter = 0;
    // word list, used when creating descriptions etc.
    static List<String> words = new ArrayList<String>();

    //number of unique account prefixes (account number series)
    static List<String> accountPrefixes = new ArrayList<String>();

    // transaction codes list, used when creating transaction codes and code descriptions.
    static List<String> transCodes = new ArrayList<String>();

    // Generated data
    static final List<Account> accounts = new ArrayList<Account>();
    static final List<User> users = new ArrayList<User>();
    static List<String> descriptions = new ArrayList<String>(Generator.DESCRIPTIONS_TO_CREATE);

    // account number generation
    final static int ACCOUNT_PREFIXES_TO_CREATE = 50;

    static String getRandomAccountNumber() {
        return accountPrefixes.get(random.nextInt(ACCOUNT_PREFIXES_TO_CREATE)) + (1000000 + random.nextInt(9000000));
    }

    public static Account getNextAccount() {
        return accounts.get(getNextAccountIndex(accounts.size()));
    }

    /**
     * Tries to distribute transactions across all accounts in a non-uniform way. Returns the index in the arrays list that the
     * transaction should be added to.
     */
    private static int getNextAccountIndex(int numberOfAccounts) {
//        double scaleFactor = 1.02041;
        double scaleFactor = 1;
        double rand = Utils.random.nextDouble();

        int index = (int) Math.floor(numberOfAccounts * scaleFactor * (1 - 1 / (50 * (0.02 + rand))));
        if (index >= numberOfAccounts) {
            return numberOfAccounts - 1;
        } else {
            return index;
        }
    }

    public static String[] getTransCode() {
        return transCodes.get(random.nextInt(transCodes.size())).split(",");
    }

    static String getRandomWord() {
        return words.get(random.nextInt(words.size()));
    }

    public static void saveGeneratorData(Calendar calendar, long transactions) {
        try {
            FileOutputStream file = new FileOutputStream("savedRandom.obj");
            ObjectOutputStream objStream = new ObjectOutputStream(file);
            objStream.writeObject(random);
            objStream.writeObject(calendar);
            objStream.writeLong(transactions);
            objStream.flush();
            objStream.close();
            file.flush();
            file.close();
        }catch (Exception e) {
            System.out.println("Failed to save random-generator/calendar!");
        }
    }

    public static void restoreGeneratorData() throws RuntimeException{
        try {
            FileInputStream file = new FileInputStream("savedRandom.obj");
            ObjectInputStream objStream = new ObjectInputStream(file);
            random = (Random) objStream.readObject();
            restoredCalendar = (Calendar) objStream.readObject();
            restoredCounter = objStream.readLong();
            objStream.close();
            file.close();
            System.out.println("Restored generator data from disk!");
        }catch (Exception e) {
            System.out.println("Failed to retrieve generator data! Starting with set seed.");
            throw new RuntimeException("Failed to resume last run! Aborting...");
        }
    }
}
