package no.bekk.bigdata;

public class Account
{
    public boolean isPm;
    public CompactCharSequence accountNumber;
    long numberOfTransactions = 0;

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Account account = (Account) o;

        if (accountNumber != null ? !accountNumber.equals(account.accountNumber) : account.accountNumber != null) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        return accountNumber != null ? accountNumber.hashCode() : 0;
    }
}
