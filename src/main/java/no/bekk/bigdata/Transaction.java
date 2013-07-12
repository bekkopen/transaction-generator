package no.bekk.bigdata;

import java.math.BigDecimal;
import java.util.Date;

public class Transaction
    {
        public long id;
        public Date date;
        public BigDecimal amount;
        public String description;
        public String remoteAccountNumber;
        public BigDecimal currencyAmount;
        public String currencyCode;
        public boolean isConfidential;
        public CompactCharSequence accountNumber;
        public String fullDescription;
        public String transactionCodeText;
        public String transactionCode;
        public Date valuteringDate;
        public Date posteringDate;
        public Date bokforingDate;
        public String batchNumber;
        public String archiveReference;
        public String numbericalReference;

        @Override
        public String toString()
        {
            return "Transaction{" +
                    "id=" + id +
                    "\n date=" + date +
                    "\n amount=" + amount +
                    "\n description='" + description + '\'' +
                    "\n remoteAccountNumber='" + remoteAccountNumber + '\'' +
                    "\n currencyAmount=" + currencyAmount +
                    "\n currencyCode='" + currencyCode + '\'' +
                    "\n isConfidential=" + isConfidential +
                    "\n accountNumber='" + accountNumber + '\'' +
                    "\n fullDescription='" + fullDescription + '\'' +
                    "\n transactionCodeText='" + transactionCodeText + '\'' +
                    "\n transactionCode='" + transactionCode + '\'' +
                    "\n valuteringDate=" + valuteringDate +
                    "\n posteringDate=" + posteringDate +
                    "\n bokforingDate=" + bokforingDate +
                    "\n batchNumber='" + batchNumber + '\'' +
                    "\n archiveReference='" + archiveReference + '\'' +
                    "\n numbericalReference='" + numbericalReference + "\'\n" +
                    '}';
        }

        public String getAccountNumber() {
            return accountNumber.toString();
        }

        public String toElasticJSON()
        {
            return '{' +
                    "\"id\":" + id +
                    ",\"date\":" + date.getTime() +
                    ",\"amount\":" + amount +
                    ",\"description\":\"" + description + '"' +
                    ",\"remoteAccountNumber\":" + remoteAccountNumber +
                    ",\"currencyAmount\":" + currencyAmount +
                    ",\"currencyCode\":\"" + currencyCode + '"' +
                    ",\"isConfidential\":" + isConfidential +
    //                ",\"fullDescription\":\"" + fullDescription + '"' + // this is just desc + date and some numbers
    //                ",\"transactionCodeText\":\"" + transactionCodeText + '"' + //can be 1 to 1 mapping to transCode
                    ",\"transactionCode\":\"" + transactionCode + '"' +
                    ",\"valuteringDate\":" + valuteringDate.getTime() +
                    ",\"posteringDate\":" + posteringDate.getTime() +
                    ",\"bokforingDate\":" + bokforingDate.getTime() +
                    ",\"batchNumber\":" + batchNumber +
                    ",\"archiveReference\":" + archiveReference +
                    ",\"numbericalReference\":" + numbericalReference +
                    ",\"accountNumber\":" + accountNumber +
                    '}';
        }
    }