package simpledb;

import simpledb.transaction.TransactionId;

public class Debug {
    public static void printTxn(TransactionId tid, String message) {
        System.out.printf("Tid: %s, %s\n", tid, message);
    }
}

