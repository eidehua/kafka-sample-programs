package com.mapr.examples;

import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' or 'task1' or 'task2' as argument");
        }
        switch (args[0]) {
            case "producer":
                Producer.main(args);
                break;
            case "consumer":
                Consumer.main(args);
                break;
            case "task1":
            	Task1.main(args);
            	break;
            case "task2":
            	Task2.main(args);
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}
