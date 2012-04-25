package com.kg6sed.dynamodb.cli;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodb.model.*;
import com.kg6sed.eggshell.AbstractShell;
import com.kg6sed.eggshell.Command;
import com.kg6sed.eggshell.ExitShellException;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Map;

/**
 * DynamoDB CLI
 */
public class DynamoDbCli extends AbstractShell {
    private AmazonDynamoDBClient client;

    private String tableName;
    private KeySchema keySchema;

    public DynamoDbCli() throws IOException {
        super();
        this.client = new AmazonDynamoDBClient(new PropertiesCredentials(new File(System.getProperty("user.home"), ".aws-credentials")));
        this.tableName = "";
    }

    @Command
    private void listTables() throws IOException {
        ListTablesResult result = this.client.listTables();

        if (result.getTableNames().size() > 0) {
            for (String tableName : result.getTableNames()) {
                this.console.printString(tableName);
                this.console.printNewline();
            }
        } else {
            this.console.printString("No tables found.");
        }
        this.console.printNewline();
    }

    @Override
    protected String generatePrompt() {
        return String.format("[%s] > ", tableName);
    }

    @Command
    private void describe() throws IOException {
        checkTable();
        DescribeTableResult describeTableResult = this.client.describeTable(new DescribeTableRequest().withTableName(this.tableName));
        this.console.printString(describeTableResult.toString());
        this.console.printNewline();
    }

    @Command
    private void use(String tableName) throws IOException {
        DescribeTableResult describeTableResult = this.client.describeTable(new DescribeTableRequest().withTableName(tableName));
        this.tableName = describeTableResult.getTable().getTableName();
        this.keySchema = describeTableResult.getTable().getKeySchema();
        this.console.printString(String.format("Using table %s.", this.tableName));
        this.console.printNewline();
    }


    @Command
    private void scan() throws IOException {
        checkTable();

        ScanResult result = this.client.scan(new ScanRequest().withTableName(this.tableName));

        for (Map<String, AttributeValue> item : result.getItems()) {
            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                StringBuilder builder = new StringBuilder(entry.getKey()).append(": ");
                builder.append(entry.getValue().toString()).append(", ");
                this.console.printString(builder.toString());
            }
            this.console.printNewline();
        }

        this.console.printNewline();
    }

    private void checkTable() throws MessageException {
        if (tableName.length() < 1) {
            throw new MessageException("No table selected, please select one first (use command).");
        }
    }

    @Command
    private void deleteTable() throws IOException {
        checkTable();

        DeleteTableResult result = this.client.deleteTable(new DeleteTableRequest().withTableName(this.tableName));

        // if table was properly deleted, we will get here (no exception), so set table to undefined
        this.console.printString(String.format("Table %s was successfully deleted.", result.getTableDescription().getTableName()));
        this.tableName = "";

        this.console.printNewline();
    }


    @Command
    private void getItem(String hashKey, String rangeKey) throws IOException {
        //  GetItemResult result = this.client.getItem(new GetItemRequest().withTableName(tableName).withKey(new Key().));
        checkTable();
        GetItemRequest request = new GetItemRequest().withTableName(this.tableName);


        this.console.printString(this.keySchema.getHashKeyElement().toString());
        this.console.printString(this.keySchema.getRangeKeyElement().toString());

        this.console.printNewline();
        // this.keySchema.getHashKeyElement().getAttributeName();

    }

    /**
     * Quit the shell.
     */
    @Command
    private void quit() {
        throw new ExitShellException("Goodbye.");
    }


    public static void main(String[] args) throws Exception {

        DynamoDbCli dynamoTool = new DynamoDbCli();
        dynamoTool.run();

    }

    private static class MessageException extends IllegalArgumentException {

        private MessageException(String s) {
            super(s);
        }

        @Override
        public String toString() {
            return this.getMessage();
        }

        @Override
        public void printStackTrace() {
            System.err.println(this.getMessage());
        }

        @Override
        public void printStackTrace(PrintStream printStream) {
            printStream.println(this.getMessage());
        }

        @Override
        public void printStackTrace(PrintWriter printWriter) {
            printWriter.println(this.getMessage());
        }

    }


}
