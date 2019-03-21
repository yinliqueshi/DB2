package query;

import database.Database;
import database.DatabaseException;
import databox.DataBox;
import index.BPlusTree;
import table.Record;
import table.RecordId;
import table.Schema;

import java.util.ArrayList;
import java.util.Iterator;

public class BtreeIndexScanOperator extends QueryOperator {

    public enum SearchType {
        EQUAL,
        GREATER,
        LESS,
        GREATER_OR_EQ,
        LESS_OR_EQ
    }

    public BtreeIndexScanOperator(Database.Transaction transaction,
                                  String tableName,
                                  BPlusTree bPlusTree) throws QueryPlanException, DatabaseException {
        super(OperatorType.INDEXSCAN);
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public Iterator<Record> iterator() throws DatabaseException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public Schema computeSchema() throws QueryPlanException {
        throw new UnsupportedOperationException("TODO: implement");
    }

    @Override
    public Iterator<Record> execute(Object... arguments) throws QueryPlanException, DatabaseException {
        SearchType search_type = (SearchType)arguments[0];
        DataBox val = (DataBox) arguments[1];

        if (search_type == SearchType.EQUAL){
            return find_equal(val);
        }

        return null;
    }

    public Iterator<Record> find_equal(DataBox val) throws DatabaseException{
        throw new UnsupportedOperationException("TODO: implement");
    }
}
