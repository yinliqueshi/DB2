package query;

import database.Database;
import database.DatabaseException;
import table.Record;

import java.util.Iterator;

public class INLJOperator extends JoinOperator {

    public INLJOperator(QueryOperator leftSource,
                        BtreeIndexScanOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.INLJ);

    }

    @Override
    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        throw new UnsupportedOperationException("TODO: implement");
    }

}
