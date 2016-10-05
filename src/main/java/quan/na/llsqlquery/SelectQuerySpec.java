package quan.na.llsqlquery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SelectQuerySpec {
    private static Logger log = Logger.getLogger(SelectQuerySpec.class.getName());
    private enum JoinType{
        ROOT, INNER, LEFT, RIGHT
    }

    private static class Table {
        private CharSequence table;
        private String alias;
        private JoinType joinType;
        private List<String> requiredTables;
        private List<CharSequence> onConditions;

        public Table(CharSequence table, String alias, JoinType joinType) {
            this.table = table;
            this.alias = alias;
            this.joinType = joinType;
            this.requiredTables = new ArrayList<>();
            this.onConditions = new ArrayList<>();
        }

        public CharSequence table() {
            return this.table;
        }

        public String alias() {
            return this.alias;
        }

        public JoinType joinType() {
            return this.joinType;
        }

        public List<String> requiredTables() {
            return this.requiredTables;
        }

        public List<CharSequence> onConditions() {
            return this.onConditions;
        }
    }

    private String currentTable;
    private String rootTable;
    private Map<String, Table> tables;
    private Map<String, CharSequence> fields;
    private Map<String, String> fieldProjects;
    private Map<String, CharSequence> conditions;
    private Map<String, String> conditionProjects;

    public SelectQuerySpec() {
        currentTable = null;
        rootTable = null;
        tables = new HashMap<>();
        fields = new HashMap<>();
        fields = new HashMap<>();
        fieldProjects = new HashMap<>();
        conditions = new HashMap<>();
        conditionProjects = new HashMap<>();
    }

    public SelectQuerySpec from(CharSequence table, String tableAlias) {
        if (null == table || null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == rootTable) {
            rootTable = tableAlias;
            tables.put(tableAlias, new Table(table, tableAlias, JoinType.ROOT));
            currentTable = tableAlias;
        } else
            throw new IllegalStateException("from(table,alias) is called more than once.");
        return this;
    }

    public SelectQuerySpec innerJoin(CharSequence table, String tableAlias) {
        if (null == table || null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null != rootTable) {
            if (null == tables.get(tableAlias))
                tables.put(tableAlias, new Table(table, tableAlias, JoinType.INNER));
            else
                throw new IllegalStateException(tableAlias + " is specified more than once.");
            tables.get(tableAlias).requiredTables().add(currentTable);
            currentTable = tableAlias;
        } else
            throw new IllegalStateException("innerJoin() is called before from().");
        return this;
    }

    public SelectQuerySpec leftJoin(CharSequence table, String tableAlias) {
        if (null == table || null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null != rootTable) {
            if (null == tables.get(tableAlias))
                tables.put(tableAlias, new Table(table, tableAlias, JoinType.LEFT));
            else
                throw new IllegalStateException(tableAlias + " is specified more than once.");
            tables.get(tableAlias).requiredTables().add(currentTable);
            currentTable = tableAlias;
        } else
            throw new IllegalStateException("leftJoin() is called before from().");
        return this;
    }

    public SelectQuerySpec rightJoin(CharSequence table, String tableAlias) {
        if (null == table || null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null != rootTable) {
            if (null == tables.get(tableAlias))
                tables.put(tableAlias, new Table(table, tableAlias, JoinType.RIGHT));
            else
                throw new IllegalStateException(tableAlias + " is specified more than once.");
            tables.get(tableAlias).requiredTables().add(currentTable);
            currentTable = tableAlias;
        } else
            throw new IllegalStateException("innerJoin() is called before from().");
        return this;
    }

    public SelectQuerySpec on(CharSequence condition)
    {
        if (null == condition)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == currentTable)
            throw new IllegalStateException("on() is called before from().");
        Table table = tables.get(currentTable);
        if (table.joinType() == JoinType.ROOT)
            throw new IllegalStateException("on() is called on root table.");
        table.onConditions().add(condition);
        return this;
    }

    public SelectQuerySpec on(List<CharSequence> conditions) {
        if (null == conditions || conditions.contains(null))
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == currentTable)
            throw new IllegalStateException("on() is called before from().");
        Table table = tables.get(currentTable);
        if (table.joinType() == JoinType.ROOT)
            throw new IllegalStateException("on() is called on root table.");
        table.onConditions().addAll(conditions);
        return this;
    }

    public SelectQuerySpec select(String columnAlias, CharSequence value)
    {
        if (null == columnAlias || null == value)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == currentTable)
            throw new IllegalStateException("select() is called before from().");
        if (null != fields.get(columnAlias))
            throw new IllegalStateException(columnAlias + " is specified more than once.");
        fields.put(columnAlias, value);
        fieldProjects.put(columnAlias, currentTable);
        return this;
    }

    public SelectQuerySpec where(String columnAlias, CharSequence condition) {
        if (null == columnAlias || null == condition)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == currentTable)
            throw new IllegalStateException("select() is called before from().");
        if (null != conditions.get(columnAlias))
            throw new IllegalStateException(columnAlias + " is specified more than once.");
        conditions.put(columnAlias, condition);
        conditionProjects.put(columnAlias, currentTable);
        return this;
    }

    public SelectQuerySpec from(String tableAlias) {
        if (null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == tables.get(tableAlias))
            throw new IllegalStateException(tableAlias + " is not specified.");
        currentTable = tableAlias;
        return this;
    }

    public SelectQuerySpec join(String tableAlias) {
        if (null == tableAlias)
            throw new IllegalArgumentException("arguments can not be null.");
        if (null == currentTable)
            throw new IllegalStateException("join() is called before from().");
        if (null == tables.get(tableAlias))
            throw new IllegalStateException(tableAlias + " is not specified.");
        tables.get(tableAlias).requiredTables().add(currentTable);
        currentTable = tableAlias;
        return this;
    }

    public SelectQueryBuilder builder() {
        return new SelectQueryBuilder();
    }

    public class SelectQueryBuilder{
        private List<String> selects = new ArrayList<>();
        private List<String> wheres = new ArrayList<>();
        private List<String> groups = new ArrayList<>();
        private List<String> sorts = new ArrayList<>();
        String limitAlias = null;
        String offsetAlias = null;
        String openAnd = "{{and";
        String openOr = "{{or";
        String close = "}}";

        public SelectQueryBuilder parentheses(String openAnd, String openOr, String close) {
            this.openAnd = openAnd;
            this.openOr = openOr;
            this.close = close;
            return this;
        }

        public SelectQueryBuilder selects(List<String> columnAliases) {
            this.selects.addAll(columnAliases);
            return this;
        }

        public SelectQueryBuilder selects(String columnAlias) {
            this.selects.add(columnAlias);
            return this;
        }

        public SelectQueryBuilder where(List<String> columnAliases) {
            this.wheres.addAll(columnAliases);
            return this;
        }

        public SelectQueryBuilder where(String columnAlias) {
            this.wheres.add(columnAlias);
            return this;
        }

        public SelectQueryBuilder group(List<String> columnAliases) {
            this.groups.addAll(columnAliases);
            return this;
        }

        public SelectQueryBuilder group(String columnAlias) {
            this.groups.add(columnAlias);
            return this;
        }

        public SelectQueryBuilder sort(List<String> sortSpecs) {
            this.sorts.addAll(sortSpecs);
            return this;
        }

        public SelectQueryBuilder sort(String sortSpec) {
            this.sorts.add(sortSpec);
            return this;
        }

        public SelectQueryBuilder paging(String limitAlias, String offsetAlias) {
            this.limitAlias = limitAlias;
            this.offsetAlias = offsetAlias;
            return this;
        }

        public String build() {
            if (null == rootTable) {
                log.log(Level.WARNING, "No root table specified.");
                return "SELECT 1";
            }
            List<Table> joinedTables = new ArrayList<>();
            // resolve selected items
            StringBuilder selectBuilder = new StringBuilder("SELECT ");
            if (null == selects || selects.isEmpty()) {
                log.log(Level.WARNING, "No selection specified.");
                selectBuilder.append("1 ");
            } else {
                boolean hasSelect = false;
                for (String select : selects) {
                    if (null != fields.get(select)) {
                        hasSelect = true;
                        selectBuilder.append(fields.get(select)).append(" AS ").append(select).append(", ");
                        resolveJoinedTable(fieldProjects.get(select), joinedTables);
                    } else
                        log.log(Level.WARNING, "No selection specified for " + select);
                }
                if (!hasSelect) {
                    log.log(Level.WARNING, "No effective selection specified.");
                    selectBuilder.append("1 ");
                } else
                    selectBuilder.deleteCharAt(selectBuilder.length() - 2); // remove last colon
            }
            // resolve conditional items
            StringBuilder whereBuilder = new StringBuilder("WHERE ");
            if (null == wheres || wheres.isEmpty()) {
                log.log(Level.WARNING, "No condition specified.");
                whereBuilder.append("true ");
            } else {
                boolean hasWhere = false;
                boolean emptyParenthese = false;
                Stack<String> operantStack = new Stack<>();
                String currentOperant = " AND ";
                for (String where : wheres) {
                    if (openAnd.equals(where)) {
                        whereBuilder.append("(");
                        emptyParenthese = true;
                        operantStack.push(currentOperant);
                        currentOperant = " AND ";
                    } else if (openOr.equals(where)) {
                        whereBuilder.append("(");
                        emptyParenthese = true;
                        operantStack.push(currentOperant);
                        currentOperant = " OR  ";
                    } else if (close.equals(where)) {
                        if (operantStack.isEmpty())
                            log.log(Level.WARNING, "No thing to close");
                        else {
                            if (emptyParenthese)
                                whereBuilder.append("true");
                            else
                                whereBuilder.delete(whereBuilder.length()-4, whereBuilder.length());
                            whereBuilder.append(")");
                            currentOperant = operantStack.pop();
                            whereBuilder.append(currentOperant);
                            emptyParenthese = false;
                        }
                    } else if (null != conditions.get(where)) {
                        hasWhere = true;
                        emptyParenthese = false;
                        whereBuilder.append(conditions.get(where)).append(currentOperant);
                        resolveJoinedTable(conditionProjects.get(where), joinedTables);
                    } else
                        log.log(Level.WARNING, "No condition specified for " + where);
                }
                if (!hasWhere) {
                    log.log(Level.WARNING, "No effective condition specified.");
                    whereBuilder.append("true ");
                } else {
                    if (emptyParenthese)
                        whereBuilder.append("true");
                    else
                        whereBuilder.delete(whereBuilder.length()-4, whereBuilder.length()); // remove last "AND "
                    while (!operantStack.isEmpty()) {
                        operantStack.pop();
                        whereBuilder.append(")");
                    }
                }
            }
            // resolve groups
            StringBuilder groupBuilder = new StringBuilder("GROUP BY ");
            boolean hasGroup = false;
            if (null != groups && !groups.isEmpty()) {
                for (String group : groups) {
                    if (null != fields.get(group)) {
                        hasGroup = true;
                        groupBuilder.append(fields.get(group)).append(", ");
                        resolveJoinedTable(fieldProjects.get(group), joinedTables);
                    } else
                        log.log(Level.WARNING, "No group specified for " + group);
                }
                if (hasGroup)
                    groupBuilder.deleteCharAt(groupBuilder.length() - 2); // remove last colon
                else
                    log.log(Level.WARNING, "No effective group specified.");
            }
            // resolve sorts
            StringBuilder sortBuilder = new StringBuilder("ORDER BY ");
            boolean hasSort = false;
            if (null != sorts && !sorts.isEmpty()) {
                for (String sort : sorts) {
                    String sortTokens[] = sort.split(" ");
                    if (sortTokens.length <=0 || sortTokens.length > 2)
                        continue;
                    String column = sortTokens[0];
                    String direction = sortTokens.length > 1 ? sortTokens[1] : "ASC";
                    if (!"DESC".equals(direction))
                        direction = "ASC";
                    if (null != fields.get(column)) {
                        hasSort = true;
                        sortBuilder.append(fields.get(column)).append(" ").append(direction).append(", ");
                        resolveJoinedTable(fieldProjects.get(column), joinedTables);
                    } else
                        log.log(Level.WARNING, "No column specified for " + column);
                }
                if (hasSort)
                    sortBuilder.deleteCharAt(sortBuilder.length() - 2); // remove last colon
                else
                    log.log(Level.WARNING, "No effective sort specified.");
            }
            // Build from string
            StringBuilder fromBuilder = new StringBuilder("FROM ");
            fromBuilder.append(tables.get(rootTable).table()).append(" AS ").append(tables.get(rootTable).alias()).append(" ");
            for (Table joinedTable : joinedTables) {
                if (joinedTable.joinType == JoinType.INNER)
                    fromBuilder.append("INNER JOIN ").append(joinedTable.table()).append(" AS ").append(joinedTable.alias()).append(" ON ");
                else if (joinedTable.joinType == JoinType.LEFT)
                    fromBuilder.append("LEFT JOIN ").append(joinedTable.table()).append(" AS ").append(joinedTable.alias()).append(" ON ");
                else if (joinedTable.joinType == JoinType.RIGHT)
                    fromBuilder.append("RIGHT JOIN ").append(joinedTable.table()).append(" AS ").append(joinedTable.alias()).append(" ON ");
                else
                    continue;
                boolean hasOnCondition = false;
                for (CharSequence condition : joinedTable.onConditions()) {
                    if (!hasOnCondition)
                        hasOnCondition = true;
                    else
                        fromBuilder.append(" AND ");
                    fromBuilder.append(condition);
                }
                if (hasOnCondition)
                    fromBuilder.append(" ");
                else
                    fromBuilder.append("true ");
            }
            // Join all strings
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append(selectBuilder).append(fromBuilder).append(whereBuilder);
            if (hasGroup)
                queryBuilder.append(groupBuilder);
            if (hasSort)
                queryBuilder.append(sortBuilder);
            if (null != limitAlias)
                queryBuilder.append("LIMIT :").append(limitAlias).append(" ");
            if (null != offsetAlias)
                queryBuilder.append("OFFSET :").append(offsetAlias).append(" ");
            return queryBuilder.toString();
        }

        private void resolveJoinedTable(String tableAlias, List<Table> joinedTables) {
            // FIXME: mutate joinedTables
            if (tableAlias.equals(rootTable))
                return;
            Table table = tables.get(tableAlias);
            if (table.joinType() == JoinType.ROOT || joinedTables.contains(table))
                return;
            for (String required : table.requiredTables()) {
                resolveJoinedTable(required, joinedTables);
            }
            joinedTables.add(table);
        }
    }
}
