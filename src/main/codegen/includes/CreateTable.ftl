SqlCreate SqlCreateTable(Span s, boolean replace): {
    final SqlIdentifier tableIdentifier;
    final boolean ifNotExists;
    List<SqlNode> columnList = new ArrayList<SqlNode>();
    String comment = null;
} {
    <TABLE>
    ifNotExists = IfNotExistsOpt()
    tableIdentifier = CompoundIdentifier()
    [ TableElementList(columnList) ]
    comment = CommentInfo()
    {
        return SqlDdlNodes.createTable(
            s.end(this), replace, ifNotExists,
            tableIdentifier, new SqlNodeList(columnList, getPos()), comment);
    }
}

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true;}
    |
    {return false;}
}

void TableElementList(List<SqlNode> columnList) :
{
    final Span s;
}
{
    <LPAREN> { s = span(); }
    TableElement(columnList)
    (
        <COMMA> TableElement(columnList)
    )*
    <RPAREN>
}

void TableElement(List<SqlNode> columnList) :
{
}
{
    ColumnElement(columnList)
}

void ColumnElement(List<SqlNode> list): {
    final SqlNode column;
} {
    column = NewColumn()
    { list.add(column); }
}

SqlColumn NewColumn(): {
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final Span s = Span.of();
    String comment = null;
    String defaultVal = null;
    boolean isSetDefaultVal = false;
} {
    id = SimpleIdentifier()
    (
        type = DataType()
        (
            <NULL> {
                nullable = true;
            }
            | <NOT> <NULL> {
                nullable = false;
            }
            | {
                nullable = true;
            }
        )
        (
            <DEFAULT_> {
                isSetDefaultVal = true;
                defaultVal = StringVal();
            }
            | {
                isSetDefaultVal = false;
            }
        )
        comment = CommentInfo()
    )
    { return SqlDdlNodes.column(s.end(this), id, type, nullable, isSetDefaultVal, defaultVal, comment); }
}

String CommentInfo() : {
} {
    <COMMENT> { return StringVal(); }
    | { return null; }
}

// get string value from parse, such as: `hello`, 'hello', 123
String StringVal(): {
    SqlNode sample = null;
    String value = null;
} {
    (
        sample = StringLiteral() {
            value = ((NlsString) SqlLiteral.value(sample)).getValue();
        }
        | sample = CompoundIdentifier() {
            value = ((SqlIdentifier)sample).toString();
        }
        | sample = SpecialLiteral() {
            value = sample.toString();
        }
        | sample = NumericLiteral() {
            value = ((SqlNumericLiteral)sample).toValue();
        }
        | {
            value = null;
        }
    )
    { return value; }
}