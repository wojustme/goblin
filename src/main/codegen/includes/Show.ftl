SqlNode SqlShowStat() :
{
    final Span s;
}
{
    <SHOW> { s = span(); }
    (
        <DATABASES> {
            return new SqlShow(s.end(this), "DATABASES");
        }
        | <TABLES> {
            return new SqlShow(s.end(this), "TABLES");
        }
    )
}