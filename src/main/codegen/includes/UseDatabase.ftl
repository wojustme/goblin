SqlNode SqlUseDatabase() :
{
    final Span s;
    final SqlIdentifier dbIdentifier;
}
{
    <USE> { s = span(); }
    dbIdentifier = CompoundIdentifier()
    {
        return new SqlUseDatabase(s.end(this), dbIdentifier);
    }
}