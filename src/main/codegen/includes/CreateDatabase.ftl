SqlCreate SqlCreateDatabase(Span s, boolean replace): {
    final SqlIdentifier dbIdentifier;
    final boolean ifNotExists;
    String comment = null;
} {
    <DATABASE>
    ifNotExists = IfNotExistsOpt()
    dbIdentifier = CompoundIdentifier()
    comment = CommentInfo()
    {
        return SqlDdlNodes.createDatabase(
            s.end(this), replace, ifNotExists,
            dbIdentifier, comment);
    }
}