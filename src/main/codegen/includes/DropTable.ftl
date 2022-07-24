SqlDrop SqlDropTable(Span s, boolean replace): {
    final SqlIdentifier tableIdentifier;
    final boolean ifExists;
} {
    <TABLE>
    ifExists = IfExistsOpt()
    tableIdentifier = CompoundIdentifier()
    {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, tableIdentifier);
    }
}
