boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true;}
    |
    {return false;}
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true;}
    |
    {return false;}
}
        