boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true;}
    |
    {return false;}
}