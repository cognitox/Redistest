public class CacheConfig
{
    public string Name { get; set; }
    public string Password { get; set; }
    public string KeyPrefix { get; set; }
    public string SearchPattern { get; set; }
    public bool ClusterMode { get; set; }
    public bool SetKeysFirst { get; set; }
    public int NumberOfKeysToSet { get; set; }
}