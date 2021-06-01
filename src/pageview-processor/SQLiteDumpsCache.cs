using Microsoft.Data.Sqlite;

namespace pageview_processor
{
    public interface IDumpsCache
    {
        void Init();
        void Add(string date, string path);
        bool TryGet(string date, out string path);
    }

    public class SQLiteDumpsCache : IDumpsCache
    {
        private readonly SqliteConnectionStringBuilder conn = new() { DataSource = "cache.db" };
        public SQLiteDumpsCache() { Init(); }

        public void Init()
        {
            using var connection = new SqliteConnection(conn.ConnectionString);
            connection.Open();

            var createTableCmd = connection.CreateCommand();
            createTableCmd.CommandText = @"CREATE TABLE IF NOT EXISTS dumps_cache(
                date_id TEXT PRIMARY KEY, path TEXT NOT NULL)";
            createTableCmd.ExecuteNonQuery();
        }

        public void Add(string date, string path)
        {
            using var connection = new SqliteConnection(conn.ConnectionString);
            connection.Open();

            var insertCmd = connection.CreateCommand();
            insertCmd.CommandText = "INSERT INTO dumps_cache VALUES($date, $path)";
            insertCmd.Parameters.AddWithValue("$date", date);
            insertCmd.Parameters.AddWithValue("$path", path);
            insertCmd.ExecuteNonQuery();
        }

        public bool TryGet(string date, out string path)
        {
            using var connection = new SqliteConnection(conn.ConnectionString);
            connection.Open();

            var selectCmd = connection.CreateCommand();
            selectCmd.CommandText = "SELECT path FROM dumps_cache WHERE date_id = $date";
            selectCmd.Parameters.AddWithValue("$date", date);
            using var reader = selectCmd.ExecuteReader();

            path = reader.Read() ? reader.GetString(0) : default;
            return reader.HasRows;
        }

    }
}
