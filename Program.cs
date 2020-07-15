using System;
using System.Data.Common;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;

namespace Benchmark
{
    public class Benchmarks
    {
        [Params("SqlServer", "PostgreSQL", "MySQL", "Sqlite")]
        public string Database { get; set; }

        DbProviderFactory _factory;
        DbConnection _connection;
        DbCommand _command;

        public void SharedSetup()
        {
            switch (Database)
            {
                case "SqlServer":
                    _factory = SqlClientFactory.Instance;
                    _connection = _factory.CreateConnection();
                    _connection.ConnectionString = "Server=localhost;Database=test;User=SA;Password=Abcd5678;Connect Timeout=60;ConnectRetryCount=0";
                    break;
                case "PostgreSQL":
                    _factory = NpgsqlFactory.Instance;
                    _connection = _factory.CreateConnection();
                    _connection.ConnectionString = "Host=localhost;Username=test;Password=test";
                    break;
                case "MySQL":
                    _factory = MySqlConnectorFactory.Instance;
                    _connection = _factory.CreateConnection();
                    _connection.ConnectionString = "Server=localhost;User ID=roji;Password=scsicd;Database=test";
                    break;
                case "Sqlite":
                    _factory = SqliteFactory.Instance;
                    _connection = _factory.CreateConnection();
                    _connection.ConnectionString = "Data Source=/home/roji/tmp/test.sqlite";
                    break;
                default:
                    throw new ArgumentException();
            }
            _connection.Open();
            _command = _factory.CreateCommand();
            _command.Connection = _connection;
        }

        [GlobalSetup(Target = nameof(A))]
        public void SetupA()
        {
            SharedSetup();
            _command.CommandText = @"SELECT ...";
        }

        [GlobalSetup(Target = nameof(B))]
        public void SetupB()
        {
            SharedSetup();
            _command.CommandText = @"SELECT ...";
        }

        [Benchmark]
        public async Task A()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }

        [Benchmark]
        public async Task B()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }

        static void Main(string[] args) => BenchmarkRunner.Run<Benchmarks>();
    }
}
