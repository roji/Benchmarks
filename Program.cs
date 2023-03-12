using System.Data.Common;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.Data.SqlClient;
using Npgsql;

BenchmarkRunner.Run<Benchmark>();

public class Benchmark
{
    private DbConnection _connection = null!;
    private DbCommand _command = null!;

    [GlobalSetup(Target = nameof(SqlServer))]
    public void SqlServerSetup()
    {
        _connection = new SqlConnection("Server=localhost;Database=test;User=SA;Password=Abcd5678;Connect Timeout=60;ConnectRetryCount=0;Encrypt=false");
        _connection.Open();
        _command = _connection.CreateCommand();
        _command.CommandText = "SELECT 1";
    }

    [GlobalSetup(Target = nameof(PostgreSQL))]
    public void PostgreSQLSetup()
    {
        _connection = new NpgsqlConnection("Host=localhost;Username=test;Password=test;SSL Mode=disable");
        _connection.Open();
        _command = _connection.CreateCommand();
        _command.CommandText = "SELECT 1";
    }

    [Benchmark]
    public void SqlServer()
    {
        _command.ExecuteNonQuery();
    }

    [Benchmark]
    public void PostgreSQL()
    {
        _command.ExecuteNonQuery();
    }
}
