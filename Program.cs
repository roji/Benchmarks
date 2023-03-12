using System.Data.Common;
using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Dapper;
using Microsoft.Data.SqlClient;
using Npgsql;

BenchmarkRunner.Run<Benchmark>();

// Uncomment the following for very quick test runs to make sure the code is correct
// [SimpleJob(warmupCount: 1, targetCount: 1, invocationCount: 1)]
public class Benchmark
{
    [Params(Database.SQLServer, Database.PostgreSQL)]
    public Database Database { get; set; }

    private DbConnection _connection = null!;
    private DbCommand _command = null!;

    public void CommonSetup()
    {
        _connection = Database switch
        {
            Database.SQLServer => new SqlConnection("Server=localhost;Database=test;User=SA;Password=Abcd5678;Connect Timeout=60;ConnectRetryCount=0;Encrypt=false"),
            Database.PostgreSQL => new NpgsqlConnection("Host=localhost;Username=test;Password=test;SSL Mode=disable"),
            _ => throw new UnreachableException()
        };
        _connection.Open();
        _command = _connection.CreateCommand();

        _connection.Execute("DROP TABLE IF EXISTS data");
        _connection.Execute("CREATE TABLE data (id INTEGER, int INTEGER)");
        _connection.Execute("CREATE INDEX IX_data ON data(int)");

        // TODO: Seed test data
    }

    [GlobalSetup(Target = nameof(Scenario_A))]
    public void Scenario_A_setup()
    {
        CommonSetup();

        // TODO: Specific setup for scenario A
    }

    [GlobalSetup(Target = nameof(Scenario_B))]
    public void Scenario_B_setup()
    {
        CommonSetup();

        // TODO: Specific setup for scenario B
    }

    [Benchmark]
    public void Scenario_A() => _command.ExecuteNonQuery();

    [Benchmark]
    public void Scenario_B() => _command.ExecuteNonQuery();

    [GlobalCleanup]
    public void Cleanup()
    {
        _command.Dispose();
        _connection.Dispose();
    }
}

public enum Database
{
    SQLServer,
    PostgreSQL
}
