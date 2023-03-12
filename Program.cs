using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Server;
using Npgsql;

BenchmarkRunner.Run<Benchmark>();

// Uncomment the following for quick testing that all tests technically work
// [SimpleJob(warmupCount: 1, targetCount: 1, invocationCount: 1)]
public class Benchmark
{
    // [Params(Database.SQLServer, Database.PostgreSQL)]
    [Params(Database.SQLServer)]
    public Database Database { get; set; }

    [Params(2, 10, 100, 1000)]
    public int NumSearchValues { get; set; }

    [Params(true, false)]
    public bool Found { get; set; }

    private int[] _searchValues = null!;

    private const int TotalRows = 1000;

    [Params(0, 5, 10, 50, 100, 500)]
    public int PaddingCount { get; set; }

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

        var builder = new StringBuilder("INSERT INTO data (id, int) VALUES");
        for (var i = 0; i < TotalRows; i++)
        {
            if (i > 0)
                builder.Append(",");
            builder.Append($" ({i}, {i + 100})");
        }

        _command.CommandText = builder.ToString();
        Console.WriteLine("Seed SQL: " + builder);
        _connection.Execute(builder.ToString());

        _searchValues = new int[NumSearchValues];
        for (var i = 0; i < _searchValues.Length; i++)
        {
            _searchValues[i] = Random.Shared.Next(TotalRows) + (Found ? 0 : 1000);
        }
    }

    [GlobalSetup(Target = nameof(In_with_constants))]
    public void In_with_constants_setup()
    {
        if (PaddingCount != 0)
            throw new NotSupportedException();

        CommonSetup();

        var builder = new StringBuilder("SELECT id FROM data WHERE int IN (");
        for (var i = 0; i < _searchValues.Length; i++)
        {
            if (i > 0)
                builder.Append(",");
            builder.Append(_searchValues[i]);
        }

        builder.Append(")");
        _command.CommandText = builder.ToString();
        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [GlobalSetup(Target = nameof(PG_any_with_parameter))]
    public void PG_any_with_parameter_setup()
    {
        if (Database != Database.PostgreSQL || PaddingCount != 0)
            throw new NotSupportedException();

        CommonSetup();

        _command.CommandText = "SELECT id FROM data WHERE int = ANY($1)";
        var p = _command.CreateParameter();
        p.Value = _searchValues;
        _command.Parameters.Add(p);

        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }


    [GlobalSetup(Target = nameof(In_with_padded_parameters))]
    public void In_with_padded_parameters_setup()
    {
        CommonSetup();

        var builder = new StringBuilder("SELECT id FROM data WHERE int IN (");
        for (var i = 0; i < _searchValues.Length + PaddingCount; i++)
        {
            if (i > 0)
                builder.Append(",");

            var searchValue = i < _searchValues.Length ? _searchValues[i] : _searchValues[^1];

            switch (Database)
            {
                case Database.SQLServer:
                    var parameterName = $"@p{i + 1}";
                    builder.Append(parameterName);
                    _command.Parameters.Add(
                        new SqlParameter { ParameterName = parameterName, Value = searchValue });
                    break;
                case Database.PostgreSQL:
                    builder.Append($"${i + 1}");
                    _command.Parameters.Add(new NpgsqlParameter { Value = searchValue });
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        if (Database == Database.SQLServer)
        {
            ((SqlCommand)_command).EnableOptimizedParameterBinding = true;
        }

        builder.Append(")");
        _command.CommandText = builder.ToString();
        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [GlobalSetup(Target = nameof(Array_param_with_exists_subquery))]
    public void Array_param_with_exists_subquery_setup()
    {
        if (PaddingCount != 0)
            throw new NotSupportedException();
        CommonSetup();

        switch (Database)
        {
            case Database.SQLServer:
                _command.CommandText =
"""
SELECT id FROM data AS d WHERE EXISTS (
    SELECT 1
    FROM OpenJson(@p) WITH ([Value] integer '$') AS v
    WHERE v.Value = d.int)
""";
                _command.Parameters.Add(new SqlParameter("p", JsonSerializer.Serialize(_searchValues)));
                break;
            case Database.PostgreSQL:
                _command.CommandText =
"""
SELECT id FROM data AS d WHERE EXISTS (
    SELECT 1
    FROM unnest($1) AS v
    WHERE v.v = d.int)
""";
                _command.Parameters.Add(new NpgsqlParameter { Value = _searchValues });
                break;
        }

        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [GlobalSetup(Target = nameof(Array_param_with_inner_join))]
    public void Array_param_with_inner_join_setup()
    {
        if (PaddingCount != 0)
            throw new NotSupportedException();
        CommonSetup();

        switch (Database)
        {
            case Database.SQLServer:
                _command.CommandText =
"""
SELECT id FROM data AS d
JOIN OpenJson(@p) WITH ([Value] integer '$') AS v ON v.Value = d.int;
""";
                _command.Parameters.Add(new SqlParameter("p", JsonSerializer.Serialize(_searchValues)));
                break;
            case Database.PostgreSQL:
                _command.CommandText =
"""
SELECT id FROM data AS d
JOIN unnest($1) AS v ON v.v = d.int;
""";
                _command.Parameters.Add(new NpgsqlParameter { Value = _searchValues });
                break;
        }

        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [GlobalSetup(Target = nameof(OpenXml_with_inner_join))]
    public void OpenXml_with_inner_join_setup()
    {
        if (Database != Database.SQLServer || PaddingCount != 0)
            throw new NotSupportedException();
        CommonSetup();

        _command.CommandText =
"""
DECLARE @idoc INT;
EXEC sp_xml_preparedocument @idoc OUTPUT, @doc;
SELECT id FROM data AS d
JOIN OpenXML(@idoc, '/Root/Value', 1) WITH ([value] integer) AS v ON v.Value = d.int;
EXEC sp_xml_removedocument @idoc;
""";
        var builder = new StringBuilder("<Root>");
        for (var i = 0; i < _searchValues.Length; i++)
            builder.Append($@"<Value value=""{_searchValues[i]}""/>");
        builder.Append("</Root>");

        _command.Parameters.Add(new SqlParameter("doc", builder.ToString()));

        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [GlobalSetup(Target = nameof(Temporary_table_with_inner_join))]
    public void Temporary_table_with_inner_join_setup()
    {
        if (Database != Database.SQLServer || PaddingCount != 0)
            throw new NotSupportedException();
        CommonSetup();

        // Note: this creates the TVP's type once at setup. This is because it's not possible to batch the creation
        // of the type and its use in the same SqlCommand, so that would require multiple roundtrips (better batching
        // via TDS RpcMessage would like make this work).
        // So for now, we're creating it just once, although that isn't usable by EF.
        _command.CommandText =
"""
DROP TYPE IF EXISTS int_wrapper;
CREATE TYPE int_wrapper AS TABLE (int INT);
""";
        _command.ExecuteNonQuery();

        // Note that we also create the SqlDataRecords only once at setup, which is also unrealistic and makes TVP
        // look much better than it is.
        var records = _searchValues.Select(i =>
        {
            var record = new SqlDataRecord(new SqlMetaData("int", SqlDbType.Int));
            record.SetInt32(0, i);
            return record;
        }).ToArray();

        _command.CommandText = "SELECT id FROM data AS d JOIN @ints AS ints ON ints.int = d.int";
        _command.Parameters.Add(new SqlParameter("ints", SqlDbType.Structured)
        {
            TypeName = "int_wrapper",
            Value = records
        });

        Console.WriteLine("Benchmark query: " + _command.CommandText);
    }

    [Benchmark]
    public void In_with_constants() => _command.ExecuteNonQuery();

    [Benchmark]
    public void PG_any_with_parameter() => _command.ExecuteNonQuery();

    [Benchmark]
    public void In_with_padded_parameters() => _command.ExecuteNonQuery();

    [Benchmark]
    public void Array_param_with_exists_subquery() => _command.ExecuteNonQuery();

    [Benchmark]
    public void Array_param_with_inner_join() => _command.ExecuteNonQuery();

    [Benchmark]
    public void OpenXml_with_inner_join() => _command.ExecuteNonQuery();

    [Benchmark]
    public void Temporary_table_with_inner_join() => _command.ExecuteNonQuery();

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
