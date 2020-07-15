using System;
using System.Data.Common;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;

namespace Benchmark
{
    public class JoinVsUnion
    {
        [Params("SqlServer", "PostgreSQL", "MySQL", "Sqlite")]
        // [Params("SqlServer")]
        public string Database { get; set; }

        [Params(3, 4)]
        // [Params(3)]
        public int Levels { get; set; }

        DbProviderFactory _factory;
        DbConnection _connection;
        DbCommand _command;

        #region Setup

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

        [GlobalSetup(Target = nameof(MultipleJoins))]
        public void SetupMultipleJoins()
        {
            SharedSetup();

            _command.CommandText = Levels switch
            {
                3 => @"
    SELECT L1.Id, L1P, L2P, L3AP, L3BP
    FROM L2
    JOIN      L1 ON L1.Id = L2.L1Id
    LEFT JOIN L3A ON L3A.L2Id = L2.Id
    LEFT JOIN L3B ON L3B.L2Id = L2.Id",

                4 => @"
SELECT L1.Id, L1P, L2P, L3AP, L3BP, L4AP, L4BP, L4CP, L4DP
FROM L2
JOIN      L1 ON L1.Id = L2.L1Id
LEFT JOIN L3A ON L3A.L2Id = L2.Id
LEFT JOIN L3B ON L3B.L2Id = L2.Id
LEFT JOIN L4A ON L4A.L3AId = L3A.Id
LEFT JOIN L4B ON L4B.L3AId = L3A.Id
LEFT JOIN L4C ON L4C.L3BId = L3B.Id
LEFT JOIN L4D ON L4D.L3BId = L3B.Id",

                _ => throw new ArgumentException()
            };
        }

        [GlobalSetup(Target = nameof(UnionAll))]
        public void SetupUnionAll()
        {
            SharedSetup();

            _command.CommandText = Levels switch
            {
                3 => @"
SELECT L1.Id, L1P, L2P, L3AP, L3BP
FROM L2
JOIN L1 ON L1.Id = L2.L1Id
LEFT JOIN
(
    SELECT L2Id AS Id, L3AP, NULL AS L3BP
    FROM L3A
UNION ALL
    SELECT L2Id AS Id, NULL AS L3AP, L3BP
    FROM L3B
) AS L3 ON L3.Id = L2.Id",

                4 => @"
SELECT L1.Id, L1P, L2P, L3AP, L3BP, L4AP, L4BP, L4CP, L4DP
FROM L2
JOIN L1 ON L1.Id = L2.L1Id
LEFT JOIN
(
    SELECT L2Id, L3AP,         NULL AS L3BP
    FROM L3A
UNION ALL
    SELECT L2Id, NULL AS L3AP, L3BP
    FROM L3B
) AS L3 ON L3.L2Id = L2.Id
LEFT JOIN
(
    SELECT L3AId AS Id, L4AP,         NULL AS L4BP, NULL AS L4CP, NULL AS L4DP
    FROM L4A
UNION ALL
    SELECT L3AId AS Id, NULL AS L4AP, L4BP,         NULL AS L4CP, NULL AS L4DP
    FROM L4B
UNION ALL
    SELECT L3BId AS Id, NULL AS L4AP, NULL AS L4BP, L4CP,         NULL AS L4DP
    FROM L4C
UNION ALL
    SELECT L3BId AS Id, NULL AS L4AP, NULL AS L4BP, NULL AS L4CP, L4DP
    FROM L4D
) AS L4 ON L4.Id = L2.Id",

                _ => throw new ArgumentException()
            };
        }

        [GlobalSetup(Target = nameof(UnionAllDiagonal))]
        public void SetupUnionAllDiagonal()
        {
            SharedSetup();

            _command.CommandText = Levels switch
            {
                3 => throw new NotSupportedException(),

                4 => @"
SELECT L1.Id, L1P, L2P, L3AP, L3BP, L4AP, L4BP, L4CP, L4DP
FROM L2
JOIN L1 ON L1.Id = L2.L1Id
LEFT JOIN
(
    SELECT L2Id AS Id,  L3AP,         NULL AS L4CP, NULL AS L4DP
    FROM L3A
UNION ALL
    SELECT L3BId AS Id, NULL AS L3AP, L4CP,         NULL AS L4DP
    FROM L4C
UNION ALL
    SELECT L3BId AS Id, NULL AS L3AP, NULL AS L4CP, L4DP
    FROM L4D
) AS E1 ON E1.Id = L2.Id
LEFT JOIN
(
    SELECT L2Id  AS Id, L3BP,         NULL AS L4AP, NULL AS L4BP
    FROM L3B
UNION ALL
    SELECT L3AId AS Id, NULL AS L3BP, L4AP,         NULL AS L4BP
    FROM L4A
UNION ALL
    SELECT L3AId AS Id, NULL AS L3BP, NULL AS L4AP, L4BP
    FROM L4B
) AS L4 ON L4.Id = L2.Id",

                _ => throw new ArgumentException()
            };
        }

        #endregion

        [Benchmark]
        public async Task MultipleJoins()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }

        [Benchmark]
        public async Task UnionAll()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }

        [Benchmark]
        public async Task UnionAllDiagonal()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }
    }
}
