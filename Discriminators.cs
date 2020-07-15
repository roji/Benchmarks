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
    public class Discriminators
    {
        [Params("SqlServer", "PostgreSQL", "MySQL", "Sqlite")]
        public string Database { get; set; }

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

        [GlobalSetup(Target = nameof(SingleDiscriminatorColumn))]
        public void SetupSingleDiscriminatorColumn()
        {
            SharedSetup();

            _command.CommandText = @"
    SELECT L1.Id, L1P, L2P, L3AP, L3BP, L4AP, L4BP, L4CP, L4DP,
       CASE
           WHEN L4AId IS NOT NULL THEN 'L4A'
           WHEN L4BId IS NOT NULL THEN 'L4B'
           WHEN L4CId IS NOT NULL THEN 'L4C'
           WHEN L4DId IS NOT NULL THEN 'L4D'
       END AS Discriminator
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
    SELECT Id AS L4AId,   NULL AS L4BId, NULL AS L4CId, NULL AS L4DId, L3AId AS L3Id, L4AP,         NULL AS L4BP, NULL AS L4CP, NULL AS L4DP
    FROM L4A
    UNION ALL
    SELECT NULL AS L4AId, Id AS L4BId,   NULL AS L4CId, NULL AS L4DId, L3AId AS L3Id, NULL AS L4AP, L4BP,         NULL AS L4CP, NULL AS L4DP
    FROM L4B
    UNION ALL
    SELECT NULL AS L4AId, NULL AS L4BId, Id AS L4CId,   NULL AS L4DId, L3BId AS L3Id, NULL AS L4AP, NULL AS L4BP, L4CP,         NULL AS L4DP
    FROM L4C
    UNION ALL
    SELECT NULL AS L4AId, NULL AS L4BId, NULL AS L4CId, Id AS L4DId,   L3BId AS L3Id, NULL AS L4AP, NULL AS L4BP, NULL AS L4CP, L4DP
    FROM L4D
    ) AS L4 ON L4.L3Id = L2.Id";
        }

        [GlobalSetup(Target = nameof(MultipleDiscriminatorColumns))]
        public void SetupMultipleDiscriminatorColumns()
        {
            SharedSetup();

            _command.CommandText = @"
    SELECT L1.Id, L1P, L2P, L3AP, L3BP, L4AP, L4BP, L4CP, L4DP,
       CASE WHEN L4AId IS NULL THEN NULL ELSE 'L4A' END AS IsL4A,
       CASE WHEN L4BId IS NULL THEN NULL ELSE 'L4B' END AS IsL4B,
       CASE WHEN L4CId IS NULL THEN NULL ELSE 'L4C' END AS IsL4C,
       CASE WHEN L4DId IS NULL THEN NULL ELSE 'L4D' END AS IsL4D
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
    SELECT Id AS L4AId,   NULL AS L4BId, NULL AS L4CId, NULL AS L4DId, L3AId AS L3Id, L4AP,         NULL AS L4BP, NULL AS L4CP, NULL AS L4DP
    FROM L4A
    UNION ALL
    SELECT NULL AS L4AId, Id AS L4BId,   NULL AS L4CId, NULL AS L4DId, L3AId AS L3Id, NULL AS L4AP, L4BP,         NULL AS L4CP, NULL AS L4DP
    FROM L4B
    UNION ALL
    SELECT NULL AS L4AId, NULL AS L4BId, Id AS L4CId,   NULL AS L4DId, L3BId AS L3Id, NULL AS L4AP, NULL AS L4BP, L4CP,         NULL AS L4DP
    FROM L4C
    UNION ALL
    SELECT NULL AS L4AId, NULL AS L4BId, NULL AS L4CId, Id AS L4DId,   L3BId AS L3Id, NULL AS L4AP, NULL AS L4BP, NULL AS L4CP, L4DP
    FROM L4D
    ) AS L4 ON L4.L3Id = L2.Id";
        }

        #endregion

        [Benchmark]
        public async Task SingleDiscriminatorColumn()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }

        [Benchmark]
        public async Task MultipleDiscriminatorColumns()
        {
            using var reader = await _command.ExecuteReaderAsync();
            while (reader.Read()) {}
        }
    }
}
