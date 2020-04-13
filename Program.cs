using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

namespace Benchmark
{
    public class Benchmarks
    {
        [Benchmark]
        public void Foo()
        {

        }

        static void Main(string[] args) => BenchmarkRunner.Run<Benchmarks>();
    }
}
