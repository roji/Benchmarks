using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

BenchmarkRunner.Run<Benchmarks>();

public class Benchmarks
{
    [GlobalSetup]
    public void Setup()
    {
    }

    [Benchmark]
    public void Foo()
    {
    }
}
