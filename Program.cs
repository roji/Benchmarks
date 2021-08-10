using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

BenchmarkRunner.Run<Program>();

public class Program
{
    [Benchmark]
    public int Foo()
    {
    }
}
