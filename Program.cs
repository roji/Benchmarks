using BenchmarkDotNet.Running;

namespace Benchmark
{
    public class Benchmarks
    {
        static void Main(string[] args)
            => BenchmarkSwitcher.FromAssembly(typeof(Benchmarks).Assembly).Run(args);
    }
}
